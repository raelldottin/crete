#!/usr/bin/env python3
"""Generate REST API endpoint artifacts from captured HTTP traffic in a pcapng file."""

from __future__ import annotations

import argparse
import copy
import json
import os
import re
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qs, unquote, urlsplit

from wireshark import NetworkApplication, TCPConnectionSession, Wireshark

HTTP_METHODS: Set[str] = {
    "GET",
    "POST",
    "PUT",
    "PATCH",
    "DELETE",
    "HEAD",
    "OPTIONS",
}

UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)
LONG_HEX_RE = re.compile(r"^[0-9a-fA-F]{16,}$")
PATH_PARAM_RE = re.compile(r"\{([^{}]+)\}")


@dataclass
class HTTPMessage:
    kind: str
    version: str
    headers: Dict[str, str]
    body: bytes
    method: Optional[str] = None
    target: Optional[str] = None
    status_code: Optional[int] = None
    reason: str = ""


class HTTPStreamParser:
    def __init__(self) -> None:
        self.buffer = bytearray()

    def feed(self, data: bytes) -> List[HTTPMessage]:
        self.buffer.extend(data)
        messages: List[HTTPMessage] = []
        while True:
            message = self._parse_one()
            if message is None:
                break
            messages.append(message)
        return messages

    def _parse_one(self) -> Optional[HTTPMessage]:
        while True:
            line_end = self.buffer.find(b"\r\n")
            if line_end < 0:
                return None
            start_line = bytes(self.buffer[:line_end])
            parsed = self._parse_start_line(start_line)
            if parsed is not None:
                break
            del self.buffer[0]

        headers_end = self.buffer.find(b"\r\n\r\n")
        if headers_end < 0:
            return None

        headers = self._parse_headers(bytes(self.buffer[line_end + 2 : headers_end]))
        body_start = headers_end + 4
        transfer_encoding = headers.get("transfer-encoding", "").lower()

        if "chunked" in transfer_encoding:
            parsed_body = self._parse_chunked_body(body_start)
            if parsed_body is None:
                return None
            total_length, body = parsed_body
        else:
            content_length = self._parse_content_length(headers.get("content-length"))
            total_length = body_start + content_length
            if len(self.buffer) < total_length:
                return None
            body = bytes(self.buffer[body_start:total_length])

        del self.buffer[:total_length]

        kind, metadata = parsed
        if kind == "request":
            method, target, version = metadata
            return HTTPMessage(
                kind="request",
                method=method,
                target=target,
                version=version,
                headers=headers,
                body=body,
            )

        version, status_code, reason = metadata
        return HTTPMessage(
            kind="response",
            version=version,
            status_code=status_code,
            reason=reason,
            headers=headers,
            body=body,
        )

    def _parse_start_line(self, start_line: bytes) -> Optional[Tuple[str, Tuple[Any, ...]]]:
        if start_line.startswith(b"HTTP/"):
            parts = start_line.split(b" ", 2)
            if len(parts) < 2:
                return None
            version = parts[0].decode("ascii", errors="ignore")
            try:
                status_code = int(parts[1])
            except ValueError:
                return None
            reason = parts[2].decode("latin-1", errors="ignore") if len(parts) > 2 else ""
            return "response", (version, status_code, reason)

        parts = start_line.split(b" ", 2)
        if len(parts) != 3:
            return None

        method = parts[0].decode("ascii", errors="ignore").upper()
        if method not in HTTP_METHODS:
            return None
        version = parts[2].decode("ascii", errors="ignore")
        if not version.startswith("HTTP/"):
            return None
        target = parts[1].decode("latin-1", errors="ignore")
        return "request", (method, target, version)

    @staticmethod
    def _parse_headers(header_block: bytes) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if not header_block:
            return headers

        for line in header_block.split(b"\r\n"):
            if b":" not in line:
                continue
            key, value = line.split(b":", 1)
            name = key.decode("latin-1", errors="ignore").strip().lower()
            if not name:
                continue
            headers[name] = value.decode("latin-1", errors="ignore").strip()
        return headers

    @staticmethod
    def _parse_content_length(raw_value: Optional[str]) -> int:
        if not raw_value:
            return 0
        try:
            value = int(raw_value.strip())
            return max(0, value)
        except ValueError:
            return 0

    def _parse_chunked_body(self, body_start: int) -> Optional[Tuple[int, bytes]]:
        cursor = body_start
        body = bytearray()

        while True:
            size_line_end = self.buffer.find(b"\r\n", cursor)
            if size_line_end < 0:
                return None

            size_line = bytes(self.buffer[cursor:size_line_end]).split(b";", 1)[0].strip()
            try:
                chunk_size = int(size_line, 16)
            except ValueError:
                return None

            cursor = size_line_end + 2
            if len(self.buffer) < cursor + chunk_size + 2:
                return None

            body.extend(self.buffer[cursor : cursor + chunk_size])
            cursor += chunk_size
            if bytes(self.buffer[cursor : cursor + 2]) != b"\r\n":
                return None
            cursor += 2

            if chunk_size == 0:
                trailer_end = self.buffer.find(b"\r\n\r\n", cursor)
                if trailer_end < 0:
                    if len(self.buffer) < cursor + 2:
                        return None
                    if bytes(self.buffer[cursor : cursor + 2]) != b"\r\n":
                        return None
                    cursor += 2
                else:
                    cursor = trailer_end + 4
                return cursor, bytes(body)


@dataclass
class ResponseObservation:
    content_types: Set[str] = field(default_factory=set)
    schema: Optional[Dict[str, Any]] = None


@dataclass
class OperationObservation:
    method: str
    path: str
    query_params: Set[str] = field(default_factory=set)
    request_content_types: Set[str] = field(default_factory=set)
    request_schema: Optional[Dict[str, Any]] = None
    responses: Dict[str, ResponseObservation] = field(default_factory=dict)


class EndpointCollector:
    def __init__(self) -> None:
        self.operations: Dict[Tuple[str, str], OperationObservation] = {}

    def observe_request(self, message: HTTPMessage) -> Tuple[str, str]:
        target = message.target or "/"
        parts = urlsplit(target)
        path = parts.path or "/"
        normalized_path = normalize_path(path)

        key = (message.method or "GET", normalized_path)
        operation = self.operations.get(key)
        if operation is None:
            operation = OperationObservation(method=key[0], path=key[1])
            self.operations[key] = operation

        query = parse_qs(parts.query, keep_blank_values=True)
        operation.query_params.update(query.keys())

        content_type = normalize_content_type(message.headers.get("content-type"))
        if content_type:
            operation.request_content_types.add(content_type)

        body_schema = infer_payload_schema(message.body, content_type)
        if body_schema:
            operation.request_schema = merge_schemas(operation.request_schema, body_schema)

        return key

    def observe_response(self, message: HTTPMessage, pending: Deque[Tuple[str, str]]) -> None:
        if not pending:
            return
        request_key = pending.popleft()
        operation = self.operations.get(request_key)
        if operation is None:
            return

        status = str(message.status_code or 200)
        observation = operation.responses.get(status)
        if observation is None:
            observation = ResponseObservation()
            operation.responses[status] = observation

        content_type = normalize_content_type(message.headers.get("content-type"))
        if content_type:
            observation.content_types.add(content_type)

        body_schema = infer_payload_schema(message.body, content_type)
        if body_schema:
            observation.schema = merge_schemas(observation.schema, body_schema)

    def to_openapi(self, title: str, server_url: Optional[str] = None) -> Dict[str, Any]:
        document: Dict[str, Any] = {
            "openapi": "3.0.3",
            "info": {
                "title": title,
                "version": "1.0.0",
            },
            "paths": {},
        }
        if server_url:
            document["servers"] = [{"url": server_url}]

        operation_ids: Set[str] = set()
        sorted_items = sorted(self.operations.items(), key=lambda item: (item[0][1], item[0][0]))

        for (method, path), operation in sorted_items:
            path_item = document["paths"].setdefault(path, {})
            operation_object: Dict[str, Any] = {
                "operationId": make_operation_id(method, path, operation_ids),
                "summary": f"{method} {path}",
                "responses": {},
            }

            parameters = build_parameters(path, operation)
            if parameters:
                operation_object["parameters"] = parameters

            if operation.request_schema is not None:
                request_content_types = operation.request_content_types or {"application/json"}
                operation_object["requestBody"] = {
                    "required": True,
                    "content": {
                        ct: {"schema": copy.deepcopy(operation.request_schema)}
                        for ct in sorted(request_content_types)
                    },
                }

            if operation.responses:
                for status in sorted(operation.responses.keys(), key=sort_status_code):
                    response_observation = operation.responses[status]
                    response_object: Dict[str, Any] = {
                        "description": f"Observed HTTP {status} response",
                    }
                    if response_observation.schema is not None:
                        response_content_types = response_observation.content_types or {"application/json"}
                        response_object["content"] = {
                            ct: {"schema": copy.deepcopy(response_observation.schema)}
                            for ct in sorted(response_content_types)
                        }
                    operation_object["responses"][status] = response_object
            else:
                operation_object["responses"] = {
                    "default": {
                        "description": "No response captured in traffic for this endpoint.",
                    }
                }

            path_item[method.lower()] = operation_object

        return document

    def to_flask_stub(self) -> str:
        lines: List[str] = [
            "from flask import Flask, jsonify, request",
            "",
            "app = Flask(__name__)",
            "",
        ]

        function_names: Set[str] = set()
        sorted_operations = sorted(self.operations.values(), key=lambda operation: (operation.path, operation.method))

        for operation in sorted_operations:
            route = PATH_PARAM_RE.sub(lambda match: f"<{match.group(1)}>", operation.path)
            function_name = make_function_name(operation.method, operation.path, function_names)
            path_params = list(PATH_PARAM_RE.findall(operation.path))
            args = ", ".join(path_params)

            lines.append(f"@app.route('{route}', methods=['{operation.method}'])")
            lines.append(f"def {function_name}({args}):")
            lines.append("    \"\"\"Auto-generated endpoint stub from captured traffic.\"\"\"")
            if operation.request_schema is not None:
                lines.append("    payload = request.get_json(silent=True)")
                lines.append("    _ = payload  # Keep placeholder for implementation.")
            lines.append(
                "    return jsonify({'message': 'Not implemented', 'operation': '%s'}), 501"
                % function_name
            )
            lines.append("")

        lines.extend(
            [
                "if __name__ == '__main__':",
                "    app.run(host='0.0.0.0', port=8000, debug=True)",
                "",
            ]
        )

        return "\n".join(lines)


class HttpTrafficApplication(NetworkApplication):
    collector: EndpointCollector = None  # type: ignore[assignment]

    def __init__(self, session: TCPConnectionSession, debug: bool):
        super(HttpTrafficApplication, self).__init__(session=session, debug=debug)
        if HttpTrafficApplication.collector is None:
            raise RuntimeError("HttpTrafficApplication.collector must be set before decoding")
        self.parser = HTTPStreamParser()
        self.pending: Deque[Tuple[str, str]] = deque()

    def receive(self, data: bytes):
        for message in self.parser.feed(data):
            if message.kind == "request":
                request_key = self.collector.observe_request(message)
                self.pending.append(request_key)
            elif message.kind == "response":
                self.collector.observe_response(message, self.pending)

    def finish(self):
        self.pending.clear()


def normalize_content_type(value: Optional[str]) -> str:
    if not value:
        return ""
    return value.split(";", 1)[0].strip().lower()


def normalize_path(path: str) -> str:
    path = unquote(path or "/")
    if not path.startswith("/"):
        path = "/" + path

    segments = [segment for segment in path.split("/") if segment]
    normalized: List[str] = []
    dynamic_index = 1

    for segment in segments:
        if is_dynamic_segment(segment):
            name = "id" if dynamic_index == 1 else f"id{dynamic_index}"
            normalized.append(f"{{{name}}}")
            dynamic_index += 1
        else:
            normalized.append(segment)

    return "/" + "/".join(normalized) if normalized else "/"


def is_dynamic_segment(segment: str) -> bool:
    return segment.isdigit() or bool(UUID_RE.match(segment)) or bool(LONG_HEX_RE.match(segment))


def infer_payload_schema(body: bytes, content_type: str) -> Optional[Dict[str, Any]]:
    if not body:
        return None

    parsed = parse_body(body, content_type)
    if parsed is None:
        return None
    return infer_json_schema(parsed)


def parse_body(body: bytes, content_type: str) -> Optional[Any]:
    text: Optional[str] = None

    if content_type == "application/x-www-form-urlencoded":
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError:
            return None
        parsed_query = parse_qs(text, keep_blank_values=True)
        normalized: Dict[str, Any] = {}
        for key, values in parsed_query.items():
            normalized[key] = values[0] if len(values) == 1 else values
        return normalized

    if content_type in ("application/json", "text/json") or looks_like_json(body):
        try:
            return json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None

    if content_type.startswith("text/"):
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError:
            return None
        return text

    return None


def looks_like_json(body: bytes) -> bool:
    stripped = body.strip()
    return stripped.startswith(b"{") or stripped.startswith(b"[")


def infer_json_schema(value: Any) -> Dict[str, Any]:
    if value is None:
        return {"nullable": True}
    if isinstance(value, bool):
        return {"type": "boolean"}
    if isinstance(value, int):
        return {"type": "integer"}
    if isinstance(value, float):
        return {"type": "number"}
    if isinstance(value, str):
        return {"type": "string"}
    if isinstance(value, list):
        if not value:
            return {"type": "array", "items": {}}
        item_schema: Optional[Dict[str, Any]] = None
        for item in value:
            item_schema = merge_schemas(item_schema, infer_json_schema(item))
        return {"type": "array", "items": item_schema or {}}
    if isinstance(value, dict):
        properties: Dict[str, Any] = {}
        required: List[str] = []
        for key, nested in value.items():
            properties[str(key)] = infer_json_schema(nested)
            required.append(str(key))
        schema: Dict[str, Any] = {
            "type": "object",
            "properties": properties,
        }
        if required:
            schema["required"] = sorted(required)
        return schema
    return {"type": "string"}


def merge_schemas(left: Optional[Dict[str, Any]], right: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if left is None:
        return copy.deepcopy(right)
    if right is None:
        return copy.deepcopy(left)
    if left == right:
        return copy.deepcopy(left)

    left_type = left.get("type")
    right_type = right.get("type")

    if left_type == "object" and right_type == "object":
        return merge_object_schema(left, right)
    if left_type == "array" and right_type == "array":
        merged_items = merge_schemas(left.get("items"), right.get("items"))
        return {"type": "array", "items": merged_items or {}}

    variants = []
    variants.extend(flatten_one_of(left))
    variants.extend(flatten_one_of(right))
    deduped: List[Dict[str, Any]] = []
    fingerprints: Set[str] = set()
    for variant in variants:
        fingerprint = json.dumps(variant, sort_keys=True)
        if fingerprint in fingerprints:
            continue
        fingerprints.add(fingerprint)
        deduped.append(variant)

    if len(deduped) == 1:
        return deduped[0]
    return {"oneOf": deduped}


def flatten_one_of(schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    if "oneOf" in schema and isinstance(schema["oneOf"], list):
        return [copy.deepcopy(entry) for entry in schema["oneOf"]]
    return [copy.deepcopy(schema)]


def merge_object_schema(left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
    properties: Dict[str, Any] = {}
    all_keys = set(left.get("properties", {}).keys()) | set(right.get("properties", {}).keys())
    for key in sorted(all_keys):
        properties[key] = merge_schemas(
            left.get("properties", {}).get(key),
            right.get("properties", {}).get(key),
        )

    left_required = set(left.get("required", []))
    right_required = set(right.get("required", []))
    required = sorted(left_required & right_required)

    merged: Dict[str, Any] = {
        "type": "object",
        "properties": properties,
    }
    if required:
        merged["required"] = required
    return merged


def build_parameters(path: str, operation: OperationObservation) -> List[Dict[str, Any]]:
    parameters: List[Dict[str, Any]] = []

    for name in PATH_PARAM_RE.findall(path):
        parameters.append(
            {
                "name": name,
                "in": "path",
                "required": True,
                "schema": {"type": "string"},
            }
        )

    for query_name in sorted(operation.query_params):
        parameters.append(
            {
                "name": query_name,
                "in": "query",
                "required": False,
                "schema": {"type": "string"},
            }
        )

    return parameters


def sort_status_code(status: str) -> Tuple[int, str]:
    try:
        return int(status), status
    except ValueError:
        return 10_000, status


def sanitize_identifier(value: str) -> str:
    identifier = re.sub(r"[^a-zA-Z0-9_]", "_", value)
    identifier = re.sub(r"_+", "_", identifier).strip("_")
    if not identifier:
        return "op"
    if identifier[0].isdigit():
        return f"_{identifier}"
    return identifier


def make_operation_id(method: str, path: str, used: Set[str]) -> str:
    parts = [sanitize_identifier(part.strip("{}")) for part in path.split("/") if part]
    base = sanitize_identifier("_".join([method.lower()] + (parts or ["root"])))
    candidate = base
    suffix = 2
    while candidate in used:
        candidate = f"{base}_{suffix}"
        suffix += 1
    used.add(candidate)
    return candidate


def make_function_name(method: str, path: str, used: Set[str]) -> str:
    name = make_operation_id(method, path, used)
    return f"handle_{name}"


def write_outputs(output_dir: str, collector: EndpointCollector, title: str, server_url: Optional[str]) -> Dict[str, str]:
    os.makedirs(output_dir, exist_ok=True)

    openapi_path = os.path.join(output_dir, "openapi.json")
    flask_path = os.path.join(output_dir, "generated_flask_app.py")

    openapi_document = collector.to_openapi(title=title, server_url=server_url)
    with open(openapi_path, "w", encoding="utf-8") as file:
        json.dump(openapi_document, file, indent=2, ensure_ascii=False)
        file.write("\n")

    with open(flask_path, "w", encoding="utf-8") as file:
        file.write(collector.to_flask_stub())

    return {
        "openapi": openapi_path,
        "flask_stub": flask_path,
    }


def assert_pcapng(capture_file: str) -> None:
    with open(capture_file, "rb") as file:
        magic = file.read(4)
    if magic != b"\x0a\x0d\x0d\x0a":
        raise ValueError(
            "Only pcapng captures are currently supported by this parser. "
            "Convert with tshark/editcap if your input is .pcap."
        )


def run_generator(capture_file: str, debug: bool = False) -> EndpointCollector:
    collector = EndpointCollector()
    HttpTrafficApplication.collector = collector

    shark = Wireshark(file_path=capture_file)
    shark.register_tcp_application(HttpTrafficApplication)
    shark.debug = debug
    shark.decode()
    return collector


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto-generate REST endpoint artifacts from a pcapng capture")
    parser.add_argument("--capture-file", "-f", required=True, help="Input pcapng file")
    parser.add_argument("--output-dir", "-o", default="generated", help="Directory for generated artifacts")
    parser.add_argument("--title", default="Generated API", help="OpenAPI info.title")
    parser.add_argument("--server-url", default="", help="Optional server URL for the OpenAPI document")
    parser.add_argument("--debug", "-d", action="store_true", help="Enable debug mode in packet decoder")
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    assert_pcapng(args.capture_file)

    collector = run_generator(capture_file=args.capture_file, debug=args.debug)
    outputs = write_outputs(
        output_dir=args.output_dir,
        collector=collector,
        title=args.title,
        server_url=args.server_url or None,
    )

    print("Generated artifacts:")
    print(f"- OpenAPI: {outputs['openapi']}")
    print(f"- Flask stubs: {outputs['flask_stub']}")
    print(f"- Endpoints discovered: {len(collector.operations)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
