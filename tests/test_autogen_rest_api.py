import json
import unittest
from collections import deque

from autogen_rest_api import (
    EndpointCollector,
    HTTPStreamParser,
    infer_json_schema,
    merge_schemas,
    normalize_path,
)


class HTTPStreamParserTests(unittest.TestCase):
    def test_parses_request_and_response_with_fragmented_input(self):
        parser = HTTPStreamParser()
        payload = (
            b"POST /users/42?include=profile HTTP/1.1\r\n"
            b"Host: api.example.com\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: 16\r\n"
            b"\r\n"
            b'{"name":"Raell"}'
            b"HTTP/1.1 201 Created\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: 11\r\n"
            b"\r\n"
            b'{"ok":true}'
        )

        messages = []
        messages.extend(parser.feed(payload[:45]))
        messages.extend(parser.feed(payload[45:120]))
        messages.extend(parser.feed(payload[120:]))

        self.assertEqual(2, len(messages))
        request = messages[0]
        response = messages[1]

        self.assertEqual("request", request.kind)
        self.assertEqual("POST", request.method)
        self.assertEqual("/users/42?include=profile", request.target)

        self.assertEqual("response", response.kind)
        self.assertEqual(201, response.status_code)


class EndpointCollectorTests(unittest.TestCase):
    def test_collects_openapi_fields_from_observed_messages(self):
        parser = HTTPStreamParser()
        collector = EndpointCollector()
        pending = deque()

        stream = (
            b"GET /orders/123?limit=10 HTTP/1.1\r\n"
            b"Host: api.example.com\r\n\r\n"
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: 15\r\n\r\n"
            b'{"total": 10.5}'
        )

        for message in parser.feed(stream):
            if message.kind == "request":
                pending.append(collector.observe_request(message))
            else:
                collector.observe_response(message, pending)

        doc = collector.to_openapi("Captured API")
        self.assertIn("/orders/{id}", doc["paths"])
        get_operation = doc["paths"]["/orders/{id}"]["get"]
        parameters = get_operation["parameters"]

        self.assertTrue(any(param["name"] == "id" and param["in"] == "path" for param in parameters))
        self.assertTrue(any(param["name"] == "limit" and param["in"] == "query" for param in parameters))
        self.assertIn("200", get_operation["responses"])

    def test_normalize_path_dynamic_segments(self):
        self.assertEqual("/users/{id}/devices/{id2}", normalize_path("/users/42/devices/99"))


class SchemaTests(unittest.TestCase):
    def test_merge_object_schema_intersects_required_fields(self):
        left = infer_json_schema({"a": 1, "b": "x"})
        right = infer_json_schema({"a": 2, "c": True})

        merged = merge_schemas(left, right)
        self.assertEqual("object", merged["type"])
        self.assertEqual(["a"], merged["required"])
        self.assertCountEqual(["a", "b", "c"], merged["properties"].keys())


if __name__ == "__main__":
    unittest.main()
