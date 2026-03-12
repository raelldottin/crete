# Architecture

## Objectives

The generator is designed to derive a practical API contract from observed network behavior with minimal assumptions.

## Component Model

- `Wireshark` (`wireshark.py`): parses `pcapng` block structure and decodes IPv4/TCP packet payloads.
- `HttpTrafficApplication` (`autogen_rest_api.py`): receives TCP payload bytes and forwards to the HTTP stream parser.
- `HTTPStreamParser`: reconstructs request/response messages from fragmented TCP streams.
- `EndpointCollector`: aggregates endpoint observations and pairs responses to pending requests.
- Artifact writers: serialize OpenAPI and Flask stub outputs.

## Data Flow

1. Load capture file and iterate blocks.
2. For each TCP packet with payload, feed bytes into application session.
3. Parse HTTP start-line, headers, and body.
4. Record request observation (method, path, query, request schema).
5. Record response observation (status, content type, response schema).
6. Merge repeated observations by operation key.
7. Emit deterministic artifacts.

## Operation Identity

Operation key format:
- `(<HTTP_METHOD>, <normalized_path>)`

Path normalization rules:
- Percent-decode incoming path.
- Ensure leading `/`.
- Replace numeric, UUID-like, and long-hex segments with templated params.

Example:
- `/users/42/orders/af34...` -> `/users/{id}/orders/{id2}`

## Schema Inference

Inference strategy is sample-driven:

- JSON objects -> object schemas with inferred field types
- JSON arrays -> array schemas with merged item schemas
- URL-encoded forms -> object schema from key-value fields
- Text payloads -> string schema
- Multiple observed variants -> merged using `oneOf`

Required field semantics:
- For merged object schemas, required keys are the intersection of observed required sets.

## Pairing Strategy

Responses are paired to requests via a per-stream FIFO queue of pending request keys.
This is simple and deterministic for common request-response traffic patterns.

## Tradeoffs

- Prioritizes operational simplicity over perfect protocol reconstruction.
- Does not attempt semantic business inference beyond observable payload structure.
- OpenAPI output should be reviewed before production contract adoption.
