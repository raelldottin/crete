# Output Specification

## Generated Files

- `openapi.json`: OpenAPI 3.0.3 document generated from captured traffic.
- `generated_flask_app.py`: Flask scaffold with route handlers for each discovered operation.

## OpenAPI Structure

Top-level fields:
- `openapi`: `3.0.3`
- `info.title`: configurable via `--title`
- `info.version`: fixed `1.0.0`
- `servers`: optional, from `--server-url`
- `paths`: generated operation map

Per operation:
- `operationId`: deterministic, unique identifier
- `summary`: `<METHOD> <PATH>`
- `parameters`: includes path params and observed query params
- `requestBody`: generated when request body schema is inferred
- `responses`: observed statuses with inferred body schema when available

Fallback behavior:
- If no response is captured for an operation, a `default` response placeholder is emitted.

## Flask Stub Structure

For each operation:
- Route decorator with normalized path
- Handler named `handle_<operation_id>`
- Optional request JSON placeholder
- Default `501 Not Implemented` response

## Determinism

Output ordering is stable by sorted path and method, enabling cleaner diffs in version control.

## Consumption Recommendations

- Import `openapi.json` into Swagger UI, Postman, or code generators for downstream integration.
- Treat scaffold code as a starting point; it intentionally does not include business logic.
