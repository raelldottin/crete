# Operations Guide

## Runtime Requirements

- Python 3.9+
- Input capture in `pcapng` format

## Standard Runbook

1. Place capture file in a controlled workspace.
2. Execute generator with explicit output directory.
3. Validate generated OpenAPI document for expected endpoints.
4. Commit artifacts after review.

Example:

```bash
python3 autogen_rest_api.py \
  --capture-file ./captures/service_traffic.pcapng \
  --output-dir ./artifacts/api \
  --title "Service API (Observed)" \
  --server-url "https://api.internal.example"
```

## CI Integration Pattern

Recommended checks for pull requests:

1. Run unit tests.
2. Run syntax validation.
3. If fixture captures are stored in-repo, regenerate artifacts and assert no unintended diff.

## Versioning Strategy

Suggested approach:
- Tag generated OpenAPI snapshots by environment and date.
- Track meaningful schema changes via PR review.
- Promote contract versions only after endpoint owner validation.

## Failure Handling

Common failures:
- Invalid capture magic number -> input is not `pcapng`
- Empty outputs -> no parsable HTTP traffic found
- Partial endpoints -> traffic incomplete or encrypted

Mitigation:
- Re-capture with full request/response visibility.
- Decrypt traffic upstream where policy allows.
- Combine captures from multiple test flows.
