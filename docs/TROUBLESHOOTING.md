# Troubleshooting

## No Endpoints Generated

Symptoms:
- `Endpoints discovered: 0`

Checks:
- Confirm input file is `pcapng`.
- Confirm traffic includes HTTP/1.x payloads over TCP.
- Confirm traffic is not encrypted TLS without decryption.

## Unexpected Path Parameters

Symptoms:
- Path segments converted to `{id}` unexpectedly.

Cause:
- Numeric, UUID-like, and long-hex segments are normalized as dynamic identifiers.

Action:
- Review normalization logic in `normalize_path` and tune for your domain conventions.

## Missing Request/Response Schema

Symptoms:
- Operation present but no requestBody/content schema.

Cause:
- Body absent or non-JSON/non-form payload.

Action:
- Capture complete payloads and ensure supported content types.

## Hook Script Issues

If README generation fails in hooks:
- Verify `git config --get remote.origin.url` returns a valid remote URL.
- Ensure repository metadata is accessible in local git context.
