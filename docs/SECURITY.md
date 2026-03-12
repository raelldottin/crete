# Security and Data Handling

## Data Classification

Packet captures may contain:
- Authentication tokens
- Personal data
- Internal service metadata
- Proprietary request/response payloads

Treat captures and generated artifacts as sensitive by default.

## Handling Controls

- Run processing in a trusted environment.
- Restrict file permissions on capture and output directories.
- Avoid committing raw captures to public repositories.
- Redact sensitive examples before sharing documentation.

## Logging and Exposure

- Avoid running with debug output in shared terminals when captures contain sensitive payloads.
- Generated OpenAPI examples and inferred schemas can reveal internal field names.

## Encryption Considerations

- HTTPS traffic cannot be inferred without decryption at capture/source level.
- Only process decrypted data when policy and legal controls explicitly permit it.

## Recommended Enterprise Controls

- Integrate secret scanning on generated artifacts.
- Add retention policies for capture files.
- Store artifacts in access-controlled repositories.
- Document approval workflows for contract publication.
