# Development log

## 2026-08-11 - Foundation started

### Completed

- Defined the business boundary and MVP acceptance criteria.
- Selected Databricks, PySpark, Delta Lake, SQL and Power BI as the core portfolio stack.
- Created an independent repository scaffold instead of copying the reference implementation.
- Added initial source metadata, contracts, scoring configuration and pure scoring tests.

### Decisions

- Start with one bounded area around Wiesbaden.
- Use only freshly acquired public data in the new implementation.
- Keep grid capacity `unknown` unless an official or client-confirmed source is added.
- Keep complete raw downloads out of Git until redistribution terms are confirmed.

### Next

1. Validate Databricks Free Edition with a minimal Delta table.
2. Implement the bounded ALKIS parcel connector.

### Blockers

- Databricks workspace registration/login must be completed by the project owner.

## 2026-08-12 - First real parcel ingestion

### Completed

- Confirmed the official Hessen `cp:CadastralParcel` OGC API Features endpoint.
- Confirmed the source license as Datenlizenz Deutschland - Zero - Version 2.0.
- Implemented a bounded, configurable HTTP connector with retry and response validation.
- Added immutable raw GeoJSON output and a provenance manifest with run ID, timestamps, request
  parameters, source/license metadata, schema summary and SHA-256 checksum.
- Added mocked HTTP tests for valid requests, unsupported limits, transient errors and non-JSON
  responses.
- Ran the first real Wiesbaden ingestion: 100 Polygon features, HTTP 200, one attempt.

### Verification

- `pytest`: 7 passed.
- `ruff check`: passed.
- `ruff format --check`: passed.
- Raw snapshot size: 160,428 bytes.
- The raw snapshot and manifest remain local and are ignored by Git.

### Decisions

- Use only limits accepted by the official API: 1, 5, 10, 20, 50, 100, 200 or 500.
- Keep the first snapshot at 100 records to reduce load and make the learning pipeline easy to
  inspect.
- Treat the public parcel service as geometry and cadastral metadata only; never enrich it with
  owner or other personal data.

### Next

1. Load the immutable GeoJSON snapshot into the first Bronze Delta table.
2. Add ingestion metadata columns without modifying the raw payload.

### Blockers

- None for Bronze ingestion.

