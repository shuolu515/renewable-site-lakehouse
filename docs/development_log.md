# Development log

## 2026-08-13 - Grid-asset Silver transformation completed

### Completed

- Added typed parsing for OSM identifiers, asset types, geometry, centroids and voltage levels.
- Added Point, LineString and Polygon GeoJSON construction for node and way elements.
- Added deterministic source-consistency, geometry, voltage, coordinate and capacity rules.
- Added valid and quarantine Delta targets with idempotent merge keys.
- Added in-memory failure-path assertions that do not write synthetic project records.
- Verified Bronze-to-Silver row accounting in Databricks.
- Ran the notebook twice and confirmed stable target counts.

### Decisions

- Normalize `minor_line` to analytical type `line` while preserving `source_power_tag`.
- Keep optional OSM voltage, operator and name tags nullable.
- Preserve `capacity_status = unknown`; public OSM assets do not prove connection capacity.

### Next

1. Build the Gold site-screening model from the two standardized Silver datasets.
2. Expose business-ready measures for Power BI.

### Blockers

- None.

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

## 2026-08-12 - Parcel Bronze ingestion completed

### Completed

- Added Unity Catalog schema and managed-volume setup for raw landing files.
- Added multiline GeoJSON parsing that retains each complete feature as raw JSON.
- Added manifest, feature-count, identifier uniqueness and non-PII checks before writing.
- Added an idempotent Delta merge keyed by ingestion and feature identifiers.
- Added run-level verification and Delta history output.
- Executed the notebook twice in Databricks; both runs returned 100 rows and 100 distinct feature
  identifiers for the first ingestion.

### Decisions

- Use `workspace.landing.raw` for file-based landing data and `workspace.bronze` for managed Delta
  tables.
- Keep Bronze close to the source: only expose the feature id and geometry type needed for lineage
  and validation; defer business parsing to Silver.
- Do not overwrite prior ingestion runs. A repeated run of the same snapshot inserts no duplicates.

### Next

1. Implement the bounded OSM grid-asset connector as the next independent feature.
2. Load grid-asset snapshots into their Bronze Delta table.

### Blockers

- None.

## 2026-08-13 - Parcel Silver transformation completed

### Completed

- Added typed parsing for parcel identifiers, area, geometry and centroid coordinates.
- Added deterministic key, type, geometry, unit, area and coordinate-range quality rules.
- Added valid and quarantine Delta tables with idempotent merge keys.
- Added in-notebook synthetic checks that exercise failure paths without polluting project tables.
- Verified Bronze-to-Silver row accounting in Databricks.
- Ran the notebook twice and confirmed stable target counts.

### Decisions

- Keep technically valid small parcels in Silver and expose `meets_minimum_area` as a business flag.
- Preserve the original Bronze payload in quarantine so rejected rows remain debuggable.
- Use machine-readable quality error codes rather than a single pass/fail flag.

### Next

1. Implement the grid-asset Silver transformation and quarantine.
2. Build the Gold screening model after both Silver datasets are standardized.

### Blockers

- None.

## 2026-08-12 - Grid-asset Bronze ingestion completed

### Completed

- Added PySpark parsing for heterogeneous OSM node and way elements.
- Added manifest, element-count, unique-key, supported-tag and capacity-status checks.
- Retained complete raw elements, request lineage, SHA-256, ODbL license and attribution.
- Added an idempotent Delta merge keyed by ingestion id and OSM element key.
- Executed the notebook twice in Databricks; both runs returned 163 rows, 163 distinct elements
  and 163 records with unknown capacity.

### Decisions

- Combine element type and numeric id because OSM node and way identifier spaces are independent.
- Keep `capacity_status = unknown` in Bronze and prevent any other value from being written.
- Parse business fields such as voltage and operator only in Silver; Bronze remains source-oriented.

### Next

1. Start Silver parsing and data-quality quarantine.

### Blockers

- None.

## 2026-08-12 - OSM grid-proxy ingestion implemented

### Completed

- Added a bounded Overpass query for substations, transformers and power lines.
- Added response validation, retry handling, immutable raw output and a provenance manifest.
- Recorded ODbL licensing and the required `© OpenStreetMap contributors` attribution.
- Excluded contributor metadata from the query and limited each response to 500 assets.
- Preserved `capacity_status = unknown` as an explicit trust boundary.
- Ran the first real Wiesbaden ingestion: 163 grid proxies, HTTP 200, one attempt.

### Verification

- `pytest`: 12 passed.
- `ruff check`: passed.
- `ruff format --check`: passed.
- Asset mix: 120 substations, 8 transformers, 27 lines and 8 minor lines.
- Element mix: 102 nodes and 61 ways.
- Raw snapshot size: 81,007 bytes.
- Raw SHA-256: `caf508596242881bd7f8e4de0eb9402d46f0979cc310aa095e34830e994d4c1d`.

### Decisions

- Use OSM only as a public proximity and asset-type proxy.
- Query one small Wiesbaden bounding box and use exponential retry for temporary HTTP failures.
- Store source tags in Bronze later, but never infer operator approval or available grid capacity.

### Next

1. Load the grid snapshot into `bronze.grid_assets_raw`.

### Blockers

- None.

