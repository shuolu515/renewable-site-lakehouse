# Databricks Silver walkthrough

## Parcel transformation

Import `notebooks/03_transform_parcels_silver.py` after both Bronze notebooks have completed. The
notebook reads one parcel ingestion from `workspace.bronze.parcels_raw` and creates:

- `workspace.silver.parcels` for records that pass every technical quality rule
- `workspace.silver.invalid_parcels` for rejected records and their explicit failure reasons

No file upload is required because Silver reads the managed Bronze Delta table.

The transformation also converts source coordinates such as `"8.156836"` into numeric GeoJSON
coordinates such as `8.156836`. When the same ingestion is rerun, the Delta merge updates existing
Silver rows whose normalized geometry changed; no table deletion is required.
It also restores coordinate pairs that Spark serializes as nested JSON strings when Polygon and
MultiPolygon records are inferred together.

The source bbox is an intersection filter: a parcel can intersect the requested area while its
centroid lies just outside it. Silver therefore validates centroids against WGS84 coordinate ranges,
not against the request bbox. If a previously quarantined row becomes valid after a rule correction,
the rerun removes that stale quarantine row automatically.

If the source omits `properties.pos`, as it does for the six MultiPolygon records in the current
snapshot, Silver derives a deterministic centre from the geometry bounding box. This fallback is
used for distance screening and is not represented as a source-provided cadastral centroid.

Silver also applies the OGC topology check used by the downstream spatial join. For the current
snapshot, 970 parcels pass and two topologically invalid polygons are quarantined. A rerun moves
rows between the valid and quarantine tables when a corrected quality rule changes their status.

Run the notebook twice. On both runs:

- `bronze_count` must equal `accounted_count`
- the Silver and quarantine counts must not increase on the second run
- every quarantined record must contain one or more `quality_errors`

The notebook also runs two in-memory invalid records through the transformation function. They
exercise missing-key, duplicate-key, negative-area, unit, geometry and coordinate-range rules but
are never written to project tables.

## Data quality versus business eligibility

A parcel smaller than 20,000 square metres is technically valid and remains in Silver. It receives
`meets_minimum_area = false` for later screening. Quarantine is reserved for malformed or
untrustworthy data, not valid data that happens to be unsuitable for the business use case.

## Grid-asset transformation

Import `notebooks/04_transform_grid_assets_silver.py` after the grid Bronze notebook has completed.
It creates `workspace.silver.grid_assets` and `workspace.silver.invalid_grid_assets`. No additional
file upload is required.

The transformation converts OSM nodes to GeoJSON points, line ways to line strings and substation
ways to polygons. It normalizes `minor_line` to the analytical type `line` while preserving the
original power tag. Optional OSM voltage, operator and name tags remain nullable.

Run the notebook twice and verify that `bronze_count` equals `accounted_count` on both runs, and
that the Silver and quarantine counts do not increase on the second run.
