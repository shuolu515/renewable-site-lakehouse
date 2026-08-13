# Databricks Silver walkthrough

## Parcel transformation

Import `notebooks/03_transform_parcels_silver.py` after both Bronze notebooks have completed. The
notebook reads one parcel ingestion from `workspace.bronze.parcels_raw` and creates:

- `workspace.silver.parcels` for records that pass every technical quality rule
- `workspace.silver.invalid_parcels` for rejected records and their explicit failure reasons

No file upload is required because Silver reads the managed Bronze Delta table.

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
