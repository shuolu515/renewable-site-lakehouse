# Architecture

## Goal

Build a small, reproducible medallion lakehouse that turns heterogeneous public geospatial data
into explainable renewable-site screening records.

## Data flow

1. Python connectors fetch bounded source payloads and write immutable raw files.
2. Bronze Delta tables retain raw payloads, source metadata and ingestion IDs.
3. Silver PySpark jobs parse schemas, standardize units, deduplicate business keys and quarantine
   invalid records.
4. Gold jobs match parcels to grid proxies, calculate transparent score components and write a
   star schema for Power BI.
5. A pipeline-run table records counts, status, duration and rejected records.

## MVP tables

### Bronze

- `bronze.parcels_raw`
- `bronze.grid_assets_raw`
- `bronze.source_register`
- `bronze.pipeline_runs`

### Silver

- `silver.parcels`
- `silver.grid_assets`
- `silver.invalid_parcels`
- `silver.invalid_grid_assets`

### Gold

- `gold.dim_parcel`
- `gold.dim_grid_asset`
- `gold.dim_data_source`
- `gold.fact_site_assessment`

## Trust boundary

The nearest public grid proxy is useful for pre-screening only. It is not a confirmed connection
point and provides no evidence of remaining capacity. `capacity_status` stays `unknown` until a
lawful, official source or client-confirmed record is introduced.

