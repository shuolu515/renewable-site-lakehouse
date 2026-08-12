# Databricks Bronze walkthrough

This walkthrough loads the first immutable Hessen parcel snapshot into
`workspace.bronze.parcels_raw`.

## What this step demonstrates

- Unity Catalog schemas and a managed volume
- PySpark ingestion of a multiline GeoJSON `FeatureCollection`
- Raw-payload and provenance retention
- Data-contract checks before a write
- Idempotent Delta Lake `MERGE`
- Delta table history for auditability

## Files to upload

Use the two local files created by `python ingestion/fetch_parcels.py`:

```text
data/raw/hessen_alkis_inspire_wfs/<ingestion-id>/parcels.geojson
data/raw/hessen_alkis_inspire_wfs/<ingestion-id>/manifest.json
```

The first project snapshot uses ingestion id
`bad4b270-a420-4075-b159-2775966077da`.

Do not add these raw files to Git. Upload them directly to Databricks.

## Run the notebook

1. Import `notebooks/01_load_parcels_bronze.py` into the Databricks workspace.
2. Run the setup cells through the cell that prints `Upload directory`.
3. In **Catalog**, open `workspace` > `landing` > `Volumes` > `raw`.
4. Create this directory structure inside the volume:

   ```text
   hessen_alkis_inspire_wfs/bad4b270-a420-4075-b159-2775966077da/
   ```

5. Upload `parcels.geojson` and `manifest.json` to that directory.
6. Return to the notebook and run all remaining cells.

The full input path is:

```text
/Volumes/workspace/landing/raw/hessen_alkis_inspire_wfs/
bad4b270-a420-4075-b159-2775966077da/
```

## Expected result

The table `workspace.bronze.parcels_raw` contains 100 rows for the first ingestion. Each row keeps
the complete source feature in `raw_feature_json` and links it to the manifest through
`ingestion_id`, source, license, request and checksum fields.

Run the notebook a second time with the same ingestion id. The table should still contain 100 rows
for that ingestion because the Delta merge key is `(ingestion_id, feature_id)`.

## Troubleshooting

- **Cannot create schema or volume**: confirm the notebook is running in the workspace where you
  created the foundation table and that the `catalog` widget is `workspace`.
- **Path does not exist**: run the setup cells first, then upload both files to the exact printed
  directory.
- **Feature count mismatch**: do not mix a manifest from one ingestion with the GeoJSON from
  another ingestion.
- **Personal-data check fails**: do not bypass it. Only the declared public, non-owner-data source
  belongs in this project.

## Load the OSM grid snapshot

The second Bronze notebook loads the uploaded OSM snapshot into
`workspace.bronze.grid_assets_raw`.

1. Confirm these files exist in the volume:

   ```text
   /Volumes/workspace/landing/raw/openstreetmap_overpass/
   4db321da-72b6-44ae-8d8f-6d3470e85f57/grid_assets.json
   /Volumes/workspace/landing/raw/openstreetmap_overpass/
   4db321da-72b6-44ae-8d8f-6d3470e85f57/manifest.json
   ```

2. Import `notebooks/02_load_grid_assets_bronze.py` into the Databricks workspace.
3. Run all cells twice.

Both runs should report 163 rows, 163 distinct elements and 163 records with unknown capacity.
The asset summary should report 120 substations, 8 transformers, 27 lines and 8 minor lines.

The composite merge key is `(ingestion_id, element_key)`, where `element_key` combines the OSM
element type and id, for example `node/123`. This prevents a node and way with the same numeric id
from being treated as the same asset.
