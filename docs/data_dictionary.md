# Data dictionary

## Bronze parcel grain

`bronze.parcels_raw` contains one row per source feature and ingestion run. The composite key is
`(ingestion_id, feature_id)`. `raw_feature_json` retains the complete source feature; the other
columns provide lineage and allow an ingestion to be validated without reparsing the manifest.

| Field | Type | Required | Rule |
|---|---|---:|---|
| `ingestion_id` | string | yes | UUID and equal to the snapshot manifest |
| `feature_id` | string | yes | unique within one ingestion |
| `source_id` | string | yes | `hessen_alkis_inspire_wfs` for this table |
| `collection_id` | string | yes | source collection identifier |
| `geometry_type` | string | no | source-provided geometry type |
| `raw_feature_json` | string | yes | complete, unmodified GeoJSON feature serialized as JSON |
| `source_license` | string | yes | license recorded by the manifest |
| `source_license_url` | string | yes | canonical license reference |
| `request_bbox` | string | no | bounded source request extent |
| `request_limit` | integer | no | requested maximum feature count |
| `payload_sha256` | string | yes | checksum of the original response |
| `source_file` | string | yes | Unity Catalog volume path |
| `source_completed_at` | timestamp | no | source ingestion completion time |
| `loaded_at` | timestamp | yes | Bronze write time |

## Parcel contract

| Field | Type | Required | Rule |
|---|---|---:|---|
| `parcel_id` | string | yes | non-empty and unique in the source snapshot |
| `geometry_json` | string | yes | GeoJSON Polygon or MultiPolygon |
| `municipality` | string | no | normalized display name |
| `land_use` | string | no | controlled value or `unknown` |
| `source_area_m2` | double | no | greater than zero when present |
| `source_id` | string | yes | must exist in the source register |
| `ingestion_id` | string | yes | UUID for the ingestion run |
| `ingested_at` | timestamp | yes | UTC timestamp |

## Grid asset contract

| Field | Type | Required | Rule |
|---|---|---:|---|
| `grid_asset_id` | string | yes | non-empty and unique in the source snapshot |
| `asset_type` | string | yes | substation, transformer or line |
| `geometry_json` | string | yes | valid GeoJSON geometry |
| `voltage_level_kv` | double | no | positive when present |
| `operator_name` | string | no | source-provided proxy metadata |
| `source_id` | string | yes | `openstreetmap_overpass` for the MVP |
| `capacity_status` | string | yes | `unknown` for the MVP |
| `ingestion_id` | string | yes | UUID for the ingestion run |
| `ingested_at` | timestamp | yes | UTC timestamp |

## Site assessment grain

One row per `parcel_id` and `assessment_date`.

Required measures include gross area, estimated usable area, estimated PV MWp, nearest grid asset,
distance, score components, total score, red flags, data-quality status and capacity confidence.

