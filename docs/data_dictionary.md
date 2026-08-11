# Data dictionary

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

