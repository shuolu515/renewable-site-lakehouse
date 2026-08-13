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

## Bronze grid-asset grain

`bronze.grid_assets_raw` contains one row per OSM element and ingestion run. The composite key is
`(ingestion_id, element_key)`, and the complete source element remains in `raw_element_json`.

| Field | Type | Required | Rule |
|---|---|---:|---|
| `ingestion_id` | string | yes | UUID and equal to the snapshot manifest |
| `element_key` | string | yes | `<OSM type>/<OSM id>`, unique within one ingestion |
| `osm_element_type` | string | yes | `node` or `way` |
| `osm_element_id` | long | yes | source-provided OSM identifier |
| `power_tag` | string | yes | substation, transformer, line or minor_line |
| `raw_element_json` | string | yes | complete OSM element serialized as JSON |
| `source_id` | string | yes | `openstreetmap_overpass` |
| `source_license` | string | yes | ODbL 1.0 recorded by the manifest |
| `source_license_url` | string | yes | canonical OSM license/copyright reference |
| `source_attribution` | string | yes | `© OpenStreetMap contributors` |
| `request_bbox_json` | string | no | bounded request extent serialized as JSON |
| `request_limit` | integer | no | configured maximum result count |
| `payload_sha256` | string | yes | checksum of the original response |
| `source_file` | string | yes | Unity Catalog volume path |
| `capacity_status` | string | yes | always `unknown` in the MVP |
| `source_completed_at` | timestamp | no | source ingestion completion time |
| `loaded_at` | timestamp | yes | Bronze write time |

## Parcel contract

`silver.parcels` has one row per parcel and ingestion. Only records with no technical quality
errors enter this table. `silver.invalid_parcels` retains rejected Bronze payloads and an array of
machine-readable failure reasons.

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

Additional Silver fields:

| Field | Type | Required | Rule |
|---|---|---:|---|
| `national_cadastral_reference` | string | no | source-provided public reference |
| `parcel_label` | string | no | source-provided display label |
| `source_area_uom` | string | yes | must equal `m2` |
| `centroid_lat` | double | yes | inside the source request bounding box |
| `centroid_lon` | double | yes | inside the source request bounding box |
| `meets_minimum_area` | boolean | yes | business flag; false does not mean invalid data |
| `quality_status` | string | yes | `passed` in `silver.parcels` |
| `transformed_at` | timestamp | yes | Silver processing time |

Parcel quarantine error codes:

- `missing_parcel_id`
- `source_key_mismatch`
- `duplicate_parcel_id`
- `missing_area`
- `non_positive_area`
- `unexpected_area_unit`
- `unsupported_geometry_type`
- `missing_geometry`
- `centroid_outside_request_bbox`

## Grid asset contract

`silver.grid_assets` contains standardized OSM electricity assets that pass the technical quality
rules. `silver.invalid_grid_assets` preserves rejected source elements and their failure reasons.

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

Additional Silver fields:

| Field | Type | Required | Rule |
|---|---|---:|---|
| `source_power_tag` | string | yes | original OSM power tag |
| `geometry_type` | string | yes | Point, LineString or Polygon |
| `centroid_lat` | double | yes | inside the bounded source request |
| `centroid_lon` | double | yes | inside the bounded source request |
| `voltage_raw` | string | no | original OSM voltage tag |
| `voltage_level_kv` | double | no | maximum parsed voltage converted from volts to kV |
| `asset_name` | string | no | optional OSM name tag |
| `quality_status` | string | yes | `passed` in `silver.grid_assets` |
| `transformed_at` | timestamp | yes | Silver processing time |

Grid-asset quarantine error codes:

- `missing_grid_asset_id`
- `source_key_mismatch`
- `duplicate_grid_asset_id`
- `unsupported_osm_element_type`
- `unsupported_power_tag`
- `missing_geometry`
- `insufficient_geometry_points`
- `invalid_voltage`
- `centroid_outside_request_bbox`
- `unexpected_capacity_status`
- `bronze_source_mismatch`

## Site assessment grain

One row per `parcel_id` and `assessment_date`.

Required measures include gross area, estimated usable area, estimated PV MWp, nearest grid asset,
distance, score components, total score, red flags, data-quality status and capacity confidence.

