# Power BI dashboard specification

## Data sources

Load these views from the Databricks SQL warehouse and rename them in Power BI:

| Databricks view | Power BI table | Grain |
|---|---|---|
| `workspace.gold.vw_powerbi_site_candidates` | `SiteCandidates` | one row per current parcel assessment |
| `workspace.gold.vw_powerbi_red_flags` | `RedFlags` | one row per assessment and red flag |

Create a one-to-many relationship from `SiteCandidates[assessment_id]` to
`RedFlags[assessment_id]`. Use single-direction filtering from `SiteCandidates` to `RedFlags`.

Use Import mode for the portfolio MVP. The dataset is small, and Import mode keeps interaction fast
without requiring the Databricks SQL warehouse to run for every visual click.

## Verified current-snapshot baseline

| Metric | Value |
|---|---:|
| Current parcel assessments | 970 |
| Standalone candidates | 71 |
| Land-pool opportunities | 436 |
| Below threshold | 463 |
| Shortlisted standalone parcels | 71 |

All 970 records currently carry `grid_capacity_unknown` and `planning_status_unknown`. The 436
land-pool opportunities are overlapping adjacency signals, not 436 independent or approved projects.

## Page 1: Screening overview

- Cards: Parcel Count, Standalone Candidates, Land Pool Opportunities, Shortlisted Parcels.
- Donut chart: parcel count by `candidate_type`.
- Scatter chart: `gross_area_m2` by `nearest_grid_distance_m`, sized by `estimated_pv_mwp` and
  coloured by `candidate_type`.
- Ranked table: `screening_rank`, `parcel_label`, `candidate_type`, `gross_area_m2`,
  `nearest_grid_distance_m`, `total_score`, `eligible_for_shortlist`.
- Slicers: `candidate_type`, `eligible_for_shortlist`, `nearest_grid_asset_type`.

## Page 2: Candidate map

- Azure Maps visual using `centroid_lat` and `centroid_lon`.
- Bubble size: `estimated_pv_mwp`.
- Legend: `candidate_type`.
- Tooltips: parcel label, total score, gross area, grid distance, nearest grid type and planning
  status.

The map shows parcel centroids, not parcel boundaries. It is suitable for regional screening, not
engineering or cadastral interpretation.

## Page 3: Confidence and limitations

- Bar chart: red-flag occurrences by `red_flag` from `RedFlags`.
- Cards: Unknown Grid Capacity and the count of records with `planning_status = "unknown"`.
- Table: candidate, capacity status, capacity confidence, planning status and red-flag count.
- Text box: public OSM assets are proximity proxies; the report does not confirm grid capacity,
  ownership, planning permission or engineering feasibility.

## Measures

Create the measures in `powerbi/measures.dax` under the `SiteCandidates` table. Format areas and
distances as whole numbers with thousands separators, MWp and scores with two decimals, and counts
as whole numbers.
