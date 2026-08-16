# Databricks Gold walkthrough

Import `notebooks/05_build_gold_site_assessment.py` after both Silver notebooks have completed. No
additional raw files or manual tables are required. The notebook creates:

- `workspace.gold.dim_parcel`
- `workspace.gold.dim_grid_asset`
- `workspace.gold.dim_data_source`
- `workspace.gold.fact_site_assessment`

The fact table contains one row per parcel and assessment date. The current notebook defaults to
parcel ingestion `98c5af3d-dc45-481f-bebf-9a6d7979672e` and grid ingestion
`1bd38dbc-8a46-4453-ab9c-1713966d2f54`.

For every parcel, the notebook:

- selects the nearest public grid proxy using Haversine distance between Silver centroids;
- calculates transparent land, grid, data-quality and planning score components;
- uses Databricks spatial SQL to find other parcels that directly intersect or share a boundary;
- assigns one of three screening categories:
  - `standalone_candidate`: the parcel alone meets the minimum-area rule;
  - `land_pool_opportunity`: the parcel is too small alone, but its area plus all direct neighbors
    reaches the minimum-area rule;
  - `below_threshold`: neither of the preceding area conditions is met.

Run the notebook twice on the same day. On both runs:

- `silver_parcel_count` must equal `fact_row_count`
- `fact_row_count` must equal `distinct_parcel_count`
- `silver_parcel_count` should be 970 after the topology check quarantines two invalid polygons
- `standalone_candidate_count` should be positive and no greater than the 71 large parcels observed
  before topology validation
- fact and dimension counts must not increase on the second run
- all total scores must remain in the range 0 to 100
- grid capacity must remain `unknown`

## Interpretation boundary

`land_pool_opportunity` is an exploratory adjacency signal, not a completed land pool. It does not
confirm common ownership, compatible land use, planning permission, access or willingness to
assemble the parcels. Direct-neighbor opportunities can overlap and should be investigated rather
than summed as independent projects.

The nearest OSM asset is a location proxy, not a confirmed connection point. Distance is calculated
to the asset centroid, including for lines and polygons. The estimated usable area, PV capacity and
scores are screening assumptions for prioritization, not engineering or planning conclusions.
