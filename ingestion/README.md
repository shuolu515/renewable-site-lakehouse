# Ingestion connectors

Phase 2 includes bounded, retry-aware connectors for:

1. Hessen ALKIS/INSPIRE WFS parcel features.
2. OpenStreetMap Overpass grid-asset proxies.

Connectors must write raw source responses plus an ingestion manifest. They must not fetch or store
land-owner data, and they must respect source rate limits, attribution and redistribution terms.

Run the connectors from the repository root:

```powershell
python ingestion/fetch_parcels.py
python ingestion/fetch_grid_assets.py
```

The parcel connector follows server-provided OGC API `next` links until the bounded snapshot is
complete or the configured safety limit is reached. Its manifest records the number of pages,
reported matches, accessible records, count consistency and truncation status. See
`docs/parcel_sampling_decision.md` for the current MVP boundary and validation evidence.

The grid connector requests only electricity features inside the configured bounding box, limits
the response size and excludes OSM contributor metadata. Its output is a public screening proxy,
not evidence of remaining grid capacity or a viable connection point.

