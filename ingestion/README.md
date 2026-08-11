# Ingestion connectors

Phase 2 will add bounded, retry-aware connectors for:

1. Hessen ALKIS/INSPIRE WFS parcel features.
2. OpenStreetMap Overpass grid-asset proxies.

Connectors must write raw source responses plus an ingestion manifest. They must not fetch or store
land-owner data, and they must respect source rate limits, attribution and redistribution terms.

