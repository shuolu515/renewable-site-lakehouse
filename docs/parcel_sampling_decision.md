# Parcel sampling decision

## Problem

The first Wiesbaden snapshot contained only the first 100 records from a query matching 41,759
parcels. Its median area was 476 square metres and no parcel met the 20,000 square metre screening
threshold. That page was not a representative basis for a Hessen-wide business conclusion.

## Revised MVP boundary

The pipeline now uses a bounded rural area in Limburg-Weilburg:

```text
min_lon: 8.150
min_lat: 50.425
max_lon: 8.175
max_lat: 50.450
```

The boundary was selected after comparing several bounded official API samples. It is small enough
for a complete educational snapshot while containing both large standalone parcels and smaller
parcels that can later support a land-pooling experiment.

## Pagination behavior

The connector follows the official OGC API `next` links, treats their URLs as opaque and restricts
them to the configured HTTPS host. It also enforces a local maximum-feature limit and rejects
duplicate parcel identifiers across pages.

During validation on 2026-08-14, the source reported 973 matching records but exposed only 972
records across five pages. The final response contained a stale `next` link and the next offset
returned HTTP 404. The connector therefore also stops on a short final page and records both the
reported and accessible counts in the manifest instead of hiding the discrepancy.

## Validation result

The accessible 972-record snapshot contained:

- total parcel area: 6,195,654 square metres
- median parcel area: 1,580 square metres
- 90th percentile area: 15,315 square metres
- maximum parcel area: 292,464 square metres
- parcels at or above 20,000 square metres: 71
- parcels at or above 50,000 square metres: 11

These results support two separate Gold screening paths: rank standalone parcels that already meet
the area threshold, and evaluate spatially adjacent smaller parcels as possible land pools. Neither
path establishes planning permission, owner consent, engineering feasibility or grid capacity.
