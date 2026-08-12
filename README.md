# Renewable Site Lakehouse

A portfolio-focused data engineering project for pre-screening German renewable-energy sites.
The project ingests public parcel and grid-proxy data, builds Bronze/Silver/Gold Delta tables,
generates explainable candidate-site scores, and serves business-ready tables to Power BI.

> This is a screening and prioritization tool. It does not confirm grid capacity, planning
> permission, land ownership, or engineering feasibility.

## MVP scope

- Region: a bounded area around Wiesbaden, Germany
- Sources: Hessen ALKIS/INSPIRE WFS and OpenStreetMap Overpass
- Processing: Databricks, PySpark, Spark SQL and Delta Lake
- Serving: Gold star schema and Power BI
- Governance: source register, ingestion audit, data-quality quarantine and explicit confidence
- Capacity rule: public grid proxies always start with `capacity_status = unknown`

## Architecture

```text
ALKIS WFS + OSM Overpass
          |
     Python ingestion
          |
   Bronze Delta tables
          |
 PySpark validation/standardization ----> quarantine tables
          |
    Silver Delta tables
          |
 scoring + Gold star schema
          |
        Power BI
```

## Repository structure

```text
config/       bounded source and pipeline configuration
docs/         architecture, contracts, decisions and development log
ingestion/    source connectors (implemented in Phase 2)
notebooks/    Databricks Bronze/Silver/Gold notebooks
src/          reusable, testable business logic
tests/        local unit and data-contract tests
powerbi/      dashboard documentation; PBIX files are ignored by default
```

## Current status

Phase 1 - Foundation is in progress.

- [x] Independent repository scaffold
- [x] MVP scope and architecture
- [x] Initial data contract and source register
- [x] Explainable scoring configuration and unit-testable scoring logic
- [x] Databricks Free Edition workspace validation
- [x] Fresh public-data ingestion
- [ ] Bronze/Silver/Gold Delta implementation
- [ ] Power BI dashboard

## Local setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
python -m pytest
```

PySpark and Delta processing will run in Databricks. Local Python is used for connector tests,
configuration validation and pure business rules.

## Fetch the first parcel snapshot

The connector uses the bounded Wiesbaden configuration in `config/pipeline.yml`. Complete raw
downloads are ignored by Git; every run writes an auditable manifest next to its response.

```powershell
python ingestion/fetch_parcels.py
```

Expected output:

```text
data/raw/hessen_alkis_inspire_wfs/<ingestion-id>/parcels.geojson
data/raw/hessen_alkis_inspire_wfs/<ingestion-id>/manifest.json
```

The official API exposes a public subset without owner data. Do not add owner information or other
personal data to this project.

## Data and licensing

Do not commit complete raw downloads until redistribution terms are confirmed. Never ingest or
publish land-owner or other personal data. See `config/data_sources.yml` and
`docs/data_dictionary.md` before adding a connector.

## Roadmap

1. Validate a minimal Delta table in Databricks.
2. Implement a bounded ALKIS parcel connector.
3. Implement a bounded OSM grid-asset connector.
4. Build Bronze and Silver Delta tables with quarantine handling.
5. Build the Gold site-assessment fact and dimensions.
6. Create the Power BI dashboard, verify the public repository, and publish to GitHub.

