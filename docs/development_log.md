# Development log

## 2026-08-11 - Foundation started

### Completed

- Defined the business boundary and MVP acceptance criteria.
- Selected Databricks, PySpark, Delta Lake, SQL and Power BI as the core portfolio stack.
- Created an independent repository scaffold instead of copying the reference implementation.
- Added initial source metadata, contracts, scoring configuration and pure scoring tests.

### Decisions

- Start with one bounded area around Wiesbaden.
- Use only freshly acquired public data in the new implementation.
- Keep grid capacity `unknown` unless an official or client-confirmed source is added.
- Keep complete raw downloads out of Git until redistribution terms are confirmed.

### Next

1. Validate Databricks Free Edition with a minimal Delta table.
2. Implement the bounded ALKIS parcel connector.

### Blockers

- Databricks workspace registration/login must be completed by the project owner.

