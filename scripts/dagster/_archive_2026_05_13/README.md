# dog_beach Dagster project

Procedural orchestration for Dog Beach Scout. Pairs with `scripts/dbt/dog_beach/`
for the declarative data layer.

## What lives here

- `dog_beach/definitions.py` — root `Definitions` with assets, resources, schedules.
- `dog_beach/assets/` — Python asset definitions (Day 3+).
- `dog_beach/resources/` — DB / Supabase / dbt resources (Day 3+).

## Setup

1. **Use Python 3.12 or 3.13** — Dagster doesn't ship wheels for 3.14 yet.
   From the repo root:
   ```bash
   py -3.12 -m venv .venv-pipeline      # Windows; on macOS/Linux: python3.12 -m venv ...
   source .venv-pipeline/Scripts/activate     # Windows
   # source .venv-pipeline/bin/activate       # macOS/Linux
   pip install dbt-postgres
   pip install -e scripts/dagster/dog_beach
   ```
   The venv directory is gitignored.

2. Set DB connection env vars (same as the dbt project):
   ```
   export SUPABASE_DB_HOST=db.<ref>.supabase.co
   export SUPABASE_DB_PORT=5432
   export SUPABASE_DB_USER=postgres
   export SUPABASE_DB_PASSWORD=<service-role-password>
   export SUPABASE_DB_NAME=postgres
   ```
   Or add to `scripts/pipeline/.env`. The Definitions root auto-loads it.

3. Start the Dagit UI:
   ```bash
   dagster dev
   ```
   Opens `http://localhost:3000`. Asset graph + run history + materialize buttons.

## Project structure (target by end of Day 4)

```
dog_beach/
├── definitions.py          # Root: pulls assets, resources together
├── resources.py            # Postgres / dbt / Supabase clients
├── assets/
│   ├── __init__.py         # Aggregate all_assets list
│   ├── dbt_assets.py       # Auto-loaded from scripts/dbt/dog_beach
│   ├── ingest.py           # scrape_cdpr_park_pages, extract_operator_*
│   └── verdicts.py         # recompute_all_dogs_verdicts_by_origin
└── schedules.py            # Daily / weekly cron triggers
```

## Mapping to existing scripts

| Existing | Becomes Dagster asset |
|---|---|
| `scripts/scrape_cdpr_park_pages.py` | `cpad_unit_dogs_policy_cdpr` |
| `scripts/extract_operator_dogs_policy.py` | `operator_policy_extractions` |
| `scripts/one_off/merge_operator_dogs_policy.py` | `operator_dogs_policy` |
| `recompute_all_dogs_verdicts_by_origin()` | `beach_verdicts` |

The asset graph in Dagit will show: scrape + extract → merge → recompute.
Dependencies wired so "Materialize beach_verdicts" runs the whole upstream
chain in the right order.
