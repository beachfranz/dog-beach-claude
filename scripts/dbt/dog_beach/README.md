# dog_beach dbt project

Declarative data layer for Dog Beach Scout. Pairs with `scripts/dagster/dog_beach/`
for procedural orchestration.

## What lives here vs. Dagster

- **dbt** — declarative transformations: SELECT-shaped models with a clean
  lineage graph. Sources, staging, marts.
- **Dagster** — procedural orchestration: scrapers, LLM extractions, the
  per-row verdict cascade, recompute drivers. Wraps existing Python and
  SQL functions as assets.

## Setup

1. **Use Python 3.12 or 3.13** — Dagster doesn't ship wheels for 3.14 yet,
   so we standardize on a 3.12 venv for both projects. From the repo root:
   ```bash
   py -3.12 -m venv .venv-pipeline           # Windows
   source .venv-pipeline/Scripts/activate    # Windows; macOS/Linux: source .venv-pipeline/bin/activate
   pip install dbt-postgres
   ```
   The venv is gitignored.

2. Set DB connection env vars (from Supabase Dashboard → Settings → Database):
   ```
   export SUPABASE_DB_HOST=db.<ref>.supabase.co
   export SUPABASE_DB_PORT=5432
   export SUPABASE_DB_USER=postgres
   export SUPABASE_DB_PASSWORD=<service-role-password>
   export SUPABASE_DB_NAME=postgres
   ```
   Or add them to `scripts/pipeline/.env` and source it before running.

3. From this directory:
   ```bash
   dbt debug              # verify connection
   dbt deps               # install package deps (none yet)
   dbt parse              # syntax-check
   dbt run                # build all models
   dbt docs generate      # build the lineage HTML
   dbt docs serve         # localhost:8080 viewer
   ```

## Layout

- `models/sources.yml` — declarations of public-schema tables. Day 1.
- `models/staging/` — passthrough views per source. Day 2-3.
- `models/marts/` — joined / aggregated marts. Day 3.
- `macros/` — reusable SQL helpers (e.g., wraps for our PL/pgSQL functions).

## Schemas

- Sources: `public` (read-only)
- Outputs: `dbt` schema in the linked Supabase project (created automatically).

## Conventions

- Source references: `{{ source('public', 'beach_locations') }}`
- Model references: `{{ ref('stg_beach_locations') }}`
- Spatial functions: import via macros so we can wrap PostGIS calls consistently.
