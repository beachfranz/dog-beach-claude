# GitHub Actions — External Runner for orch_jobs

GitHub Actions is the external runner for orch_jobs catalog rows whose work
needs real Python (Playwright, Anthropic LLM, psycopg2 with long-running
queries, etc.). Supabase Edge Functions can't host these — Deno + 150s
timeout + no Python.

## Architecture

```
orch_tick (pg_cron, every minute)
    │
    ▼
orch_jobs catalog row
  worker_kind   = 'edge_function'
  worker_target = 'dispatch-github-workflow'
  worker_payload = { workflow, ref, inputs }
    │
    ▼
dispatch-github-workflow  (Supabase Edge Function)
  POST → GitHub workflow_dispatch API
    │
    ▼
GitHub Actions runner (ubuntu-latest, ~6h limit, ~14GB RAM)
  runs the workflow steps (Python, Playwright, etc.)
    │
    ▼
writes results back to Supabase via standard scripts/common/db.py
```

## Current workflows

| Workflow file | Used by orch_jobs row | Dispatch pattern |
|---|---|---|
| `deploy.yml` | (not orch-dispatched) | push to main → deploy edge fns |
| `dp_coverage.yml` | `monthly_dog_park_coverage` | orch + monthly schedule backstop |

## Setup: one-time secrets

### Step 1 — GitHub repo secrets

`Settings → Secrets and variables → Actions → New repository secret`:

| Secret | Purpose |
|---|---|
| `SUPABASE_URL` | https://ehlzbwtrsxaaukurekau.supabase.co |
| `SUPABASE_SERVICE_KEY` | service role key (from Supabase Dashboard → Project Settings → API) |
| `SUPABASE_DB_PASSWORD` | DB password (Project Settings → Database) |
| `ANTHROPIC_API_KEY` | for LLM-using ops (dog_park_pipeline extract / retry) |
| `ADMIN_SECRET` | same value as `ADMIN_SECRET` in edge fns |
| `GOOGLE_PLACES_API_KEY` | optional — ingest_queue geocoding |

### Step 2 — Supabase edge function secrets

`Supabase Dashboard → Edge Functions → Secrets → Add new secret`:

| Secret | Purpose |
|---|---|
| `GH_DISPATCH_TOKEN` | GitHub PAT or fine-grained token with `actions:write` scope on `beachfranz/dog-beach-claude` |

To create the GitHub token:
- **Classic PAT**: `Settings → Developer settings → Personal access tokens
  → Tokens (classic) → Generate new token`. Scopes: `workflow`. Expiry: as
  long as comfortable (1 year suggested; rotate before expiry).
- **Fine-grained token** (preferred for least privilege): `Settings →
  Developer settings → Personal access tokens → Fine-grained tokens →
  Generate new token`. Repository access: only `beachfranz/dog-beach-claude`.
  Permissions → Repository permissions → Actions: Read and write.

### Step 3 — Smoke test the dispatch

Once both secrets are set, fire the workflow manually via the edge fn:

```bash
curl --ssl-no-revoke -s -X POST 'https://ehlzbwtrsxaaukurekau.supabase.co/functions/v1/dispatch-github-workflow' \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk" \
  -H "apikey: sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk" \
  -H "x-admin-secret: <ADMIN_SECRET>" \
  -d '{"workflow":"dp_coverage.yml","inputs":{"states":"CA","only_ops":"preflight,pip_address_city"}}'
```

Should return `{"dispatched": true, ...}` and a new run appears in
`Actions → Dog Park Coverage Refresh`.

## Adding new GitHub-hosted jobs

1. Create `.github/workflows/<name>.yml` with `workflow_dispatch` trigger
   (and an optional `schedule` backstop)
2. Add the secrets the workflow needs to the repo
3. Update or insert an `orch_jobs` row:
   ```sql
   UPDATE public.orch_jobs
      SET worker_kind   = 'edge_function',
          worker_target = 'dispatch-github-workflow',
          worker_payload = jsonb_build_object(
            'workflow', '<name>.yml',
            'inputs',   jsonb_build_object('foo', 'bar')
          )
    WHERE job_name = '<job_name>';
   ```

## Why not just run Dagster on GitHub Actions?

Could. We chose not to because:
- Dagster wants a long-running daemon, not one-shot script runs
- The orch_jobs unified scheduler is simpler than Dagster + GH Actions cron
- This pattern works for any one-shot Python script, not just Dagster assets
