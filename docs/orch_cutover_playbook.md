# orch_jobs Cutover Playbook

Step-by-step cutover for the 13 jobs that remain in `shadow_mode=TRUE`.
Six were unshadowed autonomously on 2026-06-01 (the read-only audits +
idempotent backstops). The rest each have a tradeoff: double-fire risk
with active legacy pg_cron, manual-review tradition, just-patched code,
or external-cost concerns.

## General cutover pattern

```sql
-- 1. Verify orch_runs decisions look right (per job, see below)
SELECT * FROM public.orch_runs
 WHERE job_name = '<job>' ORDER BY started_at DESC LIMIT 20;

-- 2. Flip out of shadow
UPDATE public.orch_jobs SET shadow_mode = FALSE WHERE job_name = '<job>';

-- 3. Wait for next scheduled fire; verify orch_runs shows status='fired'
--    (or 'succeeded' if downstream observation reports back)

-- 4. Unschedule the legacy pg_cron entry (only after step 3 succeeds!)
SELECT cron.unschedule('<legacy_pg_cron_name>');
```

## Already unshadowed (2026-06-01)

| Job | Cadence | First will fire | Watch via |
|---|---|---|---|
| `daily_pg_cron_audit` | daily 12:35 UTC | next 12:35 UTC | `orch_runs WHERE job_name = 'daily_pg_cron_audit'` |
| `daily_codify_coverage_audit` | daily 12:30 UTC | next 12:30 UTC | same |
| `weekly_dog_park_data_quality_audit` | Sun 14:00 UTC | next Sunday | same |
| `weekly_pipeline_health` | Sun 15:00 UTC | next Sunday | same |
| `daily_dog_park_advisories` | daily 12:00 UTC | next 12:00 UTC | `dog_park_advisory` table + `orch_runs` |
| `weather_grid_inventory` | daily 04:00 UTC | next 04:00 UTC | `weather_grid` row count + `orch_runs` |

If `daily_pg_cron_audit` raises tonight or any morning, look at the
error message in `orch_jobs.last_error` for the job name + reason.

## Pending unshadow — per-job recipe

Ordered roughly by safety. Top of list is easiest.

### 1. `weather_grid_hourly` (hourly :00)

Legacy: Dagster `hourly_weather_grid_schedule` (RUNNING per recent
toggle). Dagster only fires when daemon is up.

**Cutover**
```sql
-- (no pg_cron equivalent — Dagster is the legacy)
UPDATE public.orch_jobs SET shadow_mode = FALSE WHERE job_name = 'weather_grid_hourly';

-- Next :00 UTC, watch orch_runs + supabase fn logs for refresh-weather-grid:
SELECT * FROM public.orch_runs
 WHERE job_name = 'weather_grid_hourly' ORDER BY started_at DESC LIMIT 5;

-- Once a successful fire confirmed, stop the Dagster schedule:
-- DAGSTER_HOME=/c/Users/beach/dagster_home dagster schedule stop \
--   hourly_weather_grid_schedule -w /c/Users/beach/dagster_home/workspace.yaml
```

Risk: low. The edge function is already deployed and smoke-tested
(`limit=200` cap, ~20s per fire). Worst case double-fire (Dagster +
orch) reads the same Open-Meteo data twice — wasteful but harmless.
Open-Meteo free tier is 10K requests/day; we use ~40-50.

### 2. `daily_weather_advisories` (12:00 UTC)

Legacy: Dagster `daily_weather_advisories_schedule` (RUNNING).

**Cutover** — same shape as weather_grid_hourly. Watch
`beach_advisory` rows with `raw_data->>'scoring_version' = 'v2'`.

### 3. `weekly_tide_refresh` (Sun 08:00 UTC)

Legacy: pg_cron `weekly_tide_refresh`.

**Cutover**
```sql
UPDATE public.orch_jobs SET shadow_mode = FALSE WHERE job_name = 'weekly_tide_refresh';
-- Wait for next Sunday 08:00; confirm via orch_runs
SELECT cron.unschedule('weekly_tide_refresh');
```

### 4. `process_geom_change_queue` (every 5 min)

Legacy: pg_cron `process_geom_change_queue`.

**Cutover** — flip + wait 5 min + unschedule. The queue drainer is
idempotent (advisory lock), so even double-fire just means one of two
gets the lock and the other no-ops.

### 5. `daily_refresh_scoring_tier` (07:00 UTC)

Legacy: pg_cron `daily_refresh_scoring_tier`.

**Cutover** — flip + wait until tomorrow 07:00 + unschedule. Idempotent
recompute; double-fire would be wasteful but harmless.

### 6. `verdict_cascade_nightly` (03:00 UTC)

Legacy: pg_cron `verdict_cascade_nightly`.

**Cutover** — flip + observe tomorrow 03:00 + unschedule.

### 7. `nightly_pipeline_maintenance` (04:00 UTC)

Legacy: pg_cron `nightly_pipeline_maintenance_ca`.

**Special**: was BROKEN for 5+ nights (procedure error, fixed today).
**Let it succeed at least once on the legacy pg_cron** before flipping
orch. Then unshadow + unschedule per the general pattern. The orch
version covers all MVP+ states; the legacy covers CA only.

### 8 + 9. `beach_refresh_chunked` + `dog_park_refresh_chunked` (every 2 min)

Legacy: pg_cron `beach_refresh_chunked` + `dog_park_refresh_chunked`.

**Cutover** for each:
```sql
UPDATE public.orch_jobs SET shadow_mode = FALSE WHERE job_name = 'beach_refresh_chunked';
-- Wait 2-4 min; confirm at least one orch_runs row with status='fired'
SELECT cron.unschedule('beach_refresh_chunked');
```

Risk: low. The edge function has its own internal staleness gate
(`skip_recent_hours: 22`), so double-fire just means a small number of
beaches get re-processed.

### 10 + 11. `hourly_beach_now_refresh` (:05) + `hourly_dog_park_now_refresh` (:10)

Legacy: pg_cron `hourly-beach-now-refresh` (:00) + `hourly_dog_park_now_refresh` (:05).

**Cutover** — flip + observe one hourly fire + unschedule. Note that
the orch versions are at :05 and :10 (staggered after weather grid at
:00), the legacy versions are at :00 and :05. Brief overlap but no
hard race.

### 12. `weekly_codify_gap_clone` (Mon 13:00 UTC)

Legacy: none scheduled — currently STOPPED in Dagster.

**Decision required** (not just safety). This is the only catalogued
job that mutates `beach_policy_source` automatically. You have a
tradition of manually reviewing every codify change. Flipping out of
shadow means the gap-clone tool runs weekly without review.

**To unshadow**:
```sql
UPDATE public.orch_jobs SET shadow_mode = FALSE WHERE job_name = 'weekly_codify_gap_clone';
```

Or leave shadowed and run manually:
```sql
SELECT public._orch_w_codify_gap_clone('{"states":["CA","OR","WA","MD","UT"]}'::jsonb);
```

### 13. `monthly_dog_park_coverage` (1st 08:00 UTC)

Legacy: none scheduled (the workflow's own `schedule:` block was
removed in 6bdd137).

**Cost**: ~$25-75 LLM spend per fire across MVP+. GitHub Actions
runtime is free.

**Decision required**. To unshadow:
```sql
UPDATE public.orch_jobs SET shadow_mode = FALSE WHERE job_name = 'monthly_dog_park_coverage';
```

You can also fire manually any time:
```bash
curl --ssl-no-revoke -s -X POST 'https://ehlzbwtrsxaaukurekau.supabase.co/functions/v1/dispatch-github-workflow' \
  -H "x-admin-secret: <ADMIN_SECRET>" \
  -H "Content-Type: application/json" \
  -d '{"workflow":"dp_coverage.yml","inputs":{"states":"CA"}}'
```

## After all 13 are unshadowed

These pg_cron entries should remain (they're independent of orch):
- `orch_tick` (drives the orchestrator itself)
- `quarterly_populate_operators` (procedure-with-commits, can't run from orch's function context)

Everything else in `cron.job` should have its orch_jobs equivalent
firing instead. Final audit:

```sql
SELECT j.jobname, j.schedule, j.active
  FROM cron.job j
 WHERE j.jobname NOT IN ('orch_tick', 'quarterly_populate_operators')
 ORDER BY j.jobname;
```

If anything still shows up here after the cutover, either it's a
legacy entry you forgot to unschedule, or it's a new pg_cron entry
you've added that should also be cataloged in orch_jobs.
