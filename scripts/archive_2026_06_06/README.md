# Archived 2026-06-06 — SQL-native cutover

Four Python scripts retired after the day's SQL-native refresh build-out
(see pin `[[advisories-sql-native]]`). All replaced by equivalent SQL
functions or edge functions running on hourly orch_jobs schedules.

| Script | Replaced by | Commit |
|---|---|---|
| `compute_weather_advisories.py` | `public.refresh_beach_advisories()` SQL fn | `c0d2e8e` |
| `compute_dog_park_advisories.py` | `public.refresh_dog_park_advisories()` SQL fn | `c0d2e8e` |
| `compute_marine_advisories.py` | `public.refresh_marine_advisories()` SQL fn (via `marine_grid_hourly`) | `58828c6` → `a49ffe0` (grid cutover) |
| `fetch_marine_forecast.py` | `refresh-marine-grid` edge function | `a7661fd` |

Performance gain — Python ~3.5h aggregate runtime → SQL/edge ~100s
aggregate, single-transaction, immune to pooler disconnects.

**Why archive instead of delete**: kept around for rollback ergonomics
and as reference for the per-fid loop pattern (template documented in
`project_advisories_sql_native.md`). Do NOT re-run any of these against
production — they would write to legacy tables (`beach_marine_forecast`
for fetch_marine_forecast) or thrash the SQL fns' `fetched_at` watermark
sweep.
