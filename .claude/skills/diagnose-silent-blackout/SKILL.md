---
name: diagnose-silent-blackout
description: Use this skill when the consumer surface looks stale or empty but no error is visible — edge function returns 200 with empty data, a cron is "running" but the page doesn't reflect changes, scoring hasn't updated in days, BestTime/NOAA/weather data is mysteriously missing, or a daily-refresh appears to skip beaches silently. Triggers include "the data looks stale", "best window isn't updating", "page is blank but no error", "cron ran but nothing changed", "X hasn't been refreshed in days", "silent blackout". DO NOT use when there IS a visible error (just read the error and fix); use for the specific class where everything LOOKS fine but data isn't flowing.
---

# diagnose-silent-blackout — incident ladder for "running but nothing changed"

The hardest class of bug is the one with no error. The edge function returns 200, the cron `last_run` ticks every hour, the orchestrator phases pass — but the consumer page is stale. Today's pipeline-silent-blackout (cron `depends_on` pointed at disabled `weather_grid_hourly`) and the 4-day daily-beach-refresh blackout (deployed-with-bare-`supabase functions deploy`, 401 on every call but no UI surface) are the canonical examples.

This skill encodes the diagnostic ladder so the next blackout takes 5 min, not 4 days.

## The 6-rung ladder (climb in order)

### Rung 1 — Is the edge function actually responding?

```bash
curl -s -X POST 'https://ehlzbwtrsxaaukurekau.supabase.co/functions/v1/<fn-name>' \
  -H "Authorization: Bearer $SUPABASE_SERVICE_KEY" \
  -H "Content-Type: application/json" \
  -d '{}' | head -c 200
```

If you see `UNAUTHORIZED_INVALID_JWT_FORMAT`, the function was deployed via bare `supabase functions deploy` without `--no-verify-jwt`. **This took out daily-beach-refresh for 4 days (2026-05-30 → 2026-06-03).** Fix:

```bash
scripts/deploy_edge_function.sh <fn-name>
```

If you see `{"error": "Could not find ... in the schema cache"}` or a column-not-found error referencing a renamed/dropped table, the deploy is stale per [[edge-function-stale-deploy-first]]. Same fix: redeploy via the wrapper script.

If you see actual JSON data, function is fine — climb to rung 2.

### Rung 2 — Is the cron firing?

```sql
SELECT jobname, schedule, active, last_run_started_at, last_run_finished_at,
       last_run_status, last_run_message
FROM cron.job
LEFT JOIN cron.job_run_details ON cron.job_run_details.jobid = cron.job.jobid
WHERE jobname LIKE '%<fn-name>%'
ORDER BY last_run_started_at DESC
LIMIT 5;
```

Check:
- `active = true`? If false, someone (or a migration) disabled it.
- `last_run_started_at` recent? If hours behind schedule, pg_cron itself is broken.
- `last_run_status = 'succeeded'`?

If the cron is firing successfully but data isn't changing, climb to rung 3.

### Rung 3 — Does the cron's command target current tables?

Cron-pipeline `depends_on` is today's bug. Cron jobs invoke edge functions via `pg_net.http_post`. The cron's SQL command may reference tables/RPCs that have been renamed or dropped — the cron silently no-ops and reports `succeeded`.

```sql
SELECT jobname, command FROM cron.job WHERE jobname LIKE '%<fn-name>%';
```

Look at the `command` SQL. If it references a table or function that's been renamed/dropped (like today's `weather_grid_hourly` → `weather_grid_t1`), re-point it in a migration.

If the cron command looks correct, climb to rung 4.

### Rung 4 — Is the edge function returning data but failing to write?

If function logs are accessible (Supabase dashboard → Functions → Logs):

- Look for `INSERT ... ON CONFLICT` rows count
- Look for early returns (e.g., `if (beaches.length === 0) return ...`)
- Look for try/catch swallowing errors silently

Common patterns:
- Filter list is empty (`is_scoreable=true` set to false on every row by accident)
- Body parsing fails silently (POST body missing or wrong shape)
- Service role key env var missing (function falls back to anon, gets RLS'd out)

If function is actually writing rows, climb to rung 5.

### Rung 5 — Is the propagation cascade running?

For dog-policy-touching writes: BEP rows land but `beach_dog_policy` (consumer surface) doesn't update unless `compute_beach_field_consensus + promote_canonical_dogs_to_beach_dog_policy` runs per affected fid.

For scoring writes: `beach_day_hourly_scores` lands but `beach_day_recommendations` (daily rollup) doesn't update unless the rollup populator runs.

Check the time gap:
```sql
SELECT max(updated_at) FROM public.beach_enrichment_provenance WHERE source='<recent source>';
SELECT max(updated_at) FROM public.beach_dog_policy WHERE arena_group_id IN (<affected fids>);
```

If BEP is recent but `beach_dog_policy` is older, propagation didn't fire. Manually run the DO-block (see `apply-migration` skill Step 5).

If propagation IS running, climb to rung 6.

### Rung 6 — Is the consumer page caching stale data?

- Browser cache: hard-refresh (Ctrl+Shift+R)
- CDN: GitHub Pages serves `main` directly with no CDN cache for HTML
- Edge function response cache: Supabase edge functions don't cache by default
- IndexedDB / localStorage: some pages stash data — clear the relevant key

If none of these apply, the data IS landing on the consumer surface. Re-verify with `verify-sweep`.

## Today's blackouts — the receipts

### Daily-beach-refresh 4-day blackout (2026-05-30 → 2026-06-03)

- Symptom: best windows stopped updating; users would have seen 4-day-old recommendations
- Diagnosis: function returned `UNAUTHORIZED_INVALID_JWT_FORMAT` on every call
- Root cause: deployed via bare `supabase functions deploy daily-beach-refresh` without `--no-verify-jwt`
- Detection: caught by orch response reconciler (NEW post-blackout)
- Fix: redeploy via `scripts/deploy_edge_function.sh daily-beach-refresh`
- Encoded: CLAUDE.md "Supabase CLI" section + [[edge-function-stale-deploy-first]]

### Cron-pipeline silent blackout (2026-06-04)

- Symptom: cron jobs firing but downstream data not landing
- Diagnosis: `depends_on` SQL pointed at `weather_grid_hourly` which was disabled
- Root cause: refactor renamed the table to `weather_grid_t1` but the cron `depends_on` SQL was never updated
- Detection: ad-hoc audit; no automated catcher
- Fix: re-pointed depends_on in migration; added T2 Open-Meteo retry/backoff
- Encoded: today's `dingospank` safeword + (proposed) cron-audit depends_on health check

### `get-beaches-find` stale deploy (2026-05-25)

- Symptom: find.html chip surgery looked broken — beaches not showing
- Diagnosis: 15 min chasing chip dispatcher / leash filter / distance filter
- Root cause: function deployment was stale, referenced `public.location_day_hourly_scores` (renamed away)
- Fix: `supabase functions deploy get-beaches-find --no-verify-jwt`
- Encoded: [[edge-function-stale-deploy-first]]

## The shape of silent blackouts

These all share a structural property: **a layer between intent and effect silently drops the work, with the upstream layer reporting success.**

| Layer | Failure mode |
|---|---|
| Edge function (bare deploy) | 401 silently; cron logs `succeeded` |
| Cron `depends_on` | SQL returns 0 rows; cron logs `succeeded` |
| Stale deploy referencing dropped column | 500 silently; consumer sees empty array |
| Propagation cascade not running | BEP lands; `beach_dog_policy` stays stale; consumer reads old data |
| RLS-blocked SELECT | empty response; no error |
| Service role key env var missing | function falls back to anon; RLS blocks; empty response |

The cure for the entire class is **end-state verification** per [[claim-tested-without-end-state-verification]]: don't trust upstream success signals. Verify on the consumer surface (browser, `beach.html?fid=...`) that the change actually landed.

## What to check FIRST when Franz says "looks stale"

In order, ~30 sec each:

1. **Hard-refresh the page** (Ctrl+Shift+R). Rules out browser cache.
2. **Open dev tools → Network tab**. Check the failing request. 4xx/5xx? Empty array? Stale data?
3. **If empty array / stale**: rung 1 (curl the function directly). If 401 → redeploy via wrapper.
4. **If function works**: rung 5 (propagation cascade). Compare BEP timestamp to `beach_dog_policy` timestamp.

90% of "looks stale" cases resolve at rung 1 or 5.

## Anti-patterns

- **Don't immediately debug the source code** when a function looks broken. Rung 1 (redeploy) takes 30 sec; debugging takes 30 min. Today's [[edge-function-stale-deploy-first]] receipt is the canonical case.
- **Don't trust `cron.job_run_details.last_run_status='succeeded'`** as evidence the work happened. The cron may have run a no-op SQL command successfully.
- **Don't add retry-backoff** as a fix for silent blackouts. Retries don't help when the function is structurally broken (401, stale schema). Retries are for transient failures.
- **Don't fix one rung without checking the others**. Today's cron-pipeline fix at rung 3 also revealed a need for rung 1 reconciler.

## Per Franz preferences

- [[claim-tested-without-end-state-verification]] — the parent rule. SQL counts don't prove the consumer surface works. CLICK the page.
- [[edge-function-stale-deploy-first]] — the rung-1 reflex. Redeploy is a CHECK, not a fix.
- [[never-solve-same-problem-twice]] — silent-blackout patterns repeat unless encoded. Today's pipeline-silent-blackout would not have been a multi-hour diagnosis if the cron-audit depends_on check existed (next parking-lot item).
- [[regular-data-quality-audits]] — surveillance > forensics. Build Dagster crons that watch for "data hasn't flowed in N hours" and alert.
