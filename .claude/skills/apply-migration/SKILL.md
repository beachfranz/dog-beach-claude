---
name: apply-migration
description: Use this skill when applying a SQL migration to the Supabase database via the CLI. Triggers include "apply this migration", "apply the migration", "run the migration", "land this in DB", drafting a new `supabase/migrations/*.sql` file and needing to apply it, or asking how to land a schema change safely. Encodes filename convention + apply command + post-apply verification + cascade propagation if dog-policy-touching + verify-sweep trigger if consumer-facing. DO NOT use for ad-hoc one-off SQL queries (just run `supabase db query --linked "..."` directly) or for dbt model changes (use `dbt build`, not migrations).
---

# apply-migration — land a SQL migration safely

The migration ritual is small but error-prone in specific ways. This skill encodes the steps so nothing gets skipped — especially the post-apply propagation that today's [[claim-tested-without-end-state-verification]] miss reminded us is mandatory.

## Step 1 — Filename + location

```
supabase/migrations/YYYYMMDD_<description>.sql
```

- `YYYYMMDD` = today's date (or the date the migration was originally drafted if part of a sequence)
- `<description>` = snake_case summary, e.g. `phase14_pad_us_resolver_rewrite`, `photo_source_type_nhsp`, `bep_field_group_constraint`
- Avoid filename clashes; numbered phases use suffixes (`20260604_phase14a_resolver.sql`, `_phase14b_*.sql`)

For phased migrations that depend on prior steps, ORDER MATTERS — `ls` lists alphabetically, so `20260604_phase14a` runs before `_phase14b`.

## Step 2 — Idempotency hygiene

Per Supabase convention, every migration should be idempotent or guarded:

| Operation | Idempotent form |
|---|---|
| `CREATE TABLE` | `CREATE TABLE IF NOT EXISTS` |
| `CREATE INDEX` | `CREATE INDEX IF NOT EXISTS` |
| `ALTER TABLE … ADD COLUMN` | `ALTER TABLE … ADD COLUMN IF NOT EXISTS` |
| `INSERT INTO config_table VALUES (…)` | `INSERT … ON CONFLICT (pk) DO UPDATE SET …` |
| `CREATE OR REPLACE FUNCTION` | already idempotent |
| `DROP TABLE` / `DROP COLUMN` | use `IF EXISTS` |

This matters because re-applying after a partial failure is the usual recovery path. If half the migration ran and half didn't, re-running should be safe.

## Step 3 — Apply via Supabase CLI

```bash
supabase db query --linked -f supabase/migrations/<filename>.sql
```

That's the canonical command. The `--linked` flag uses the project's linked DB. Output should end with `SUCCESS` or whatever the last statement returned.

If applying a multi-file phase in order:
```bash
for f in supabase/migrations/20260604_phase14*.sql; do
  echo "=== $f ==="
  supabase db query --linked -f "$f" || break
done
```
The `|| break` halts on first error so you don't apply phase14c on top of a failed phase14b.

## Step 4 — Verify rows / structure landed

Pick a verification SQL matching what the migration did:

| Migration type | Verify |
|---|---|
| `CREATE TABLE` | `SELECT count(*) FROM information_schema.tables WHERE table_name='<name>'` |
| `ADD COLUMN` | `SELECT column_name FROM information_schema.columns WHERE table_name='<t>' AND column_name='<c>'` |
| `CREATE FUNCTION` | `\df+ <fnname>` via `supabase db query --linked "..."` |
| Backfill `UPDATE` | row count, e.g. `SELECT count(*) FROM t WHERE <new column> IS NOT NULL` |
| Insert into config table | `SELECT id, label FROM <table> WHERE id='<new id>'` |
| Constraint add | `SELECT conname FROM pg_constraint WHERE conname='<name>'` |

Skipping this step is the [[claim-tested-without-end-state-verification]] anti-pattern. "Apply returned 0 errors" ≠ "the change landed correctly." E.g., a `CREATE INDEX IF NOT EXISTS` with a typo in the column name returns SUCCESS but creates no index.

## Step 5 — Propagate if cascade-touching

If the migration writes to any of:
- `beach_enrichment_provenance` (BEP)
- `policy_source` / `beach_policy_source`
- `operator_dogs_policy` / `cpad_unit_dogs_policy` / `pad_us_unit_dogs_policy`
- `entity_operator` / `beach_operator`

…then run the propagation cascade for affected fids:

```sql
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT DISTINCT <fid_column> AS fid FROM <table> WHERE <recently-touched filter>
  LOOP
    PERFORM public.compute_beach_field_consensus(r.fid);
    PERFORM public.promote_canonical_dogs_to_beach_dog_policy(r.fid);
  END LOOP;
END
$$;
```

Without this step, the migration writes rows but `beach_dog_policy` (the consumer surface) doesn't see them. This is the canonical "data is in the DB but the page doesn't show it" symptom.

For dog parks: `PERFORM public.promote_canonical_dog_park_policy(<dog_park_fid>);`

## Step 6 — Redeploy edge functions if signatures changed

If the migration changes function signatures, column names that edge functions SELECT, or RPCs that edge functions call — per [[edge-function-stale-deploy-first]], redeploy the affected edge functions:

```bash
scripts/deploy_edge_function.sh <fn-name>
```

Otherwise you'll get the "edge function returns 200 but data doesn't land" symptom and waste an hour debugging logic that's actually correct in source.

**Always use `scripts/deploy_edge_function.sh`**. A bare `supabase functions deploy` returns 401 silently because the publishable key isn't a JWT — see CLAUDE.md "Supabase CLI" section. This pattern took out daily-beach-refresh for 4 days.

## Step 7 — Verify-sweep if consumer-facing

If the migration affected what `beach.html` / `dog-park.html` / `find.html` / `index.html` / `detail.html` read, fire the `verify-sweep` skill. Click-through is mandatory per [[claim-tested-without-end-state-verification]] — SQL counts alone don't prove the cascade landed on the consumer surface.

Skip the verify-sweep if the migration is purely backend (Dagster asset, internal table, audit-only).

## Step 8 — Commit + clean tmp

If the migration was draft (not yet committed):
```bash
git add supabase/migrations/<filename>.sql
git commit -m "<short message describing why>"
```
Per [[merge-workflow]], don't push to remote unless Franz explicitly asks. Don't auto-merge to main.

## Common gotchas

1. **Forgetting Step 5 propagation** — most common bug. Cascade writes land but consumer surface doesn't update. Fix: run the DO-block.
2. **`supabase functions deploy` instead of `scripts/deploy_edge_function.sh`** — silently returns 401. Always use the wrapper.
3. **Migration applies but `psql` connection lost mid-statement** — Supabase pooler session-mode disconnects on idle. Re-run; ON CONFLICT clauses handle the partial state.
4. **`CREATE TABLE` without `IF NOT EXISTS`** — re-apply fails on existing table.
5. **`ALTER TABLE … DROP COLUMN` on a column edge functions still SELECT** — edge function 500s on next call. Either redeploy edge functions FIRST or apply migration during maintenance window.
6. **Backfill UPDATE without WHERE clause** — touches every row. Always include a `WHERE` to scope.
7. **PowerShell `Get-Content` returns PSObjects** for fid lists — pipe through `[string[]]@(... | ForEach-Object { $_.Trim() })` before JSON-serializing.

## Per Franz preferences

- [[no-unilateral-architectural-decisions]] — don't drop/relax constraints without asking. Schema is load-bearing.
- [[claim-tested-without-end-state-verification]] — verify the data is on the consumer surface, not just in the DB. Steps 4-7 are mandatory.
- [[never-solve-same-problem-twice]] — the propagation cascade is the encoded solution. Don't skip it.
- [[use-pipeline-infrastructure]] — for state-launch migrations, the canonical pipeline handles ordering.
- [[merge-workflow]] — never auto-merge to main; don't push to remote without explicit approval.
