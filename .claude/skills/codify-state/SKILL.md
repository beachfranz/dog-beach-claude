---
name: codify-state
description: Use this skill when running the codify pipeline for a US state to produce per-jurisdiction policy_source rows with verbatim ordinance citations. Triggers include "codify <state>", "run derive_policy_source_for_jurisdiction for <state>", "codify the OR coastal counties", "start codify for FL", or asking for next steps after a codify --pilot returns defer_stubborn / success_human_review outcomes. DO NOT use for per-beach extraction (use phase-10b-extract), per-park extraction (use dog-park-state-launch), or for ad-hoc one-off URL discovery on a single jurisdiction (just run the script directly). Codify is the BULK pass; defers must be handled with the rescue playbook per [[script-defers-dispatch-agents]].
---

# codify-state — bulk per-jurisdiction policy_source ingestion

Pipes municipal/county/state/federal dog-policy ordinances into `policy_source` rows with verbatim cite text, then bridges to beaches via the resolver cascade. Driver: `scripts/derive_policy_source_for_jurisdiction.py` (the BULK pass). Rescue path: playbook agent dispatch per `docs/jurisdiction_policy_source_playbook.md` (the RESCUE pass).

The hardest part isn't writing code — it's not blurring the two layers. The script's `defer_stubborn` outcome is the SIGNAL to escalate to playbook agents, not the signal to tweak the script.

## Step 1 — Preflight

```sql
-- How many beaches in this state? (skip codify if 0 per [[beach-catalog-prereq-for-codify]])
SELECT count(*) FROM public.beaches_gold WHERE state = '<STATE>' AND is_active;

-- How many jurisdictions have ≥1 beach inside?
SELECT count(DISTINCT j.gid)
FROM public.jurisdictions j
JOIN public.beaches_gold b ON ST_DWithin(j.geom, b.geom, 0.0018)  -- 200m per [[pip-for-places-uses-200m]]
WHERE b.state = '<STATE>' AND b.is_active;

-- Existing ps coverage in this state
SELECT count(*) FROM public.policy_source ps
JOIN public.jurisdictions j ON j.gid = ps.jurisdiction_gid
WHERE j.statefp = (SELECT statefp FROM public.states WHERE state_code = '<STATE>');
```

If beach count is 0, STOP — codify needs an inventory to attribute to. Run GNIS / OSM ingestion first.

## Step 2 — Smoke test with --pilot

```bash
.venv-pipeline/Scripts/python.exe scripts/derive_policy_source_for_jurisdiction.py \
  --state <STATE> --pilot 5
```

The `--pilot 5` flag picks 5 jurisdictions covering the state's classification distribution (city, county, state agency, federal, tribal). Watch the JSONL output for outcomes:

| Outcome | Meaning | Next step |
|---|---|---|
| `success_auto_commit` | Migration generated + ready to apply | Apply via supabase db query --linked |
| `success_human_review` | URL found but needs human eyeball before commit | **Dispatch playbook agent** (do NOT investigate manually) |
| `defer_stubborn` | Script exhausted Phase A + Step 6.8 web_search | **Dispatch playbook agent** (do NOT tweak script) |
| `defer_no_jurisdiction` | Jurisdiction has 0 beaches; correctly skipped | Ignore |
| `defer_federal_branch` | Federal — wait for federal branch implementation | Park |
| `defer_tribal` | Tribal lands need CPRA request | Park |
| `fail_<reason>` | Crash or unexpected — investigate | Investigate the script (this IS a script-side issue) |

## Step 3 — Run the bulk pass

```bash
.venv-pipeline/Scripts/python.exe scripts/derive_policy_source_for_jurisdiction.py \
  --state <STATE> > tmp/codify_<state>_bulk.log 2>&1 &
```

Estimate ~20-40 jurisdictions/hour. The log will show one JSONL row per jurisdiction. Tail via `wc -l` not `tail -f` because Python buffers when redirected.

Monitor DB-side:

```sql
SELECT count(*) FROM public.policy_source ps
JOIN public.jurisdictions j ON j.gid = ps.jurisdiction_gid
WHERE j.statefp = (SELECT statefp FROM public.states WHERE state_code = '<STATE>')
  AND ps.created_at > now() - interval '4 hours';
```

## Step 4 — Handle defers via playbook AGENT dispatch

This is the most-skipped step. The HARD rule [[script-defers-dispatch-agents]] exists because today's-pattern (recently again 2026-05-18) is to spend 30+ min "investigating the script" or "doing slug recon" when the script's defer IS the signal that bulk-pass exhausted its tools and rescue-pass is needed.

When the bulk pass completes, extract defer'd jurisdictions:

```bash
grep -E '"outcome":"(defer_stubborn|success_human_review)"' tmp/codify_<state>_bulk.log \
  | jq -r '.jurisdiction + "|" + .type + "|" + .state' \
  > tmp/codify_<state>_rescue.tsv
```

For each row, **dispatch ONE agent in parallel** using the playbook template from `docs/jurisdiction_policy_source_playbook.md`. The agent's job is to find the authoritative URL (Municode/codepublishing/county.gov/state-park-system/etc.) using web search + manual judgment that the BULK pass's heuristics can't replicate.

Agent prompt template lives in `docs/jurisdiction_policy_source_playbook.md` under "Agent prompt template" and "STUBBORN JURISDICTION URL DISCOVERY". Don't try to wrangle the prompt yourself — use the playbook's version.

Collect agent returns into a CSV `tmp/codify_<state>_manual_urls.csv` with columns: `jurisdiction,type,state,source_url`.

## Step 5 — Re-run script in --from-csv mode

```bash
.venv-pipeline/Scripts/python.exe scripts/derive_policy_source_for_jurisdiction.py \
  --from-csv tmp/codify_<state>_manual_urls.csv > tmp/codify_<state>_rescue.log 2>&1 &
```

The script trusts the manual URL and skips Phase A discovery — goes straight to fetch → LLM rule extraction → migration emit.

## Step 6 — Apply migrations

The script writes one migration per jurisdiction to `supabase/migrations/auto_codify_<state>_<jurisdiction>_<date>.sql`. Review then apply:

```bash
for f in supabase/migrations/auto_codify_<state>_*_$(date +%Y%m%d).sql; do
  supabase db query --linked -f "$f"
done
```

## Step 7 — Trigger cascade propagation

After ps rows land, the resolver re-runs automatically via trigger. But to be safe, manually re-resolve affected fids:

```sql
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT DISTINCT bps.beach_fid AS fid
    FROM public.beach_policy_source bps
    JOIN public.policy_source ps ON ps.id = bps.policy_source_id
    JOIN public.jurisdictions j ON j.gid = ps.jurisdiction_gid
    WHERE j.statefp = (SELECT statefp FROM public.states WHERE state_code = '<STATE>')
      AND ps.created_at > now() - interval '2 hours'
  LOOP
    PERFORM public.compute_beach_field_consensus(r.fid);
    PERFORM public.promote_canonical_dogs_to_beach_dog_policy(r.fid);
  END LOOP;
END
$$;
```

## Step 7.5 — Classify off-leash carve-outs (R1 recurrence guard) — MANDATORY

Codify emits `beach_policy_source` rows for EVERY off-leash clause in a
jurisdiction's code — including carve-outs that do NOT make the beach off-leash:
a fenced dog park, private property / "at home", an activity exemption
(service/police/herding), a DIFFERENT named place, or a mere DEFINITION of "at
large". `_canonical_dogs_from_policy_sources` rolls up `off_leash_flag` as
`bool_or(any operative off_leash row)`, so an un-tagged carve-out **re-inflates
the whole beach off-leash** (regression R1, 2026-06-23). New off-leash rows
default `is_carveout=false`, so this recurs on every codify run unless tagged.

**Principle:** off-leash *permissions* are beach-specific, never territorial —
the inverse of [[citywide-leash-inference]] (which is correct for *restrictions*).

`run_state_pipeline.py` does this automatically (phase `offleash_carveout`, after
`zone_rules_v2_refresh`). For an **ad-hoc** codify run (the manual steps above),
run it yourself after Step 7:

```bash
# Pass 1 — free deterministic high-precision regex (idempotent):
supabase db query --linked "SELECT public.tag_offleash_carveouts_deterministic('<STATE>');"
# Pass 2 — Haiku for the genuine/ambiguous boundary (~$0.0006/row, marker-gated):
python scripts/classify_offleash_carveout.py --apply --state <STATE> --model haiku
```

Marker-gated via `beach_policy_source.carveout_classified_at` — only newly-extracted
rows are processed, so re-runs are nearly free. The bps UPDATE statement trigger
(`_refresh_beaches_from_ps`) auto-re-promotes each corrected beach; no manual
promote needed. Genuine off-leash (voice-control, named off-leash beaches like
Fort Funston / Pacifica Esplanade, on-beach off-leash zones) is preserved — the
classifier is conservative and the deterministic pass guards voice-control / dog-beach.

## Step 8 — Verify sweep

Build a verify page per the **`verify-sweep`** skill. Sample 6-8 beaches across the state's attribution paths:
- 1-2 from each major operator type that got new ps rows
- 1-2 state-park beaches if any landed
- 1-2 city beaches with new codify rows
- 1-2 control beaches that DID NOT get ps changes (ensure no regression)

Open each on `beach.html?fid=<N>`. Look for:
- Dog policy section shows the new cite text
- Scout blurb references the rule correctly
- Best window respects any new prohibition windows

## Step 9 — Report yield

```sql
SELECT
  count(*) FILTER (WHERE ps.created_at > now() - interval '4 hours') AS new_ps,
  count(DISTINCT bps.beach_fid) FILTER (WHERE bps.created_at > now() - interval '4 hours') AS beaches_covered
FROM public.policy_source ps
LEFT JOIN public.beach_policy_source bps ON bps.policy_source_id = ps.id
JOIN public.jurisdictions j ON j.gid = ps.jurisdiction_gid
WHERE j.statefp = (SELECT statefp FROM public.states WHERE state_code = '<STATE>');
```

Typical bulk-pass yield: 40-60% of jurisdictions land cleanly; 30-50% defer → rescue. Rescue-pass yield: ~70-85% of defers convert to manual URLs. End-to-end: ~70-90% jurisdiction coverage per state.

## Common gotchas

1. **City preemption** ([[codify-city-preemption]]) — A county-scope rule does NOT apply to beaches inside an incorporated city. The cascade has a guard, but the script must demote county rules with `NOT EXISTS (jurisdictions within 200m)`.
2. **AVG MITM** ([[avg-antivirus-https-mitm]]) — Franz's machine intercepts HTTPS. If `requests` fails but `curl` succeeds, you're missing `pip-system-certs` in the venv.
3. **403 → Playwright** ([[403-means-playwright-skip-ua-tricks]]) — Don't iterate on UA/Referer. Mark the URL `shape='js_rendered_or_blocked'` and skip the requests round-trip.
4. **Page-level over agency-level** ([[page-level-over-agency-level]]) — Deep-link the chapter URL, never the agency root. Red flag = N rows sharing 1 URL.
5. **Don't classify unknowns for deletion** ([[dont-classify-unknowns-for-deletion]]) — If the bulk pass returns an unfamiliar agency_name, grep the codebase for dependencies before "cleaning up." 7 BEP source-class anchor rows fell into this trap recently.
6. **Beach catalog is a prereq** ([[beach-catalog-prereq-for-codify]]) — Codify pre-filter skips jurisdictions with 0 beaches; if the inventory is missing, the codify result LOOKS clean but covers nothing. Ingest beaches first.

## Per Franz preferences

- [[no-unilateral-architectural-decisions]] — Don't relax the URL gate, don't drop the validity check, don't add a "loose mode" without asking.
- [[never-solve-same-problem-twice]] — The bulk pass + rescue pass is the encoded solution. Don't bypass it with one-off scripts.
- [[script-defers-dispatch-agents]] — Hard rule with receipts: 30 min of slug recon vs 30 min of agent dispatch produces wildly different yields. Reach for the playbook.
- [[claim-tested-without-end-state-verification]] — `verify-sweep` (step 8) is mandatory. SQL counts only mean rows exist; clicking proves the cascade landed.
