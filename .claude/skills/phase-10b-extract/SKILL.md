---
name: phase-10b-extract
description: Use this skill when running per-beach dog-policy extraction via extract_per_beach_policy_v1.py across one or more states for the Phase 10b workflow. Triggers include the user saying "run phase 10b for <state>", "extract per-beach policy for <fids>", "rerun v1 on <state>", or asking how to extract per-beach policies for a new state's beach set. DO NOT use for the codify pipeline (derive_policy_source_for_jurisdiction.py), for dog parks (different sub-pipeline), or for the older off-leash-only extractor (extract_per_beach_offleash_v2.py). For new states never run before, first verify the state has operator_dogs_policy URL coverage — if not, the harvest framework must run first.
---

# Phase 10b — per-beach policy extraction

Target consumer outcome: beaches attributed to applies_to_all=false operators get a per-beach `policy_source` + `beach_policy_source` row with a cite-required verbatim quote, so the cascade reader surfaces operator-specific rules instead of falling through to municipal-code defaults.

Today's proof point (2026-06-04): **545 fids → 104 new beach_policy_source rows, ~$25 LLM, ~22 min wallclock with 4-way parallel.**

## Step 1 — Identify the target fid set

Default: all beaches attributed to canonical operators with `operator_dogs_policy.applies_to_all=false`, scoped to one state, **excluding** beaches that already have a v1 BEP row (freshness guard) AND beaches that already have an op-posted policy_source for that attribution.

```sql
WITH applies_false_ops AS (
  SELECT DISTINCT odp.operator_id
  FROM public.operator_dogs_policy odp
  JOIN public.operators op ON op.id = odp.operator_id
  WHERE odp.policy_found = true AND odp.applies_to_all = false
    AND op.is_canonical = true
)
SELECT DISTINCT eo.entity_id AS fid
FROM applies_false_ops af
JOIN public.entity_operator eo
  ON eo.entity_type='beach' AND eo.operator_id = af.operator_id
JOIN public.beaches_gold g ON g.fid = eo.entity_id
WHERE g.state = '<STATE>'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_enrichment_provenance bep
    WHERE bep.gold_fid = eo.entity_id AND bep.source LIKE 'per_beach_policy_v1%'
  )
ORDER BY eo.entity_id;
```

Export to `tmp/phase10b_<state>_fids.txt` (one fid per line, via `psql -t -A`). Count it. Sanity-check the count makes sense against the population (e.g., MA has ~138 such fids; OR ~22).

## Step 2 — Smoke-test if a state hasn't been tried before

Before firing a large batch, run a dry-run on 5 representative fids spanning the state's operator distribution:

```bash
.venv-pipeline/Scripts/python.exe scripts/extract_per_beach_policy_v1.py \
  --fids <5 comma-separated fids> --no-web-search
```

Check that candidates_for_beach is producing candidate URLs (look for `candidates: N>0`). If most beaches show `candidates: 0`, the state needs `state_park_urls.json` entry OR the operator_dogs_policy.source_url isn't populated for that state's ops yet — go fix that upstream first.

## Step 3 — Fire the run (parallel for ≥40 fids)

For small sets (<40 fids) — serial is fine:

```bash
FIDS=$(paste -sd, tmp/phase10b_<state>_fids.txt)
.venv-pipeline/Scripts/python.exe scripts/extract_per_beach_policy_v1.py \
  --apply --fids "$FIDS" > tmp/phase10b_<state>_extract.log 2>&1 &
```

For larger sets (≥40 fids) — 4-way parallel:

```bash
split -n l/4 -d --additional-suffix=.txt tmp/phase10b_<state>_fids.txt tmp/phase10b_<state>_chunk_

for i in 00 01 02 03; do
  FIDS=$(paste -sd, tmp/phase10b_<state>_chunk_$i.txt)
  .venv-pipeline/Scripts/python.exe scripts/extract_per_beach_policy_v1.py \
    --apply --fids "$FIDS" > tmp/phase10b_<state>_p$i.log 2>&1 &
done
```

Each process holds one Supabase pool connection — 4 parallel = 4 connections, well under the medium-tier cap. BEP sentinel pattern (`per_beach_policy_v1_no_result`) makes accidental chunk overlap idempotent.

**Estimate**: ~12-18 sec/fid average. Cost ~$0.05/fid (mix of $0.01-0.02 direct calls + $0.05-0.10 web_search escalations). Serial wallclock = N × 15s. Parallel ÷ 4.

## Step 4 — Monitor progress (DB-side, not via log)

Python's stdout is block-buffered when redirected to a file. Don't rely on `tail` of the log — check DB:

```sql
SELECT bep.source, count(*)
FROM public.beach_enrichment_provenance bep
WHERE bep.source LIKE 'per_beach_policy_v1%'
  AND bep.gold_fid IN (<comma list>)
GROUP BY bep.source;

-- Most recent write
SELECT max(updated_at)::timestamptz(0) AS last_write,
       count(DISTINCT gold_fid) AS done
FROM public.beach_enrichment_provenance
WHERE source LIKE 'per_beach_policy_v1%' AND gold_fid IN (<comma list>);
```

## Step 5 — Propagate to consumer surface

After all workers complete, run propagation for fids that got new bps rows:

```sql
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT DISTINCT bps.beach_fid AS fid
    FROM public.beach_policy_source bps
    JOIN public.policy_source ps ON ps.id = bps.policy_source_id
    WHERE ps.citation LIKE '%per-beach policy (v1)%'
      AND bps.beach_fid IN (<comma list>)
  LOOP
    PERFORM public.compute_beach_field_consensus(r.fid);
    PERFORM public.promote_canonical_dogs_to_beach_dog_policy(r.fid);
  END LOOP;
END
$$;
```

Without this step, the new bps rows exist but the consumer surface (`beach_dog_policy`) doesn't see them.

## Step 6 — Spot-check on beach.html

Pick 3-4 representative beaches with non-trivial cite quotes:

```sql
SELECT bps.beach_fid, g.name, g.state, bps.rule, length(bps.evidence_verbatim) AS qlen
FROM public.beach_policy_source bps
JOIN public.policy_source ps ON ps.id = bps.policy_source_id
JOIN public.beaches_gold g ON g.fid = bps.beach_fid
WHERE ps.citation LIKE '%per-beach policy (v1)%'
  AND bps.beach_fid IN (<comma list>)
  AND length(bps.evidence_verbatim) > 80
ORDER BY g.state, qlen DESC
LIMIT 5;
```

Open each in browser: `file:///C:/Users/beach/Documents/dog-beach-claude/beach.html?fid=<N>`. Look for:
- Page renders (no 500)
- Dog policy section shows the rule + cite
- Scout blurb is reasonable for the rule (NOT recommending dog-walk during prohibition window)
- No "Come back at 11:59pm now" garbage (the Oceano sentinel fix should suppress this)

Per HARD rule [[claim-tested-without-end-state-verification]]: clicking the pages is the verification. SQL counts alone aren't proof.

## Step 7 — Report yield

| State | Fids | BEP wins | New bps | Yield | Cost |
|---|---|---|---|---|---|
| Calculate: `count(*) FILTER (WHERE source='per_beach_policy_v1')` for BEP wins; `count(DISTINCT beach_fid)` for new bps | | | | wins/fids | $0.05 × fids |

Typical yield: 15-25%. Lower in states with sparse operator-URL coverage (WA had 14% because many op_dogs_policy.source_url values are codify-style municipal pages that don't name specific beaches). Higher in states with state-park-listing URLs (OR had 27% via USFS Siuslaw NF).

## Common gotchas

1. **`field_group` must be `'dogs'`**, not `'dog_policy'`. CHECK constraint will reject. (Bugfix 16ee1ff.)
2. **RealDictCursor returns dicts** — use `row['id']` not `row[0]`. Otherwise you get cryptic "write error: 0". (Same 16ee1ff.)
3. **PowerShell `Get-Content` returns PSObjects**; pipe through `[string[]]@(... | ForEach-Object { $_.Trim() })` before `ConvertTo-Json`. Otherwise location_ids get serialized as `{value, PSPath, ...}` objects and PostgREST fails to match.
4. **Some answers are correct but lose nuance** in `_derive_dominant_sand_rule` collapse. The BEP audit row preserves full zone_rules — parking-lot item 1b will surface it via a separate promote function.
5. **Anthropic web_search counts against budget** — `--no-web-search` cuts cost ~50% but also halves yield in non-CA states with sparse candidate builders. CA has hardcoded URL builders so `--no-web-search` is much less costly there.

## Per Franz preferences

- Per [[no-unilateral-architectural-decisions]]: don't change the v1 script's design (prompt shape, write paths, fallback rules) without asking. Today's "I was wrong, include regions and sections" course-correction is a HARD-encoded design intent: [[extractor-prompts-request-full-structure]].
- Per [[never-solve-same-problem-twice]]: the BEP sentinel freshness guard means re-runs are no-ops on already-processed beaches. Don't add explicit `--skip-fids` flags or other workarounds.
- Per [[claim-tested-without-end-state-verification]]: SQL-only verification is insufficient. Step 6 click-through is mandatory before reporting done.
