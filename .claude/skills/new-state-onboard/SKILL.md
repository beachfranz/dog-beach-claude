---
name: new-state-onboard
description: Use this skill when launching a new US state end-to-end (beaches + operators + codify + photos + descriptions + scoring), or asking how to onboard a state. Triggers include "add NJ", "onboard Hawaii", "launch FL state", "new state launch <STATE>", "what does a state launch involve", "let's do <STATE>". The canonical orchestrator is `scripts/run_state_pipeline.py` (3,492 lines, 47 phases, idempotent + resumable). This skill is a MAP to that orchestrator + a guide to which existing skills extend it. DO NOT propose a parallel pipeline — per HARD rule [[use-pipeline-infrastructure]], deviating requires explicit Franz approval. DO NOT use for re-running one specific phase on existing data (just invoke `run_state_pipeline.py --state X --phase-from <phase>` directly).
---

# new-state-onboard — map to the canonical state-launch pipeline

The canonical orchestrator already exists: `scripts/run_state_pipeline.py`. It is single-command, idempotent, resumable, and self-skipping on phases whose `criterion` already passes. This skill exists so you (or Franz) don't re-read 3,492 lines to know what runs and where the existing project skills extend it.

**Per HARD rule [[use-pipeline-infrastructure]]**: if work fits a pipeline phase, USE the pipeline. Don't build parallel one-off drivers. Explicit Franz approval is required to deviate.

## TL;DR — to launch a state

```bash
.venv-pipeline/Scripts/python.exe scripts/run_state_pipeline.py --state <STATE>
```

That's it. The orchestrator runs all 47 phases in order, writes status to `public.pipeline_phase_status`, halts on first criterion failure, and is resumable by `run_id`. Wall time: a few hours for a coastal state (most LLM-heavy phases dominate).

## Flags

| Flag | When |
|---|---|
| `--state <CODE>` | Required (e.g., `--state OR`, `--state HI`) |
| `--run-id <N> --resume` | Resume a halted run by ID — picks up at the failed phase |
| `--force` | Ignore prior status (re-run phases that already passed) |
| `--phase-from <key>` | Skip to a specific phase (e.g., `--phase-from bep_refire`) |
| `--skip-precheck` | Skip the precheck phase (rarely needed) |
| `--dry-run` | Print the plan without executing |

## 47 phases — grouped by stage

| Stage | Phases (in order) | What it produces |
|---|---|---|
| **1. Integrity + seeds** | `chain_integrity_check`, `state_policy_seed`, `seasonal_closure_seed`, `state_park_url_check` | Confirms populator chains intact; seeds state_dogs_policy + seasonal_closures; halts if `state_park_urls.json` lacks an entry |
| **2. Upstream loaders** | `ensure_tiger_places`, `ensure_pad_us`, `pad_us_geom_geog_check`, `pad_us_manager_class_preflight`, `ensure_county_subdivisions`, `ensure_overpass`, `ensure_poi_landing`, `ensure_amenities`, `ensure_dog_features` | Loads TIGER places, PAD-US units, OSM landings, POI CSV — all gated by `external_source_status` so idempotent |
| **3. Sanity gate** | `precheck` | Belt-and-suspenders check that loaders succeeded |
| **4. Arena → gold** | `operators`, `arena_seed`, `cluster_group`, `cluster_extras`, `promote`, `beach_inventory_check`, `ensure_pip_membership`, `validate_state_geom` | Populates operators, dedups arena clusters, promotes to `beaches_gold`, validates beach polygon containment |
| **5. Enrichment** | `address_poi`, `address_city`, `name_source`, `strip_plus_codes`, `catchment_refresh`, `refresh_nearest_dog_park` | Addresses, name sources, catchment cascade, nearest-park RPC refresh |
| **6. Cascade** | `codify_cascade`, `dedup`, `dedup_distance_name`, `geom_queue`, `operator_llm_extract`, `purge_pollution`, `operator_merge`, `bep_refire`, `section_extract` | Codify (statutes), LLM operator policy extraction, late-stage dedup, BEP cascade refire, section extraction |
| **7. Operating hours + status** | `operating_hours_refresh`, `hourly_status_refresh` | Open/close hours; status board materialization |
| **8. Photos** | `photos_wikimedia`, `photos_flickr`, `photos_websearch`, `state_photo_galleries`, `photo_centroid_backfill`, `photos_tag`, `photos_curate` | Loads all 4 photo sources, backfills centroids, runs vision tagging, curates top-3 per beach |
| **9. Final** | `harvest_park_text`, `descriptions`, `descriptions_audit`, `daily_refresh_fire`, `codify_coverage_check`, `field_population_check` | Park-text harvest, Sonnet descriptions + audit, fires daily-beach-refresh, codify coverage check, field population check |

## Where existing skills extend the orchestrator

The 6 project skills do NOT replace the orchestrator — they are scoped re-run tools for specific stages when you want to extend coverage on already-launched states without re-running the whole pipeline.

| Existing skill | Orchestrator phase it relates to | When to fire |
|---|---|---|
| `codify-state` | `codify_cascade` (bulk) + agent-dispatch playbook (rescue) | Run **after** orchestrator completes if defers need rescue — orchestrator's codify pass exhausts heuristics, rescue is manual URL discovery |
| `phase-10b-extract` | `operator_llm_extract` + `bep_refire` | Run **after** orchestrator to extract per-beach policies for `applies_to_all=false` operator beaches (narrower than orchestrator's bulk LLM extraction) |
| `dog-park-state-launch` | (PARALLEL pipeline — NOT inside this orchestrator) | Run **separately** for dog parks. `run_state_pipeline.py` covers BEACHES; `scripts/dog_park_pipeline/run.py` covers DOG PARKS. They are sister pipelines. |
| `add-state-park-loader` | Prerequisite to `state_photo_galleries` | Run **before** orchestrator if a new state lacks a state-park photo loader. Add the 3 pieces, then orchestrator's `state_photo_galleries` phase picks them up. |
| `edge-function` | (outside the orchestrator — frontend/backend code) | Deploy/modify edge functions any time |
| `verify-sweep` | (post-orchestrator validation) | Run **after** orchestrator completes — click-through audit per [[claim-tested-without-end-state-verification]] |

## Pre-launch checklist for a fresh state

Before invoking the orchestrator on a state never run before:

1. **`scripts/state_park_urls.json` has an entry for `<STATE>`** — phase 4 (`state_park_url_check`) halts otherwise. Either:
   - Has a real `loader` + `loader_class` + `pattern` (if the state has a state-park-specific photo CMS worth implementing) — see `add-state-park-loader` skill
   - Has `loader_deferred: true` with a `notes` field (Wikimedia + websearch are sufficient)

2. **PAD-US loaded** for `<STATE>` — orchestrator's `ensure_pad_us` handles this, but if you've prepared PAD-US data ahead of time, the phase self-skips.

3. **`public.state_government_strength` row exists** — needed by `ensure_county_subdivisions` to decide if COUSUB layer load is needed (town-strong NE states need it; county-strong states don't).

4. **`public.states` row exists** — basic state metadata. Most states are pre-seeded.

5. **`scripts/pipeline/.env` is loaded** — `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `SUPABASE_DB_PASSWORD`, `ADMIN_SECRET`, `ANTHROPIC_API_KEY`. Per [[check-secrets-first]], check existing values before asking for new ones.

## How to monitor a run

The orchestrator writes per-phase status to `public.pipeline_phase_status`:

```sql
SELECT phase_key, status, started_at, ended_at, rows_affected, error_message
FROM public.pipeline_phase_status
WHERE state = '<STATE>' AND run_id = (SELECT max(run_id) FROM public.pipeline_phase_status WHERE state='<STATE>')
ORDER BY started_at;
```

A halted run shows the failed phase + error_message. Resume via `--run-id <N> --resume`.

## Where the canonical pipeline ENDS and skills BEGIN

The orchestrator covers ~85% of a state launch end-to-end. The remaining 15% is:

- **Codify rescues** — per [[script-defers-dispatch-agents]], when `codify_cascade` returns `defer_stubborn` or `success_human_review` for jurisdictions, the **rescue path is playbook agent dispatch** (handled in `codify-state` skill). The orchestrator's bulk pass exhausts its heuristics; manual URL discovery is the rescue.
- **Per-beach policy extraction** for famous beaches with `applies_to_all=false` operators — `phase-10b-extract` handles this targeted pass.
- **Dog parks** — sister pipeline (`scripts/dog_park_pipeline/run.py`). `dog-park-state-launch` skill covers it.
- **Verify sweep** — click-through audit (`verify-sweep` skill).

A complete state launch sequence:

```
1. (optional) add-state-park-loader   if state needs new state-park photo loader
2. python scripts/run_state_pipeline.py --state <STATE>
3. codify-state                       handle defers via playbook agents
4. phase-10b-extract                  per-beach policy for applies_to_all=false beaches
5. python -m scripts.dog_park_pipeline.run --state <STATE>   (sister pipeline)
6. verify-sweep                       click-through audit
```

## Common gotchas

1. **Phase halts on missing state_park_urls.json entry** — phase 4 (`state_park_url_check`) deliberately halts with a template if missing, so the state-parks photo loader doesn't silently skip. Add the entry, resume.

2. **PAD-US `geom_geog` NULL** — phase `pad_us_geom_geog_check` catches the regression family where `pad_us_units.geom` is populated but `geom_geog` is NULL. NH was the canary (0/8321 geom_geog → 0 PAD-US memberships → silent stay-at-policy-tier-3). Fixed via backfill UPDATE.

3. **Operators chunked for OR** — phase `operators` uses chunked autocommit per state due to OR's 915K-vertex PAD-US polygons OOMing the monolithic FUNCTION. Resolved by `20260513_split_operator_seeding.sql`.

4. **POI landing matters even for OSM-rich states** — `ensure_poi_landing` gap surfaced by DE virgin-state test (49 → 6 beaches without POI). Don't skip the POI CSV loader.

5. **Don't deviate from the canonical pipeline** — per [[use-pipeline-infrastructure]], every "I'll just write a quick script" requires Franz approval. Almost always there's a phase that fits, or a populator to extend.

## Per Franz preferences

- [[use-pipeline-infrastructure]] — the HARD rule. Default move: USE THE PIPELINE.
- [[every-point-every-polygon]] — the orchestrator does NOT pre-filter which polygons a beach is eligible for; every active+scoreable beach goes through every PIP.
- [[chunked-subprocess]] — the orchestrator uses `_chunked_subprocess` for any >10min phase. Don't reinvent.
- [[claim-tested-without-end-state-verification]] — orchestrator completes ≠ launch successful. `verify-sweep` is mandatory.
- [[never-solve-same-problem-twice]] — the orchestrator IS the encoded solution. The 47 phases exist because each one solved a past problem.

## Anti-patterns

- **Writing a one-off "let me just bulk insert these beaches" script** — that's the populator's job. Go via `arena_seed` + `promote`.
- **Skipping `precheck` to save time** — it's already self-skipping if loaders are recent. The "save time" framing is wrong; precheck is cheap.
- **Running phases out of order via Python loop** — the orchestrator's phase ordering encodes dependencies you may not see.
- **Building parallel chunked drivers** — `_chunked_subprocess` is the canonical chunker. Use it via the orchestrator.

## Anchors

- `scripts/run_state_pipeline.py` — the canonical orchestrator
- `docs/canon-issues-log.md` — historical issues that drove phase additions/reorderings
- `public.pipeline_phase_status` — per-run status table
- `public.external_source_status` — per-source loader status (used by ensure_* phase criteria)
- `public.state_government_strength` — drives `ensure_county_subdivisions` skip logic
