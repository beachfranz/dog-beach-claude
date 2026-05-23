# clean-pipeline-v2 — Architecture Doc

Branch cut 2026-05-22 from `main @ 43edf1a` after MD launch landed (43/43 phases).

## Why this branch exists

Franz directive 2026-05-22 EOD: "We have probably written the same procedures multiple times. We have tons of duplicate tables with similar data used in different places. We need procedures run where they will be fastest. We need smart, self-correcting pre-flights. We need scripts that are holistic and not relying on ad-hoc objects. We are producing production quality code. The end product should be cohesive, coherent, consistent, consolidated, stream-lined, clean-schema."

Three deep audits (table / Python / edge functions) ran 2026-05-22 evening to map the actual debt. Consolidated findings below; full audit transcripts available in the parent session at task IDs `a8ec50e7f3c5f4c34` / `a7bb3ef9656d1dba3` / `a25dcd84300fd9d4c`.

## Combined scope

| Surface | Quantitative reduction |
|---|---|
| **Tables** | ~46K rows across 26 zombie tables move to `archive_2026_05_22.*` schema (verified live 2026-05-22, see "Tables audit detail" below) |
| **Python** | ~16K LOC archived (`one_off/` + arena/gold/v3 chain) + 4-5 `common/` modules dedupe ~1K LOC |
| **Edge functions** | ~5,500 LOC deleted or consolidated (~29% of 19K); 4-5 new `_shared/` modules |
| **Live bugs to fix** | 4 (admin-delete-beach phantom table, beach_polygons_truth zombie writer, extraction_prompt_variants dead FK, pg_stat stale) |
| **Total** | ~21K LOC reduction + 30 tables archived + 4 bugs fixed |

Estimate: 2-3 weeks of focused branch work.

## Branch ground rules

1. **Soft-archive only — no DROP.** Tables move to `archive_YYYY_MM_DD.*` schema; scripts move to `scripts/archive_YYYY_MM_DD/`. Per Franz 2026-05-22: "we want to soft-delete/archive rather than blowing away. at some point when we know everything is perfectly copacetic, we can get rid of them."
2. **Reversibility check before archive.** For every table about to be archived, query `pg_constraint` for incoming FKs; either drop them in the same migration (recordably) or migrate dependents first.
3. **Backup-then-rename-then-FK-cleanup.** `pg_dump` to `tmp/archives/` first (belt + suspenders), then archive.
4. **Per-area migrations.** One migration per consolidation move (`<date>_archive_<category>_v1.sql`), each documenting what was archived + what FKs dropped + how to restore.
5. **Quarterly review.** Schemas older than 3 months with zero queries → finally `DROP SCHEMA CASCADE`.
6. **Pre-existing memory pins remain authoritative** — especially the HARD rules in `~/.claude/projects/.../memory/MEMORY.md`. This refactor doesn't bypass them.

## Recommended branch sequence

### Day 1 — Foundation (low-risk, high-leverage)
- **Cut common/db.py** (`connect()`, `thread_conn()`, `pooler_url()`) — replaces ~700 LOC across 50+ files (Python audit move #1)
- **Cut common/llm.py** (`sonnet()`, `haiku()`, `opus()` factories) — 21 scripts converge; one model bump migrates the codebase (currently stuck on sonnet-4-5/4-6, no script on 4-7) (Python audit move #2)
- **Cut common/supa.py** unifying 12 drifting `supa()` impls (Python audit move #7)

### Day 1-2 — Mass archival (no logic change)
- **Python**: archive `scripts/one_off/` (81 files) + arena/gold/v3 chain (14 files, ~3.3K LOC) → `scripts/archive_2026_05_22/`
- **Tables (batch 1, snapshot/legacy zombies)** — 20,382 rows across 10 present tables:
  - `verdict_snapshot_pass6` (1,588) / `pass7` (1,588) / `pass9` (3,835) / `pass10` (3,835) / `pass11` (3,835)
  - `pass8` already gone
  - `truth_snapshot_before_50` (138)
  - `beach_enrichment_provenance_legacy_fid_archive` (5,532)
  - `ca_coastline_v3` (1), `ca_coastline_v4` (2), `ca_shoreline_v2` (28)
- **Tables (batch 2, no-consumer zombies)** — 26,016 rows across 15 tables. NOT all 0-row; the framing is "nothing reads them" not "they're empty":
  - `dog_amenities` (22,377) — biggest "zombie with data"; nothing on the consumer surface reads it
  - `extraction_calibration` (1,452), `operator_polygons_cache` (1,219), `paired_ccc_cpad_200m` (471), `operator_polygons_by_county_cache` (285), `extraction_prompt_variants` (141)
  - `state_park_operators` (10), `park_operators` (10), `operator_amenity_claims` (10), `dogs_verdict_override` (24), `dog_policy_zones` (5), `pipeline_sources` (12)
  - Truly 0-row: `beach_polygons_truth`, `beach_geom_change_queue`, `pipeline_runs`
- **Context** — 148 public tables + 38 views + 7 matviews total; the 26 zombies are ~18% of public tables.
- **Edge functions**: archive v1 jurisdiction pipeline (12 functions, ~2,400 LOC) — all replaced by v2-* per `v2-run-pipeline` orchestrator
- **Edge functions**: archive 4 per-tier `v2-*-dog-policy` (replaced by `v2-enrich-operational`, self-declared in its own header)

### Day 2-3 — `_shared/` extractions (edge functions)
- `_shared/p-limit.ts` — pLimit concurrency helper duplicated in 20 files (~140 LOC)
- `_shared/beach-resolve.ts` — FID/slug bridge duplicated 4× in consumer funcs (~240 LOC)
- `_shared/supabase.ts` — client bootstrap duplicated 97× (~290 LOC)
- `_shared/timezone.ts` — Pacific date helper duplicated 5× (~50 LOC)
- `_shared/rate-limit.ts` — extract from `admin-auth.ts` for `beach-chat` reuse
- Fix typed-as-string-passed-array CORS bugs in 7 functions

### Day 3-5 — Substantive consolidations
- **Operator/operators unification** (per `operate-two-table-finding` memory pin): pick winner; remap `agency_aliases` (26K rows misnamed) into the merge. Touches 8-12 Python scripts + 15-20 migrations.
- **Admin-update-* collapse**: 6 table-writers → 1 parametric handler + table-config map (~500 LOC freed). Delete `admin-update-location` first (already self-flagged DEPRECATED 2026-05-03).
- **Admin RPC wrappers**: 13 thin wrappers → either 1 parametric proxy + allowlist OR direct PostgREST RPC. ~700 LOC.
- **Codify cascade to PL/pgSQL**: move `scripts/codify_cascade_phase.py` (~500 LOC) to a Postgres RPC; keep Python as thin retry+logging wrapper.

### Day 5+ — Pending tasks scheduled inline
- `#150` state_dogs_policy missing PK + OR dupes — schema fix coordinated with operator unification
- `#154` cascade-correctness (auto-dispatch + admin_review status) — biggest single substantive piece
- `#157` beach_polygon_membership stale fids — cleanup pass
- `#158` nearest_dog_park sentinel — replace launch-mode threshold with actual "no DP within cap" semantic
- `#138` beach_amenities playground field
- `#142` CDPR loader fanout fix
- `#74` v3.r4 Type-B loader refactor (now subsumed by StateParksLoader ABC — verify)
- `#161` thin-data short-form description prompt
- `#152` WA osm_features under-loaded + ST_Contains anomaly

### After all batches land
- **Bug fixes**: admin-delete-beach phantom `beaches` reference; beach_polygons_truth zombie writer chain; extraction_prompt_variants dead FK pointer cleanup
- **`config/pipeline_config.py`**: worker counts + thresholds + pgbouncer ceiling + freshness windows (Python audit move #5, low risk)
- **Split `run_state_pipeline.py` (2387 LOC) into `phases/phase_NN_*.py`** — each phase becomes testable in isolation (Python audit move #9)
- **Photo source CHECK → lookup table** — ALREADY DONE on main (commit `19a7596`, photo_source_type table); pattern replicates for other CHECK enums on the branch
- **Convert `cpad_units_coastal` + `jurisdictions_coastal` from base tables to materialized views** (Table audit move #10)

## Pre-branch state recap

Already landed on main today (2026-05-22):
- MD codify wave: 16 migrations, 128/129 covered (99.2%)
- MD pipeline: 43/43 phases ok (run_id 31)
- Pipeline improvements: parallel codify_cascade, --canonical N walkthrough, --launch-mode flag, criterion COALESCE fix, operators_chunked registration, truststore inject in pipeline
- Loader framework: StateParksLoader ABC + WSPRC/OPRD/CDPR/MD DNR subclasses
- Consolidation #1 (photo_source_type lookup table — replaces CHECK constraint)
- Consolidation #2 (`scripts/common/text_cleaning.py` — but only 1 caller migrated; finish on branch)
- gen_state_park_baseline.py templated state-park codify

Memory pins added this session:
- `state-dogs-policy-honest-framing` (HARD)
- `pipeline-must-prompt-for-state-resources` (HARD)
- `agency-photo-centroid-must-populate-lat-lng` (HARD)
- `polygon-via-membership-pattern`
- `parks-schema-spine-decision` (entity_type column in beaches_gold, NOT sibling table)
- `mapillary-photo-quality-rejected` (HARD — don't re-propose)
- `honest-brand-prefers-candor`

## Open follow-ups (not blockers for the branch work)

- `#89` find.html photo thumbnails — UI work, independent
- `#122` Marina State Beach (8567) bps-less edge case — surgical
- `#145` MD DNR loader — already complete

## The vocabulary Franz set 2026-05-22

> "Cohesive, coherent, consistent, consolidated, stream-lined, clean-schema."

This is the bar. Every consolidation move on this branch should advance one or more of those properties. If a move doesn't, it's wrong-priority for v2.
