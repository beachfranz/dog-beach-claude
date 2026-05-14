# Dog Beach Scout — Dagster project (v2)

Fresh project as of 2026-05-13. Replaces the prior project (archived at
`scripts/dagster/_archive_2026_05_13/`) which predated `run_state_pipeline.py`
and had accumulated stale assets.

## What this is

A Dagster orchestration project that wraps every substantive piece of
the Dog Beach Scout pipeline (Phases 1-33 per `docs/pipeline-overview.md`)
as a Dagster asset, plus downstream consumers (edge functions, HTML pages)
as lineage-only AssetSpecs.

Today this project is **observation-mode** alongside `run_state_pipeline.py`
which remains the canonical engine. Sensors and schedules default to
STOPPED. The intent is to migrate incrementally — when a Dagster asset
matches what `run_state_pipeline.py` produces and you've verified output
parity, you can flip the corresponding pg_cron or shell wrapper off.

## Quick start

```bash
cd scripts/dagster/dog_beach
../../../.venv-pipeline/Scripts/python -m dagster dev
```

UI at http://localhost:3000.

## Layout

```
dog_beach/
├── __init__.py                Definitions object — wires everything together
├── resources.py               20 ConfigurableResources (DB, LLM, photos, GIS, ...)
├── partitions.py              state_partitions (static) + operator_partitions / fid_partitions (dynamic)
├── jobs.py                    6 pre-defined execution plans
├── assets/
│   ├── upstream_loaders.py    Phases 1-9 (chain integrity → precheck)
│   ├── operator_seeding.py    Phase 10 (populate_operators_for_state, 12-pass)
│   ├── catalog_assembly.py    Phases 11-25 (arena → gold → enrichment → dedup → geom_queue)
│   ├── operator_llm_cascade.py Phases 26-28 (operator LLM extract → merge → bep_refire)
│   ├── per_fid_enrichment.py  Phases 29-31 (section_extract, descriptions, photos)
│   ├── scoring_and_audit.py   Phases 32-33 (daily_refresh, field_population_check)
│   └── frontend.py            20 lineage-only AssetSpecs (edge fns + HTML pages)
├── checks/
│   ├── operator_seeding.py    Phase 10 breadth check
│   └── catalog_assembly.py    6 AssetChecks on critical phase criteria
├── sensors/
│   └── reactive.py            5 reactive sensors (default STOPPED)
└── schedules/
    └── time_based.py          3 cron schedules (default STOPPED)
```

## Resources

20 ConfigurableResources cover every external surface the pipeline touches:

| Group | Resources |
|---|---|
| Database | `postgres` (pooler), `postgres_session` (session-mode for LISTEN/NOTIFY) |
| LLM/search | `anthropic`, `tavily` |
| Supabase platform | `supabase_edge`, `subprocess` |
| External GIS | `arcgis`, `overpass`, `google_maps`, `geoapify` (parked) |
| Scoring inputs | `open_meteo`, `noaa_co_ops`, `best_time` (on hold) |
| Photos | `wikimedia_commons`, `flickr`, `pixabay`, `pexels`, `unsplash`, `mapillary` (deprecated), `nps_multimedia` (parked) |

Secrets are pulled from `scripts/pipeline/.env` via `EnvVar` at runtime.

## Partitions

- `state_partitions` (static): CA / OR / WA / MA / FL / MI
- `operator_partitions` (dynamic, populated by `new_operators_sensor`): one per `operators.id`
- `fid_partitions` (dynamic, populated by `new_fids_sensor`): one per active+scoreable beach

## Jobs

| Job | Selection | When to use |
|---|---|---|
| `state_launch_job` | All 33 phases | First-time state launch — mirrors `python run_state_pipeline.py --state X` |
| `catalog_assembly_job` | Phases 1-25 | Catalog rebuild without LLM cost |
| `llm_cascade_job` | Phases 26-29 | After upstream policy changes; LLM cost |
| `daily_refresh_job` | Phase 32 | Daily scoring fan-out only |
| `pipeline_health_audit_job` | Phase 33 | Audit-only run |
| `bep_refire_job` | Phase 28 | Ad-hoc refire after operator/PAD-US changes |

## Sensors (all default STOPPED)

- `pad_us_loaded_sensor` — `external_source_status.pad_us='ok'` → fire `operators_for_state`
- `new_operators_sensor` — new `operators.id` rows → register `operator_partitions` + fire `operator_policy_extraction` (**incurs ~$0.30/op LLM cost**)
- `new_fids_sensor` — new `beaches_gold` active+scoreable rows → register `fid_partitions`
- `arena_changed_sensor` — new `arena` rows for a state → fire `promote_to_gold`
- `geom_change_sensor` — unprocessed `geom_change_queue` entries → fire `bep_refire`

## Schedules (all default STOPPED)

- `daily_beach_refresh_schedule` — 09:00 UTC daily; replaces pg_cron `daily_beach_refresh_nightly`
- `hourly_now_refresh_schedule` — every hour at :00; replaces `hourly-beach-now-refresh`
- `weekly_pipeline_health_schedule` — Sun 06:00 UTC; new

**Important**: when flipping any Dagster schedule ON, disable the equivalent pg_cron job to avoid duplicate triggers:
```sql
SELECT cron.unschedule('daily_beach_refresh_nightly');
SELECT cron.unschedule('hourly-beach-now-refresh');
```

## Asset Checks

7 checks attached to critical-criterion assets:

- `operators_for_state_has_breadth` — Phase 10 — >=3 levels, >=5 non-basic for non-CA
- `promote_to_gold_complete` — Phase 14 — every active beach has county_fips + state
- `name_source_complete` — Phase 17 — every active beach has name_source
- `align_scoreable_clean` — Phase 19 — no Tier 3/4 beach is scoreable
- `catchment_refresh_complete` — Phase 20 — every active beach has scoring_tier
- `descriptions_coverage` — Phase 30 — >=50% of tier-1+2 have descriptions
- `daily_refresh_coverage` — Phase 32 — >=95% of scoreable have today's rec

All severity=WARN (logs in UI, doesn't block downstream).

## Migration status

- ✅ All 33 phases declared as Dagster assets
- ✅ Downstream lineage (edge fns + HTML pages) declared as AssetSpec
- ✅ Sensors + schedules in place (default STOPPED)
- ✅ 7 AssetChecks on critical criteria
- ⏳ Sensors not yet enabled (would auto-trigger downstream work)
- ⏳ Schedules not yet replacing pg_cron
- ⏳ `run_state_pipeline.py` still the canonical engine for production runs

## Design principles

- **Per-state partitions** are the natural unit. Re-running OR doesn't touch WA.
- **DynamicPartitionsDefinition** for per-operator / per-fid fan-outs (better than internal chunking).
- **Default STOPPED** for everything that costs money or DB load. Opt-in via UI.
- **Transaction-mode pooler (port 6543)** is the default DB resource. Session-mode is opt-in.
- **Lineage-only AssetSpecs** for things Dagster doesn't materialize (HTML pages, edge fns) — keeps the UI's dependency graph honest.
- **Subprocess wrappers** for legacy scripts. We don't rewrite the LLM extractors into Dagster ops — we wrap them via `SubprocessResource`. The `_chunked_subprocess` pattern from `run_state_pipeline.py` becomes `DynamicPartitionsDefinition` (Dagster-native chunking).
- **Resources for parked/deprecated APIs** carry `is_active=False` — calling them when off raises a clear error rather than failing late.

## What changes when you flip a sensor or schedule ON

| Toggle | What starts running | What you should disable |
|---|---|---|
| `daily_beach_refresh_schedule` | Daily scoring fan-out at 09:00 UTC | pg_cron `daily_beach_refresh_nightly` |
| `hourly_now_refresh_schedule` | Hourly NOW refresh | pg_cron `hourly-beach-now-refresh` |
| `pad_us_loaded_sensor` | Auto-fires operators_for_state for newly-loaded states | nothing — additive |
| `new_operators_sensor` | Auto-fires LLM extraction for new operators (~$0.30/op) | nothing, but BUDGET CHECK |
| `geom_change_sensor` | Auto-fires bep_refire for changed beaches | nothing — additive |

## What's NOT yet in Dagster

- Photo loaders other than Wikimedia (Flickr / Pixabay / Pexels / Unsplash) — they're declared as resources but no asset wires them
- `v2-*` legacy catalog ingest edge functions — superseded by the Phase 11-14 chain
- `admin-*` edge functions (~25) — declared as AssetSpec but not orchestrable here (they're manual curator actions)
- Dagster Cloud deployment — local-only for now (`dagster dev`)

## Reading order for someone new

1. `docs/pipeline-overview.md` — the canonical 33-phase pipeline spec
2. `scripts/dagster/dog_beach/dog_beach/__init__.py` — see how everything wires up
3. `dog_beach/assets/upstream_loaders.py` — example of the per-state-partitioned wrapper pattern
4. `dog_beach/assets/operator_llm_cascade.py` — example of DynamicPartitionsDefinition (Phase 26)
5. `dog_beach/sensors/reactive.py` — example of reactive sensor pattern
