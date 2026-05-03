# Harmony branch — catalog ingest pipeline migration

**Status:** Phases 1-7(1+2) + phase 8 slices 1+2+3+4 shipped 2026-05-03. Phase 7.3 BLOCKED behind 6.3b/c + admin migration (audit at [docs/harmony-7-3-audit.md](harmony-7-3-audit.md)). Phase 8 = curator-on-canonical-tables migration (see `~/.claude/projects/C--Users-beach/memory/project_curator_on_canonical_tables.md`). Slice 5 (admin UI cutover + retire admin-update-location) is the last piece.

**One-line:** Migrate the `populate_from_*` / resolver / promoter family from
`locations_stage.fid` (legacy POI / OSM / CCC source IDs in the millions) to
`beaches_gold.fid` (post-path-3 spine, low thousands).

---

## Why

After path-3 (2026-05-01/02) made `beaches_gold.fid` the canonical consumer
spine, the catalog ingest pipeline never followed. As of 2026-05-03:

- `beaches_gold.fid` range: 93 → 9,716 (~764 active rows)
- `locations_stage.fid` range: up to 999,000,001
- `beach_enrichment_provenance.fid` examples: 9.7M, 2.8M, 10.9M, 11.2M
- **0 of 5,532** evidence rows match a `beaches_gold.fid`

The two fid universes don't overlap. The catalog ingest pipeline is
operating on a parallel, orphaned cohort. New consumer-side work (the
unified dog-policy pipeline; beach-polygon assignment; the bulk
production-data promotion that landed 2026-05-02) cannot reach the
catalog-side evidence.

The 2026-05-02 hacky `cpad_unit_id` backfills exposed the gap. Rather than
keep adding side-bit populators that bypass the catalog ingest pattern, we
migrate the catalog ingest pipeline onto the consumer spine.

`arena.source_id` (text) is the bridge: it holds the original source
identifier each legacy fid corresponds to, so legacy evidence can be
partially backfilled to `gold_fid` via `arena.source_id` lookup. Some
legacy evidence won't map (sources arena dedup'd as non-canonical group
members) — those rows stay legacy-only and get cleaned up in phase 7.

---

## Plan (7 phases)

| phase | description | status |
|---|---|---|
| 1 | Lock plan as memo + this doc | shipped 2026-05-03 |
| 2 | Schema: ADD `gold_fid bigint` on `beach_enrichment_provenance`. Additive — both namespaces coexist. Index + partial unique constraint where `gold_fid IS NOT NULL`. | shipped 2026-05-03 |
| 3 | Pilot: re-target `populate_from_cpad` to read `beaches_gold` and write evidence keyed on `gold_fid`. Validates the shape. New function `populate_from_cpad_gold(p_fid bigint)` ships alongside legacy. ALTER `fid` → nullable. Smoke-tested on Cabrillo (Catalina, 0.75 conf) + Las Tunas (LA County DBH, 0.95 conf); idempotent. | shipped 2026-05-03 |
| 4 | Add `populate_cpad_containment_gold(p_fid)` peer — beach-CPAD containment evidence in the same shape. New `field_group='polygon_containment'` (CHECK constraint extended). Tier 1 ranking applied (env-overlay demote, "Beach"-in-name, name token overlap, smallest area). Smoke-tested: Cabrillo→11462, Las Tunas→51669 (0.95), El Segundo→6135 (0.95), Fiesta→18858. All match the 2026-05-02 hand backfills. Idempotent. Tier 1 multi-poly resolution is in place but not currently exercised — no scoreable beach today falls in nested CPAD. | shipped 2026-05-03 |
| 5 | Promoter trio for containment: `_resolve_polygon_containment` (sets `is_canonical=true` per source priority manual > llm > cpad) + `_promote_polygon_containment_to_gold` (writes `beaches_gold.cpad_unit_id` from canonical, only when value changes) + `populate_polygon_containment_gold` (orchestrator: emit + resolve + promote). Smoke-tested on San Gregorio State Beach (8579): NULL → unit 421 (0.95 conf, has_beach + token overlap). End-to-end working. Raw `UPDATE beaches_gold SET cpad_unit_id` is now obsolete. | shipped 2026-05-03 |
| 6.1 | Jurisdictions + counties containment populators. Resolver upgraded to partition by polygon_kind (a beach can have N canonical rows, one per kind). Source CHECK extended with 'counties'. Smoke test on Las Tunas: 3 canonical rows (county=LA, cpad_unit=Las Tunas County Beach, c1_city=Malibu). | shipped 2026-05-03 |
| 6.2 | Military + tribal containment populators. NPS deferred — current `nps_places` is points-only (lat+lng, no polygon `geom`). New `scoreability_review_queue` view surfaces scoreable beaches whose canonical containment is military or tribal. Smoke tests: Las Flores Beach → MCB Camp Pendleton (MC Active); Klamath Beach → Yurok LAR. | shipped 2026-05-03 |
| 6.3a | ALTER `beaches_gold` ADD `c1_jurisdiction_id bigint` + `county_geoid text`. Extended `_promote_polygon_containment_to_gold` to write all three FK columns (cpad/c1/county) from canonical evidence. cdp/military/tribal kinds intentionally don't promote. Smoke: Las Tunas → all 3 FKs populated; Carbon Beach (no CPAD) → c1+county promote independently. | shipped 2026-05-03 |
| 6.3b | Operator-keyed populator: `populate_from_park_operators_gold`. Joins via `csp_parks` × `park_operators` (catches "CDPR state park leased to city/county" cases per `project_state_park_operators.md`). | **DEFERRED 2026-05-03** — see triggers below |
| 6.3c | URL/research populators: `populate_from_park_url_gold` + `populate_from_research_gold`. Read existing extraction tables; arena_group_id IS gold_fid already. The buffer-rescued attribution path in park_url is the complex bit. | **DEFERRED 2026-05-03** — see triggers below |
| 7 (parts 1+2) | Wired into Dagster (`scripts/dagster/dog_beach/dog_beach/assets/ingest.py`): `polygon_containment_evidence` (cheap obs) + `polygon_containment_run` (heavy). Backfill across all 763 active beaches: 1,539 evidence rows emitted, 1,078 FK promotions. **Coverage**: 763/763 county_geoid, 394/763 cpad_unit_id (52%), 290/763 c1_jurisdiction_id (38%), 15 in scoreability_review_queue. | shipped 2026-05-03 |
| 7 (part 3) | Deprecate `locations_stage`. Drop legacy `fid` column on `beach_enrichment_provenance`. | **BLOCKED** — see [docs/harmony-7-3-audit.md](harmony-7-3-audit.md). Two independent blockers: (a) admin-update-location still writes to locations_stage; (b) 5,532 legacy evidence rows have no gold equivalent until 6.3b/c lands. |

### 6.3b/c deferral — explicit triggers to un-defer

Phases 6.3b/c are **deferred, not abandoned**. The legacy populators write evidence consumed by promoters → `locations_stage` columns. We don't yet have equivalent target columns on `beaches_gold`, so porting the populators today produces "stranded evidence" — correct shape, no consumer.

The production tables those populators read from (`park_url_extractions`, `policy_research_extractions`, `operator_dogs_policy`, `cpad_unit_dogs_policy`) are **NOT** affected by the deferral. They keep being written and the direct-promotion path (`promote_production_to_beach_dog_policy.py`) keeps consuming them.

Pick 6.3b/c back up the moment any of these is on the table:

1. **Audit trail required.** "This dogs_allowed=yes came from which source URL with what confidence?" — beach_enrichment_provenance keeps that history per claim. Direct-promotion does not.
2. **Multi-source resolution as a separate step.** Today consensus is inline in `promote_production_to_beach_dog_policy.py`. If we want a pluggable resolver (e.g., to add manual-curator overrides as evidence rows, or to swap heuristics), we need evidence-based path on the gold spine.
3. **Calibration loops.** Comparing source-by-source accuracy against the truth set is much cleaner reading from evidence rows than re-running extraction.
4. **Adding any new evidence source.** A second LLM model, a second research pipeline variant, a manual-pin workflow — all are easier to add as a new `populate_from_<source>_gold` than as another branch in the inline consensus script.

If any of these comes up in a session, **don't bypass it with a one-off SQL update**. Build the populator. The pattern is established (cpad / jurisdictions / counties / military / tribal containment populators are all working examples).
| 7 | Wire into Dagster (`scripts/dagster/dog_beach/dog_beach/assets/ingest.py`). Backfill across 763 active beaches. Deprecate `locations_stage`. Drop legacy `fid` column. | terminal |

## Phase 2 schema (shipped)

`supabase/migrations/20260503_harmony_evidence_fid_dual_namespace.sql`:

```sql
alter table public.beach_enrichment_provenance
  add column if not exists gold_fid bigint;

create index if not exists beach_enrichment_provenance_gold_fid_idx
  on public.beach_enrichment_provenance (gold_fid)
  where gold_fid is not null;

create unique index if not exists beach_enrichment_provenance_gold_fid_fg_src_uq
  on public.beach_enrichment_provenance (gold_fid, field_group, source)
  where gold_fid is not null;
```

Legacy `fid` is **untouched**. Existing populators continue working unchanged.
New populators (phase 3+) write `gold_fid` instead. No FK on `gold_fid` for
now — beaches_gold rows can change during catalog-side dedup work; we don't
want evidence-write blocked on referential integrity. FK in phase 7 cleanup.

## Branch + commit hygiene

Work happens on `harmony`. No merge to main until phase 7 completes (full
pipeline runs end-to-end on `beaches_gold` and parity vs. legacy is verified).
Explicit approval required at end-to-end, not at phase boundaries.

## Tier 1 ranking (apply unchanged in phase 3+)

When migrated populators encounter multi-poly containment, apply the existing
ranking from `project_park_url_pipeline_2026-04-25.md`:

1. Demote env overlays (Marine Parks / Eco Reserves / Wildlife Areas)
2. "Beach" in `unit_name` wins
3. Trigram similarity to `display_name` (name match trumps area)
4. Smallest area as final tiebreak
5. Confidence desc / id asc for residual ties
