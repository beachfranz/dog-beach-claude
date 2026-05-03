# Harmony branch — catalog ingest pipeline migration

**Status:** Phases 1 + 2 + 3 + 4 + 5 shipped 2026-05-03. Phase 6 (migrate remaining populators) is next.

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
| 6 | Migrate remaining populators: `populate_from_jurisdictions`, `populate_from_csp_parks`, `populate_from_military_bases`, `populate_from_nps_places`, `populate_from_tribal_lands`, `populate_from_park_operators`, `populate_from_park_url`, `populate_from_research` | repetitive sweep |
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
