# Harmony phase 7.3 — locations_stage + legacy fid drop audit (2026-05-03)

**TL;DR — Updated 2026-05-03 EOD: legacy `fid` column DROPPED.** locations_stage retire is still parked behind admin-update-location migration. Original audit text below preserved for context; resolution at the bottom.

---

## What 7.3 was supposed to do

Per the original 7-phase plan in `docs/harmony.md`:

> Phase 7 (part 3) — Deprecate `locations_stage`. Drop legacy `fid` column
> on `beach_enrichment_provenance`.

The first part removes the legacy spine table entirely; the second part
removes the legacy identifier column on the evidence journal. Both are
the final cleanup once the gold-spine catalog ingest pipeline has
parity.

## What the audit found

Two independent blockers. Either alone is sufficient to keep 7.3 parked.

### Blocker 1 — locations_stage is still actively read/written

| dependent | status | impact of drop |
|---|---|---|
| `admin-update-location` edge function | active production endpoint | Drop breaks the location editor curator surface |
| `admin/location-editor.html` (+ likely `admin/index.html`, `ccc-map.html`, `active-map.html`) | active admin UI | Drop breaks admin curation workflow |
| 25 DB functions (legacy `populate_from_*`, `_promote_*_to_stage`, `_resolve_*`, `populate_all`) | stale but defined | Functions become broken refs; deferred 6.3b/c work would re-enable them on gold spine |
| `park_url_scrape_queue` view | active dependency | Drop breaks the scrape queue view |
| `scripts/one_off/export_locations_stage_csv.py` | one-off, low value | discardable |
| 862 rows of data in `locations_stage` | curated state | lose the curator's work |

**To unblock**: migrate `admin-update-location` (and the admin pages that
call it) to write directly to `beaches_gold` instead of `locations_stage`.
Or accept that the curator-side workflow is retired and build a fresh
gold-spine equivalent.

### Blocker 2 — legacy `beach_enrichment_provenance.fid` holds 5,532 rows of evidence with no gold equivalent

**UPDATE 2026-05-03 EOD (post-backfill, two passes):** Two spatial backfills landed. The first (vs beaches_gold within 50m) was misleadingly narrow — Franz pointed out that beaches_gold is a small subset of arena, the master inventory. The second pass (vs arena within 200m) is the right comparison since `arena.fid = beaches_gold.fid` by path-3 inheritance.

Final partition after both backfills:

| bucket | count | % | drop safety |
|---|---:|---:|---|
| Backfilled to gold_fid (vs beaches_gold + vs arena) | 3,617 | 65% | safe — already on gold |
| Safe to drop — gold-side duplicate exists | 784 | 14% | safe — gold-side has same `(gold_fid, field_group, source)` |
| **True orphans — not even in arena** | **1,131** | **20%** | **NOT safe — no arena beach within 200m** |

True orphans are evidence rows for legacy `locations_stage` beaches that don't exist in arena (the master inventory) within 200m. ~170 distinct beaches.

**Sampling check 2026-05-03 EOD**: 12 random true-orphan beaches turned out to be real, well-known CA beaches: Moonlight State Beach (Encinitas), Point Dume (Malibu), Pescadero Beach (San Mateo), East Beach (Santa Barbara), Swami's Beach, Limantour Beach (Point Reyes), Miramar Beach (Half Moon Bay), etc. So these are NOT "safely discardable filter rejects" as initially hypothesized. They're either:

1. Beaches arena has under a different name/geom that 200m proximity didn't catch (e.g. arena's "Encinitas Beach" might be the canonical for Moonlight State Beach but at a different geom point)
2. Real coverage gaps where a well-known beach truly never made it into arena
3. Cases where arena dedup collapsed them with a canonical row whose geom is far away

**Don't drop the legacy fid column without name-based fuzzy matching first.** The right next-session work is to name-match the 170 distinct true-orphans against arena (regardless of distance). That'll separate "in arena under different name" (mappable) from "real coverage gap" (real residual).

The 65% spatial backfill is real progress, but the 1,131 stranded rows are NOT safe drops.

See:
* `supabase/migrations/20260503_harmony_phase7_3_legacy_evidence_backfill.sql` (vs beaches_gold)
* `supabase/migrations/20260503_harmony_phase7_3b_arena_backfill.sql` (vs arena, the right comparison)

Partition of `beach_enrichment_provenance` as of 2026-05-03 backfill:

| partition | row count | notes |
|---|---:|---|
| Legacy-only (`fid` set, `gold_fid` null) | **5,532** | Pre-harmony evidence from legacy populators |
| Gold-only (`fid` null, `gold_fid` set) | 1,543 | Containment evidence from harmony phases 4-6 backfill |
| Both fids set | 0 | Clean partition, no overlap |

The 5,532 legacy-only rows by `field_group`:

| field_group | rows | source |
|---:|---:|---|
| dogs | 2,094 | park_url, research |
| governance | 1,663 | cpad, jurisdictions, park_operators |
| practical | 1,033 | park_url |
| access | 742 | cpad |

This is **exactly the goldmine the deferred 6.3b/c phases would migrate
to gold-spine evidence.** Dropping the legacy `fid` column today
discards 5,532 rows of `dogs`/`practical`/`governance`/`access`
evidence with no gold-side reader yet built.

**To unblock**: do harmony phases 6.3b (`populate_from_park_operators_gold`)
and 6.3c (`populate_from_park_url_gold` + `populate_from_research_gold`).
Once those gold-spine populators exist, re-run them on
beaches_gold to re-emit the equivalent evidence keyed on `gold_fid`.
Then the legacy `fid` rows are safe to drop.

## The dependency loop

```
phase 7.3 (drop legacy)
  ├── needs locations_stage abandoned
  │     └── needs admin-update-location migrated to beaches_gold
  │           └── small edge function refactor; out of harmony scope today
  └── needs legacy beach_enrichment_provenance.fid evidence migrated
        └── needs harmony phases 6.3b + 6.3c
              └── DEFERRED per Franz 2026-05-03; un-defer triggers in
                  project_harmony_pipeline_migration.md
```

## What we DID drop

Nothing. Audit only — no DDL, no DELETE.

## Resume entry conditions

Pick 7.3 back up when **both** are true:

1. `admin-update-location` writes to `beaches_gold` (not `locations_stage`)
   AND admin pages have been re-pointed.
2. Harmony phases 6.3b + 6.3c have shipped, and a backfill has emitted
   gold-spine evidence equivalent to (or replacing) the 5,532 legacy
   rows.

Order doesn't matter — both blockers are independent.

---

## Resolution 2026-05-03 EOD

**Phase 7.3 part 2 SHIPPED.** Migration:
`supabase/migrations/20260503_harmony_phase7_3c_drop_legacy_fid.sql`

Sequence executed:
1. **Added Huntington Beach Dog Beach to arena → beaches_gold** (fid 9717)
   via `seed_arena_beach.py`. Was the only beach with truly hand-typed
   manual evidence (4 rows, legacy_fid 999000001) and was a real
   coverage gap.
2. **Re-attached the 4 manual rows** to gold_fid=9717. Wrote canonical
   entries to `beach_dog_policy` (off-leash with the rich zone
   description) + `beach_amenities` (parking/restrooms/hours). Set
   `beaches_gold.open_time`/`close_time` from the practical evidence.
   Ran `populate_polygon_containment_gold(9717)` → c1=394 (HB city),
   county=06059 (Orange).
3. **Archived legacy fid mapping** to
   `beach_enrichment_provenance_legacy_fid_archive` (5,532 rows preserving
   id → legacy_fid mapping for future archeology).
4. **Dropped 32 legacy ingest functions**: populate_all, populate_from_*
   (10), _emit_evidence_from_park_url, _rank_park_url_evidence,
   _resolve_dogs/governance/practical/research_evidence/tiger_vs_operator,
   _promote_dogs/governance/practical_to_stage, _compute_review_flags,
   flag_dogs_consistency, pick_canonical_evidence, resolve_access/dogs/
   governance/practical, populate_governance_from_name,
   populate_dogs_from_governing_body, populate_layer1_geographic.
5. **Dropped FK** `beach_enrichment_provenance_fid_fkey` →
   `locations_stage(fid)`.
6. **Dropped legacy `fid` column** with CASCADE — auto-dropped 3 indexes
   (`bep_fid_group_idx`, `bep_one_canonical_per_group`,
   `bep_one_per_fid_group_source_url`).

What survived (intentionally):
- `cpad_units_near_beach`, `staging_find_dedup_pairs`,
  `staging_find_neighbor_inheritance` — touch `locations_stage` for
  staging dedup but NOT the dropped column. Continue to work.
- `locations_stage` table itself (862 rows of curator state,
  `admin-update-location` still wires here from `admin/location-editor.html`)
- `admin-update-location` edge function (deprecated banner, still functional)

Verified post-drop:
- `populate_polygon_containment_gold(8473)` → emitted=3, resolved=3,
  promoted=0 (steady-state; gold pipeline unaffected)
- `beaches_gold.fid=9717` Huntington Beach Dog Beach has
  `c1_jurisdiction_id=394`, `county_geoid="06059"`, dog_policy + amenities

Remaining 7.3 work (still parked):
- locations_stage retirement — needs `admin-update-location` migration
  to actually be decommissioned (curator UI cutover from `location-editor.html`
  to `beach-editor-gold.html`). Phase 8 slice 5 deprecated the legacy
  editor with a banner; actual retirement of `admin-update-location`
  endpoint requires a curator-side cutover decision.
