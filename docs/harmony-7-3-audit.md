# Harmony phase 7.3 — locations_stage + legacy fid drop audit (2026-05-03)

**TL;DR — phase 7.3 is BLOCKED.** Two independent dependency families.
Audit complete; nothing dropped. Resume when both unblockers land.

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

**UPDATE 2026-05-03 EOD (post-backfill):** Spatial backfill via locations_stage.geom + ST_DWithin(50m) against beaches_gold mapped **1,087 of 5,532 legacy rows** to gold_fid. Remaining 4,445 stranded — predominantly duplicates of gold-side evidence emitted by today's gold populators (381 dogs/park_url, 330 governance/tiger_places, 310 dogs/research, etc. — all have their gold-side equivalents written by phases 6.3b/c). Genuine orphans (legacy fid with no nearby beaches_gold beach) are a smaller subset that needs case-by-case review before column drop. See `supabase/migrations/20260503_harmony_phase7_3_legacy_evidence_backfill.sql`.

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
