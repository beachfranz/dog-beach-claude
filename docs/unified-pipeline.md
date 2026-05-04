# Unified Pipeline — soup-to-nuts spec for landing → gold

**Status:** Design memo, written 2026-05-04. Not yet implemented. Lays out the
target architecture for collapsing today's multi-path ingest+enrichment
machinery into one canonical pipeline.

**One-line:** Five layers, one canonical implementation per layer, demand-driven
extraction, manual override always wins, state-config drives polygon choice.

---

## Why this exists

The catalog ingest pipeline grew organically over ~14 days (303 migrations,
85+ scripts, 89 edge functions). What works works well — the harmony merge
landed 40 commits cleanly — but the pipeline has accumulated multiple parallel
paths that don't align:

- **4 paths from arena → `beaches_gold`** with different end states (some
  backfill `noaa_station_id` + `location_id` slug, others don't). We hit this
  bug 2026-05-03 with 9 newly-promoted test beaches.
- **5 LLM extraction scripts** with overlapping URL pools, prompt templates,
  and write targets.
- **3 paths to write `beach_dog_policy`** — gold resolver, legacy promote
  script, curator UI.
- **CCC orphaned** — loaded into `ccc_access_points`, but `arena` filters it
  out at ingest, and there's no path for CCC's high-fill amenity data
  (parking 100% / restrooms 97% / lifeguard 80% in working_set) to reach the
  gold spine.
- **Verdict cascade ≠ canonical** — `recompute_all_dogs_verdicts_by_origin`
  writes `beach_verdicts`, but HTML reads `beach_dog_policy`. Two parallel
  "what's the dog policy" calculations.
- **Extraction reaches 10% of gold.** 76 of 774 active beaches have any LLM
  extraction. The 698 unextracted are blocked by URL pool starvation.
- **JSON parser drops 80% of LLM output.** ~228 already-extracted structured
  values across 57 beaches sit unused in `raw_response` JSON. Closes
  `time_windows` (7.9%) + `seasonal_closures` (14.2%) gaps for $0.

This spec proposes one canonical flow with explicit contracts at each layer
boundary.

---

## Current state inventory

### Tables (~80 in `public`)

```
TIER 0 — RAW LANDINGS                   TIER 1 — REFERENCE / EXTERNAL
  osm_landing       (20K)                  cpad_units        (17K, 77MB)
  poi_landing        (8K)                  osm_features      (20K, 66MB)
  ccc_landing      (1.6K)                  jurisdictions    (1.6K, TIGER)
  us_beach_points    (8K, legacy)          counties           (3K)
                                           noaa_stations    (192)
                                           military_bases    (89)
                                           tribal_lands     (138)
                                           nps_places       (2.3K)
                                           csp_parks/places  (462/288)
                                           mpas             (155)
                                           waterbodies      (3.1K)

TIER 2 — MASTER INVENTORY               TIER 3 — CROSS-STATE SPINE
  arena            (2.5K, 1,251 act)       beaches_gold      (774, 773 act, 312 score)
   ↑ source_code: osm/poi/manual           gold_set_membership (47)
   ↑ NO ccc — filtered out                 beach_policy_gold_set (468)

TIER 4 — CANONICAL OVERLAYS             TIER 5 — EVIDENCE / EXTRACTION
  beach_dog_policy    (211)                beach_enrichment_provenance (6.9K)
  beach_amenities     (348)                beach_policy_extractions (5.4K, 76 beaches)
  operators          (1.2K)                policy_research_extractions (1.0K)
  operator_dogs_policy (111)               park_url_extractions (655)
  operator_policy_exceptions (209)         operator_policy_extractions (176)
  cpad_unit_dogs_policy (226)              extraction_calibration (1.5K)
  cpad_unit_policy_exceptions (76)         extraction_prompt_variants (115)

TIER 6 — CONSUMER SURFACE               TIER 7 — DEPRECATED-BUT-ALIVE
  beach_day_hourly_scores  (75K)           locations_stage         (862)
  beach_day_recommendations (3.1K)         beaches_staging family  (~3K, 3 tables)
  beach_locations          (1.1K, view)    verdict_snapshot_pass*  (4 tables, 15K)
                                           beach_enrichment_provenance_legacy_fid_archive (5.5K)
```

### Processes

```
SCRIPTS (45 top-level + 41 one-offs):
  Loaders (10):    load_cpad*, load_states*, load_counties*, load_us_beach_points,
                   load_places, load_staging_beaches
  Extractors (8):  extract_beach_policies, extract_for_orphans, extract_for_gold_v3,
                   extract_research_v2, extract_from_park_url, extract_operator,
                   extract_operator_dogs_policy, extract_cdpr_master_pet_table
  Promoters (3):   promote_arena_to_beaches_gold, promote_production_to_beach_dog_policy,
                   sync_beaches_gold_to_beaches
  Seeders (2):     seed_arena_beach, bulk_promote_socal
  Scrapers (5):    scrape_bringfido, scrape_californiabeaches, scrape_dogtrekker,
                   scrape_websearch, scrape_cdpr_park_pages

DB FUNCTIONS:
  Containment populators:  populate_polygon_containment_gold + 5 children
  Source populators:       populate_from_cpad_gold + 4 source variants
  Resolvers:               _resolve_dogs_gold, _resolve_governance_gold,
                           _resolve_practical_gold, _resolve_polygon_containment
  Cascade:                 recompute_all_dogs_verdicts_by_origin

EDGE FUNCTIONS (89):
  Consumer reads (5),  cron writers (3),  v2-* catalog ingest (~30 legacy),
  admin-* CRUD (~25),  jurisdiction checks (~10)

DAGSTER ASSETS (8 modules):
  arena, gold, ingest, verdicts, consumer_pipeline, external_sources,
  dbt_assets, frontend
```

---

## The current actual flow

```
   external                                  ┌─ ccc_landing (1.6K)
   sources       ───────────────►            │   ↓ admin-load-ccc edge fn
                                             │   ccc_access_points (1.6K)  ◄── ORPHANED
                                             │                             from gold flow

  Geoapify ──► load_us_beach_points ──► poi_landing (8K)        ┐
  OSM      ──► fetch_osm_*           ──► osm_landing  (20K)     ├─► arena (1,251 active)
                                                                │     [CCC EXCLUDED]
                                                                │
                                                                ▼
                          ┌──────────────────────────────────── arena ─┐
   promote_arena_to_gold ─┤                                             │
   seed_arena_beach.py    │                                             │
   bulk_promote_socal.py  │  (4 different code paths!)                  │
   direct INSERT          │                                             │
                          ▼                                             │
                  beaches_gold (774, 773 active)                        │
                          │                                             │
                          ├──► populate_polygon_containment_gold ──┐    │
                          │     (5 sub-populators, 100% coverage)  │    │
                          │                                        │    │
                          ├──► extract_beach_policies (76/774)    ─┤    │
                          ├──► extract_for_orphans                  │    │
                          ├──► extract_research_v2 (precision)      │    │
                          ├──► extract_from_park_url                │    ▼
                          ├──► extract_for_gold_v3                  │   beach_policy_extractions
                          │     (5 different extraction scripts)    │       │
                          │                              populate_from_*_gold (5 functions)
                          │                                         ▼       ▼
                          ▼                          beach_enrichment_provenance (6.9K)
            beach_dog_policy (211, curator) ◄─── _resolve_dogs_gold
            beach_amenities  (348, curator) ◄─── _resolve_practical_gold
                          │
                          ▼
                  consumer surface
```

---

## The proposal: five layers

Each layer has **one canonical implementation** plus a small set of helpers.
Everything else gets retired or marked deprecated.

### Layer A — Source ingest (raw mirrors stay fresh)

```
Inputs:    external APIs (Geoapify, OSM Overpass, CCC ArcGIS, CPAD,
           TIGER, NOAA, NPS, CSP, etc.)
Outputs:   *_landing tables + reference tables (cpad_units, jurisdictions,
           counties, military_bases, tribal_lands, nps_places, etc.)
Trigger:   scheduled (Dagster sensors per-state, cadence per-source)
Entry:    one Dagster asset per landing source, named `<source>_landing_refresh`
Rule:     no business logic, no joins, no filters; just keep the raw mirror current
```

**Why this layer:** Decouples external API health from pipeline correctness.
A failed CCC ArcGIS pull doesn't break gold spine builds — it just means
the existing `ccc_landing` snapshot is the most recent.

### Layer B — Spine build (deduped, identity-resolved catalog)

```
Inputs:    *_landing tables (POI + OSM + manual)
Outputs:   arena (master inventory) → beaches_gold (promoted spine)
Trigger:   arena recomputed on landing change; gold promotion is curator-gated
Entry:    Dagster asset `arena_build`, then ONE script `promote_to_gold(fids[])`
Rule:     one beach = one fid in beaches_gold. Identity comes from OSM + POI
          (status quo). State-curated layers (CCC, CPAD, BCDC) are enrichment
          overlays, never beach-identity sources.
```

**The promotion consolidation:** Replace the 4 paths
(promote_arena_to_beaches_gold.py, seed_arena_beach.py, bulk_promote_socal.py,
direct INSERT) with one Python entry point:

```python
def promote_to_gold(fids: list[int], score: bool = False) -> dict:
    """Promote arena rows to beaches_gold with full enrichment."""
    # 1. INSERT into beaches_gold from arena
    # 2. Backfill noaa_station_id (nearest, unbounded KNN)
    # 3. Backfill location_id slug (slugify(name) + collision check)
    # 4. Backfill timezone (lookup by lat/lon)
    # 5. Run populate_polygon_containment_gold (Layer C trigger)
    # 6. Optionally flip is_scoreable (if score=True)
    # 7. Schedule URL discovery + extraction (Layer D trigger)
    # 8. Write promotion_runs audit row
    # 9. Return summary {fid, slug, noaa, evidence_counts}
```

Wrap as Dagster asset `gold_promotion`. CLI tool calls the same function.
Curator UI calls the same function via edge function. **Single source of
truth for what "promoted to gold" means.**

### Layer C — Spatial enrichment (deterministic, no LLM)

```
Inputs:    beaches_gold + reference polygon layers (cpad, jurisdictions,
           counties, military, tribal, nps, csp, mpas, waterbodies)
Outputs:   beach_enrichment_provenance (polygon_containment + governance evidence)
Trigger:   nightly per-beach; on-demand on gold mutation (Layer B step 5)
Entry:     populate_polygon_containment_gold (already shipped)
Rule:      100% reproducible, idempotent, fast. State_config drives layer choice.
```

**State configuration:** `state_config` + `state_polygon_config` (already
designed in `project_polygon_sourcing.md`) drives which polygon layers
apply per state. CA uses CPAD; OR/WA/etc. use PAD-US. Adding a new state
becomes a config row, not code.

**Already shipped** — this is the layer harmony delivered. Nothing to
redesign here, just wire the state-config selector.

**Parked for discussion:** CCC integration as a point-overlay enrichment
populator. CCC's lat/lng is the access point semantic (not a beach), and
asymmetric-trust analysis 2026-05-04 showed YES values agree 95-100% with
other sources while NO values are unreliable (23% agreement with park_url).
There's a real win here for amenity coverage (especially `disabled_access`
which nothing else covers), but the design has more nuance than the simple
overlay framing — needs a live discussion before speccing. See
"Open questions" below.

### Layer D — Policy enrichment (LLM + scrapes, demand-driven)

This is the layer that needs the most work. Today's reality: 5 competing
extraction scripts with overlapping URL pools, two prompt-template families
(json_evidence wins for enums, json_structured drops most of its output),
and a city_policy_sources table populated for only 1 of ~50+ CA cities.

```
Inputs:    beaches_gold + URL pool (city_policy_sources, cpad.agncy_web,
           operators.website, OSM tags, CCC.restrictions, web-search)
Outputs:   beach_policy_extractions → beach_enrichment_provenance →
           beach_dog_policy + beach_amenities (canonical, picked by resolver)
Trigger:   demand-driven via `policy_extraction_queue` view
           (priority = is_scoreable_weight × (1 - field_coverage))
Entry:     ONE script extract_for_beach(fid, fields=[...])
           replacing all 5 existing extractors
Rule:      json_evidence prompt template (closed enum + verbatim quote) is
           canon. Per-field source weighting from calibration drives resolver
           tiebreaks. Manual entries always win.
```

**Sub-step D1 — URL discovery.** For a given beach, gather candidate URLs
from:
1. `park_url_extractions.source_url` (prior runs)
2. `operators.website` + `operators.dog_policy_url` via cpad_unit / c1 city / county
3. `cpad_units.agncy_web` for the containing CPAD unit
4. `city_policy_sources.url` for the place_fips
5. `osm_features.tags->>'website'` within 100m
6. **NEW: web-search step** for cold-start beaches with empty pools — call
   Brave Search API or Anthropic web_search with `{name} {city} {state} dogs`
7. **NEW: CCC.restrictions** — for the 73 beaches with CCC restrictions
   prose, the prose itself becomes the "page content" (no fetch needed)

**Sub-step D2 — Extraction.** Single canonical extractor:

```python
def extract_for_beach(fid: int,
                     fields: list[str] = None,  # default: all
                     model: str = 'claude-haiku-4-5',  # enum/bool fields
                     escalate_to_sonnet_for: list[str] = ['hours_text', 'evidence_quote']
                     ) -> ExtractionResult:
    """Single entrypoint. URLs from D1, json_evidence prompt template,
    cite-required, write to beach_policy_extractions with arena_group_id."""
```

Replaces extract_beach_policies / extract_for_orphans / extract_for_gold_v3 /
extract_research_v2 / extract_from_park_url. Old scripts become thin wrappers
that call the new entry point with preset fields.

**Sub-step D3 — JSON exploder.** Parse `raw_response` JSON, explode multi-key
payloads into per-key beach_policy_extractions rows OR write directly to
canonical tables. Closes the 228-value gap from
`project_json_structured_dropped_payload.md` for $0.

**Sub-step D4 — Source population.** Existing populate_from_*_gold functions
read beach_policy_extractions and write evidence rows. Already shipped on
harmony. No change.

**Sub-step D5 — Resolver with per-field source weights.** The
`_resolve_dogs_gold` / `_resolve_practical_gold` functions currently use
flat source priority (manual > llm > park_url/park_operators > research >
old_school_llm > spatial). Replace with per-field weighted priority:

```sql
-- pseudocode
case field_name
  when 'dogs_allowed' then research(0.91) > park_url(0.86) > old_school_llm(0.69)
  when 'leash_policy' then research(0.83) > old_school_llm(0.77) > park_url(0.75)
  when 'has_lifeguards' then park_url(0.90) > old_school_llm(0.82)
  when 'has_restrooms', 'has_parking' then ccc(asymmetric_yes) + old_school_llm(0.95)
  ...
end
```

Weights come from the calibration views shipped on harmony (`gold_*_calibration_binary`).
Manual still wins everything.

### Layer E — Curator (human override, top of priority stack)

```
Inputs:    curator via admin/beach-editor-gold.html
Outputs:   beach_dog_policy + beach_amenities + beaches_gold; also emits
           source='manual' rows into beach_enrichment_provenance for audit
Trigger:   human action
Entry:     admin/beach-editor-gold.html (already shipped)
Rule:      manual = highest priority in resolver. Always wins.
```

**Already shipped** — phase 8 of harmony delivered the canonical-tables
editor. Three edge functions (admin-update-beaches-gold, dog-policy,
amenities) write directly. Soak window passed.

**The one extension:** when curator saves, also write a row to
`beach_enrichment_provenance` with `source='manual'`. Today curator writes
go to canonical tables but bypass the evidence layer, so `gold_evidence_audit`
doesn't show them. Small fix; preserves audit story.

### Layer F — Scoring & consumer surface (subset of gold; the "important" beaches)

```
Inputs:    beaches_gold WHERE is_important + weather (Open-Meteo) +
           tides (NOAA) + crowds (BestTime) + scoring_config
Outputs:   beach_day_hourly_scores (75K) + beach_day_recommendations (3K)
Trigger:   daily cron (7-day fan-out) + hourly cron (NOW refresh) +
           on-demand (admin force-refresh)
Entry:     daily-beach-refresh + get-beach-now edge functions
           (scoring logic in supabase/functions/_shared/scoring.ts)
Rule:      Layer F is a CONSUMER of A-E, never a gate on them. Catalog
           enrichment runs on every gold beach regardless of scoring
           eligibility.
```

**Critical separation from Layers A-E.** Today `is_scoreable` is a single
flag doing two jobs: it gates the daily fan-out AND filters the consumer
surface (find.html `scored_only=true`). The unified model splits them:

- **Every gold beach** flows through Layers A-E (curate, enrich, resolve)
  regardless of whether it's "important" enough to score
- **A subset** flows into Layer F based on an *importance gate*
- A beach can be fully curated, fully enriched, fully consumer-visible
  in `find.html` (full-catalog mode) without being scored

This means Layer D's extraction queue prioritization treats `is_important`
as a *bias signal*, not a *filter*. Cold-start beaches outside the
important set still eventually get extracted; they just sit lower in the
queue.

**The importance gate (parked for design — see Open Questions).** Franz
flagged 2026-05-04 that the definition of "important" needs live
discussion. Candidate signals to weigh:
- Manual curator flip (today's `is_scoreable=true`, retained as override)
- Data completeness threshold (has dog_policy + amenities populated)
- Geographic priority (coastal CA, distance to population center)
- User-facing demand (favorites count once accounts ship, search-result
  hits, view counts)
- Quality threshold (curator-reviewed; not auto-promoted)
- Negative gates (federal/military/private = automatically not important)
- Operational cost (each scored beach = 4 external API calls × 24 hours
  × 7 days; ~672 calls/beach/week)

The function probably looks like
`importance_score = manual_override ?? weighted_sum(signals)` with a
threshold tunable per-state.

**Known parked items in Layer F** (real bugs/work items):

1. **`dbt build` is broken** — `stg_beaches.sql` + `consumer_beach_with_verdict.sql`
   reference `public.beaches` which was dropped 2026-05-02. Either repoint
   to `beaches_gold` or retire the models.
2. **`verdicts.beaches` Dagster asset is stale** — same root cause; the
   write-back to a dropped table. Decision parked: retire vs repoint to
   `beach_dog_policy`.
3. **`get-beaches-find scored_only=false`** — RPC default LIMIT bug; full-catalog
   mode returns the same 309 as scored-only. Once the importance gate is
   defined this becomes the right default for the "browse all beaches" UX.
4. **Verdict cascade orphaned** — `recompute_all_dogs_verdicts_by_origin`
   writes `beach_verdicts` (3.8K rows) but no consumer reads it. Was a
   parity-check vs `beach_dog_policy`. Either retire or formalize as a
   calibration view.
5. **BestTime crowd data noise** — known unreliable for many beaches. Could
   route through evidence layer for resolver treatment; today it's a
   direct read with no provenance.
6. **Hourly NOW vs daily refresh race window** — hourly NOW writes overwrite
   the forecast row for the current hour. Daily refresh on its next run
   writes the forecast back over the observed value. Subtle.
7. **`is_scoreable` is manual** — every flip is a curator action. Once
   importance is defined, much of this should be derived.

**Sub-layers within F** (existing pipeline, documented for completeness):

- F1: importance gate evaluation (TODAY: manual `is_scoreable`; FUTURE:
  importance function from parked design)
- F2: external data fetch (Open-Meteo weather + NOAA tides + BestTime crowds)
- F3: scoring engine (`scoreHours` from `_shared/scoring.ts` — 4-tier status,
  composite 0-100 score, best-window selection)
- F4: narrative generation (Claude, per-beach per-day summary)
- F5: output write (`beach_day_hourly_scores`, `beach_day_recommendations`)
- F6: live override (hourly `get-beach-now` overwrites forecast for current
  hour with observed actuals)

---

## Cross-cutting concerns

### State configuration

`state_config` + `state_polygon_config` tables drive state-specific layer
choices. Adding OR/WA = config change, not code. (Designed in
`project_polygon_sourcing.md`, partially seeded.)

### Source-quality model

Per-field source weights (the calibration findings) baked into the
resolver. Continuously refined via `gold_*_calibration_binary` views as
curator gold-set grows. Manual override always at top.

### Audit trail

`gold_evidence_audit` view (already shipped) shows per-beach per-field-group
provenance trail with canonical pick highlighted. Curator UI surfaces this.
Curator manual entries flow through evidence layer (not bypass) so audit
remains complete.

### Demand-driven extraction queue

```sql
create view policy_extraction_queue as
select g.fid, g.name,
       g.is_important,                                  -- gate from Layer F's importance fn
       count(*) filter (where bep.field_group = 'dogs') as dogs_evidence_count,
       count(*) filter (where bep.field_group = 'practical') as practical_evidence_count,
       (case when g.is_important then 10 else 1 end)    -- BIAS, not filter
       * (1 - dogs_evidence_count / 4.0) as extraction_priority
  from beaches_gold g
  left join beach_enrichment_provenance bep on bep.gold_fid = g.fid
 where g.is_active
 order by extraction_priority desc;
```

Dagster sensor pulls top-N from this view nightly and queues
`extract_for_beach` calls. **`is_important` is a bias, not a filter** —
cold-start beaches outside the important set still extract eventually;
they just sit lower in the queue. Catalog enrichment runs on the full
gold corpus.

### One Dagster sensor per layer transition

```
landing_changed_sensor    → triggers arena_build
arena_promoted_sensor      → triggers gold_promotion(fids)  [curator-gated]
gold_changed_sensor        → triggers populate_polygon_containment_gold
extraction_queue_sensor    → triggers extract_for_beach (top-N from queue)
extraction_complete_sensor → triggers populate_from_*_gold + resolvers
```

---

## Concrete first moves

In priority order, sized for weekend-scale chunks:

### 1. Promotion consolidation (~1 day)

Replace 4 promotion paths with one `promote_to_gold(fids[])` Dagster asset.
Inserts, runs containment populators, backfills noaa+slug+timezone,
schedules extraction, writes audit row. **Closes the bug we hit
2026-05-03.**

Existing scripts become thin wrappers. seed_arena_beach.py becomes
`promote_to_gold([fid], score=True)`. bulk_promote_socal.py becomes
`promote_to_gold(fids_for_county('Los Angeles'), score=True)`.

### 2. JSON parser exploder (~half day)

$0 fix; recovers ~228 structured values across 57 beaches; closes
time_windows + seasonal_closures gaps. Spec in
`project_json_structured_dropped_payload.md`.

PL/pgSQL function or one-shot Python script. Reads `beach_policy_extractions
WHERE variant_key IN ('json_structured','json_evidence_v2')`, JSON-parses
raw_response, either explodes into per-key rows or writes directly to
canonical tables.

### 3. One extraction entry point (~1-2 days)

Wrap the 5 extractors into `extract_for_beach(fid, fields)` with a
single URL-pool builder + json_evidence prompt template. Old scripts
become thin shims.

Sub-task: implement web-search URL discovery for cold-start beaches.

### 4. Per-field source weighting in resolver (~half day)

Replace flat source priority in `_resolve_dogs_gold` /
`_resolve_practical_gold` with per-field weights from calibration. Manual
still wins.

### Then sequence the cleanup

- **6.** Retire `locations_stage` + `admin-update-location` + `location-editor.html`
  (cutover plan in next-session memo).
- **7.** Retire `beaches_staging` family (3 tables, ~3K rows, all v2 legacy).
- **8.** Retire `verdict_snapshot_pass*` tables (4 tables, 15K rows).
- **9.** Reconcile cascade vs canonical — either retire `beach_verdicts`
  (it's not the consumer surface anyway) or formalize it as a parity-check
  view.
- **10.** Drop the legacy `extract_*` scripts that are now wrappers (only
  if their callers also migrate).

---

## What gets retired

```
DEPRECATED (kept until cutover):
  scripts/seed_arena_beach.py        → wrapped by promote_to_gold([fid], score=True)
  scripts/bulk_promote_socal.py      → wrapped by promote_to_gold(county_fids, score=True)
  scripts/promote_arena_to_beaches_gold.py → replaced
  scripts/extract_beach_policies.py  → wrapped by extract_for_beach
  scripts/extract_for_orphans.py     → wrapped
  scripts/extract_for_gold_v3.py     → wrapped
  scripts/extract_research_v2.py     → wrapped
  scripts/extract_from_park_url.py   → wrapped
  scripts/promote_production_to_beach_dog_policy.py → replaced by resolver

DROP (after cutover):
  locations_stage table              → blocked on admin-update-location retire
  beaches_staging table              → v2 pipeline legacy
  beaches_staging_new table          → v2 pipeline legacy
  beaches_staging_new_v1_snapshot    → v2 pipeline legacy
  verdict_snapshot_pass6/7/9/10/11   → cascade history snapshots
  beach_enrichment_provenance_legacy_fid_archive → harmony backup
  truth_snapshot_before_50           → one-off snapshot
  geo_entity_response                → old geocode cache
  us_beach_points                    → legacy CSV mirror, callers should migrate to arena
```

---

## Open questions / parked for live discussion

### Importance gate (Layer F entry condition)

What makes a beach "important" enough to score? Today the gate is a single
manual flag (`is_scoreable`) flipped by curator. The unified model needs
a more principled definition because:

- Catalog enrichment (Layers A-E) covers every gold beach (~774); only a
  subset (~312 today) gets scored
- Each scored beach costs ~672 external API calls/week (weather × hours ×
  days) plus crowd data and Claude narrative generation
- `find.html`'s "browse full catalog" mode needs the consumer-facing
  filter to make sense even for unscored beaches
- A beach can be fully curated, fully consumer-visible, but not "important
  enough" to forecast — and that should be a clean state, not an
  underspecified one

Candidate signals (to weigh, not all required):
- Manual curator override (preserved; always wins)
- Data completeness (has dog_policy AND amenities AND noaa station)
- Geographic priority (coastal, near population centers, state-config
  driven)
- User-facing demand (favorites count once accounts ship, search hits,
  view counts)
- Quality threshold (curator-reviewed vs auto-promoted)
- Negative gates (federal/military/private/inactive auto-not-important)

Open: how to weigh the signals, where the threshold sits, whether
importance is binary or has tiers (Tier 1 hourly NOW + daily 7-day,
Tier 2 daily 7-day only, Tier 3 weekly snapshot), and how it interacts
with state-config (per-state importance budget?).

**Parked until live discussion.**

### CCC integration

CCC has rich amenity data (parking 100% / restrooms 97% / lifeguard 80% /
disabled_access 98% fill on the 267-row working_set), strong cross-source
agreement on YES values, and uniquely covers `has_disabled_access` which
nothing else in the pipeline tracks.

The naive framing ("CCC as Layer C point overlay") is too simple — there
are unresolved questions about:

- The semantic gap between CCC's access-point lat/lng and arena's beach
  geometry, and how that should propagate through the resolver
- Whether CCC's `restrictions` prose (73 rows of dense seasonal/dog/access
  rules) deserves its own extraction path versus joining the Layer D
  general extraction
- Whether terrain flags (sandy/rocky/bluff/dunes/tidepool) belong in
  `beach_enrichment_provenance` as evidence or directly on `beaches_gold`
  as a typed feature column
- How CCC orphans (points with no arena match) should be treated —
  silent drop, audit log, or a "missing from OSM/POI" review queue
- Asymmetric trust mechanics: how the resolver should encode
  "CCC=YES emits high-confidence positive evidence; CCC=NO does not
  emit at all"

These need a live discussion. **Parked until then.**

## What this doesn't change

- **harmony layer (Layer C, spatial enrichment)** is already correct;
  no redesign needed
- **curator UI (Layer E)** already shipped phase 8; just add
  evidence-write fan-out
- **scoring engine (Layer F sub-layer F3)** — the `scoreHours` math in
  `_shared/scoring.ts` is fine as-is; the parked items are around the
  importance gate, broken dbt models, and stale assets, not the scoring
  algorithm
- **Dagster + dbt orchestration** stays as-is; this just consolidates
  what gets called
- **state_config + polygon_layer_registry design** already locked in
  `project_polygon_sourcing.md`; this spec assumes it ships

---

## Cross-references

- `project_unified_dog_policy_pipeline.md` — earlier 5-step Dagster chain
  spec for dog-policy specifically; this spec generalizes that to all
  fields and adds the layer model
- `project_polygon_sourcing.md` — state_polygon_config for adding states
- `project_resolution_rules_design.md` — 10 dimensions of cross-source
  resolution; per-field source weighting (Layer D5) implements Phase 1
- `project_json_structured_dropped_payload.md` — Step 2 ($0 win)
- `project_extraction_calibration.md` — source ranking that feeds Layer D5
- `project_pipeline_phases.md` — Access vs Enrichment phase model
- `project_old_pipeline_borrowable_patterns.md` — 26 patterns from the
  original pipeline still relevant
- `project_harmony_pipeline_migration.md` — what Layer C delivered
- `feedback_ccc_free_workflow.md` — CCC as overlay, not spine; this spec
  honors that by routing CCC through Layer B (identity overlay) and
  Layer D (amenity evidence), never as primary policy source
