## Summary

11 logical commits covering the past three days of work across the
data layer, ingest pipelines, photo loaders, dedup pipeline, and
curate UI.

### Highlights

- **Dedup pipeline**: 278 → 0 active same-name dup pairs cleared.
  Pipeline now state-aware + name-group-aware + cross-cluster-distance-aware.
  Wired into `run_state_pipeline.py` as two phases (`dedup` and
  `dedup_distance_name`).
- **Flickr loader v2**: 5km → 0.5km radius fix (Flickr's
  interestingness-sort + per_page=20 was crowding out the
  actually-near photos with famous-but-far landmarks). WA hourly hit
  rate **5% → 71%**. System-wide: 50 → 936 beaches with Flickr
  photos.
- **Wikimedia loader v2**: name-match scoring port, fixed stale
  `DELETE source='wikimedia_commons'` bug that never matched stored
  `'wikimedia'` (silently additive for years), insect-genera
  additions to NEGATIVE_TERMS.
- **State-relative scoring (Matrix C')**: `catchment_state_pct` via
  `percent_rank()` per state; coastal-region cluster fallback for
  low-N states. Replaces national-rank classifier so MS beaches
  aren't penalised against LA-scale cities.
- **Governance resolver**: federal denylist (BLM/USCG/military) +
  polygon-containment fallback. CA audit flipped 14 beaches; tribal
  lands regression caught in self-audit before user-facing impact.
- **ADA accessibility**: 4-phase JSONB schema (parking, path-to-sand,
  beach mat, wheelchair rental, accessible viewing). 239 beaches now
  have multi-dimensional structured accessibility (16 with real
  wheelchair-rental signals, 16 with beach mats).
- **Description generator v2**: address anchors (bullet 8), nearest-
  neighbor disambiguation (9), dog-friendly POI extensions (10), OSM
  crowd-voice notes (11), accessibility surfacing (12). Lifeguard
  phrasing: "seasonal lifeguards" not "on duty". Scope guard against
  schema-leak phrasing ("source page mentions", etc.). 5-8 sentence
  target with sentence-hygiene rules.
- **Photo curation infrastructure**: `beach_photo_rejected` tombstone
  table + RPC, parametric photographer blocklist
  (`beach_photo_blocked_photographers(p_source)`), curation progress
  source filter, `get_dog_photo_fids` RPC backing the 🐶 Dog only
  filter.
- **Curate UI overhaul**: source dropdown cascade (state → county →
  beach), 🐶 Dog only filter (also restricts photo grid), photo card
  compaction (180px → 140px image, one-row meta, dropped attribution
  clutter), always-distance sort, history-aware ← Back button,
  silent county-complete auto-advance, OSM-France → OSM.de tile swap
  (Franz was being 429'd on `.fr` pool).

### Operational results from this session

| metric | before | after |
|---|---|---|
| Active same-name dup pairs | 278 | 0 |
| Beaches with Flickr photos | 50 | 936 |
| Total Flickr photos in DB | 119 | 3,693 |
| WA hourly Flickr hit rate | 5% | 71% |
| CA daily Flickr coverage | 0 beaches | 105 beaches (83%) |
| ADA-curated beaches | 72 | 239 |

## Test plan

- [ ] All ~30 migrations applied successfully in prod (already done
      during the session)
- [ ] `run_state_pipeline.py` runs cleanly per-state for
      CA/MA/WA/OR/RI/DE — `dedup` and `dedup_distance_name` phases
      report sane kill counts
- [ ] Curate UI loads, source/county/beach dropdowns cascade,
      Save and Next advances cleanly, 🐶 Dog only filters as expected
- [ ] No regression in CA description regeneration cohort
      (Asilomar / Mile Rock schema-leak audit still pending —
      separate followup)
- [ ] Flickr loader rate-limit retry logic survives a fresh
      large-batch run without dropping more than ~1% of beaches
- [ ] beach-editor-gold + curate render with matching tile data
      (osm.de) and beach polygons visible

## Followups (not in this PR)

- 3 cross-cluster dedup stragglers needing manual merge (Harris
  Beach OR — already fixed mid-session — Pico Beach MA — already
  fixed — and one more OR)
- CA hourly Flickr refire delivered 67 of 71 — the 4 misses likely
  need name-match diagnostic
- Tier 2 photo curation ML spec pinned (`project_photo_curation_ml_tier2_spec.md`)
- Beach coordinate validation against second source
  (`project_beach_coord_validation.md`) pinned

🤖 Generated with [Claude Code](https://claude.com/claude-code)
