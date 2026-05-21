# Codify Deep-Extract — Full Spec

End-to-end spec: codified text → structured rows → consumer-ready JSON.

Supersedes / merges:
- `docs/codify_deep_extract_schema_design.md` (DB / vocab side)
- Pilot results 2026-05-20 (Del Mar / Coronado / Fiesta / Carlsbad / etc.)
- Locked: `project_zone_rules_design_locked.md` (existing zone_rules shape)

## 1. Consumer-side contract — what `ZoneRulesBlock.render()` expects

`src/zone-rules-block.js` consumes `beach_dog_policy.zone_rules` (jsonb). The
contract is locked and any new extraction output MUST map cleanly into:

```jsonc
{
  "seasons": [                              // OPTIONAL wrapper; if absent,
                                            // top-level "regions" is wrapped
                                            // as seasons[0]
    {
      "name": "Off-leash season" | null,    // shown on card header when >1
      "dates": {                            // null OR all-year season
        "start": "MM-DD",                   // recurring annual
        "end":   "MM-DD"
      } | null,
      "regions": [
        {
          "name": "Voice-control area" | null,
                                            // null = whole-beach default;
                                            // string = sub-zone (geographic)
          "sections": {                     // KEY = section enum
                                            // (sand, water, playground, etc.)
            "sand": {
              "rule": "off_leash"           // PRIMARY display rule
                    | "on_leash"
                    | "not_allowed"
                    | "unknown",
              "evidence": {                 // shown on section-click
                "quote":       "...",       // verbatim from source
                "citation":    "...",       // e.g. "36 CFR §2.15"
                "source":      "federal_regulation",   // subtype
                "source_url":  "https://...",
                "authority_tier": 1                    // 1=highest
              },
              "time_windows": [             // OPTIONAL; produces day-time
                                            // tabs in the UI
                { "start": "06:00", "end": "10:00", "rule": "off_leash" },
                { "start": "10:00", "end": "17:00", "rule": "not_allowed" },
                { "start": "17:00", "end": "21:00", "rule": "off_leash" }
              ]
            },
            "restrooms": { "rule": "on_leash" }
          }
        }
      ]
    }
  ],
  "global_notes": [                        // NEW — meta-rules that don't
                                            // belong to a region/section
    {
      "kind": "waste_pickup_required",     // | "nuisance_restriction"
                                            // | "collar_tag_required"
                                            // | "local_stricter_authorized"
                                            // | "no_policy_published"
      "evidence": { /* same shape */ }
    }
  ]
}
```

Renderer behavior (already implemented):
- `seasons[]`: filtered to "current season" via `ctx.viewMonthDay`. >1 → name shown on header.
- `regions[]`: each becomes a card. Region.name=null is suppressed when other regions are named.
- `sections{}`: rendered as emoji pills, color-tinted by rule. Sorted off > on > not_allowed > unknown.
- `time_windows[]`: aggregated across a region → tabs. Active tab auto-picked by `ctx.currentLocalHour`. Each tab shows section pills with rule-in-tab.
- `evidence.quote`: click section pill to toggle visibility.
- **`global_notes` is NEW** — needs a small "Notes" footer added to the renderer (separate spec item).

Section vocab the renderer **already labels** (others fall through to raw name):

```
sand · trails · boardwalk · bluff · dunes · picnic_area · campground
playground · tide_pools · nesting_zones · water_swim · parking_lot
restrooms · showers · restrooms_showers
```

Rule vocab the renderer **already colors**:

```
off_leash · on_leash · not_allowed · unknown
```

## 2. Producer-side contract — what deep-extract emits per source

Pilot v2 (commit 5272de9) emits a flat row list per policy_source. Each row:

```jsonc
{
  "region":          null | "verbatim region name",
  "region_anchor":   null | "verbatim boundary phrase",
  "section":         "sand" | "water" | "playground" | ... | "global",
  "rule":            "on_leash" | "off_leash" | ... | "<new>",
  "rule_modifier":   null | "leash_max_6ft" | ...,
  "operative_status":"operative" | "superseded" | "proposed" | "seasonal",
  "temporal": null | {
    "season":    null | { "start": "MM-DD|anchor", "end": "MM-DD|anchor",
                           "description": "verbatim" },
    "daily":     null | { "start": "HH:MM|dawn|dusk", "end": "HH:MM|dawn|dusk",
                           "description": "verbatim" },
    "year_round": true | false
  },
  "evidence_verbatim": "...",
  "supersedes_baseline": true | false,
  "_is_new":           true | false        // OMIT unless rule/section is new
}
```

## 3. Persistence layer — what lands in the DB

Two-table write per pilot row:

### A. `beach_policy_source` (extended)

```sql
ALTER TABLE beach_policy_source ADD COLUMN region_anchor TEXT;
ALTER TABLE beach_policy_source ADD COLUMN parent_bps_id BIGINT
  REFERENCES beach_policy_source(id) ON DELETE CASCADE;
ALTER TABLE beach_policy_source ADD COLUMN exemption_type TEXT;
ALTER TABLE beach_policy_source ADD COLUMN authority_tier SMALLINT;
ALTER TABLE beach_policy_source ADD COLUMN is_global_note BOOLEAN
  DEFAULT FALSE;
```

Field meaning:
- `region_anchor` — verbatim boundary phrase (deep-extract → geometry workstream)
- `parent_bps_id` — for exemption rows linking back to a baseline (Carlsbad pattern)
- `exemption_type` — categorical: `designated_area` | `service_animal` | `leash_required` | `permit_holder` | `time_window`
- `authority_tier` — denormalized from `policy_source.subtype` for sort speed
- `is_global_note` — when section="global" AND rule is a meta-rule (nuisance_restriction, waste_pickup_required, etc.) → renders in `global_notes`, not in a region's sections.

### B. `beach_policy_source_temporal` (already exists)

The pilot's `temporal{season, daily, year_round}` maps:

| pilot field                  | bps_temporal column                |
|---|---|
| `temporal.season.start`     | `effective_from_md`                |
| `temporal.season.end`       | `effective_to_md`                  |
| `temporal.season` (anchor)  | `anchor_start` / `anchor_end`      |
| `temporal.daily.start`      | `daily_start` (TIME, or text for dawn/dusk) |
| `temporal.daily.end`        | `daily_end`                        |
| `temporal.year_round`       | `window_kind = 'year_round'`       |
| (verbatim)                  | `season_label`                     |

Each bps row → 0 or 1 bps_temporal row. (Multi-layer temporal = multi bps rows.)

### C. Vocab queue (new)

```sql
CREATE TABLE vocab_review_queue (
  id BIGSERIAL PRIMARY KEY,
  vocab_type TEXT NOT NULL CHECK (vocab_type IN ('rule', 'section')),
  value TEXT NOT NULL,
  bps_id BIGINT REFERENCES beach_policy_source(id),
  policy_source_id BIGINT REFERENCES policy_source(id),
  context_snippet TEXT,
  status TEXT DEFAULT 'pending'
         CHECK (status IN ('pending','promoted','aliased','rejected')),
  resolution TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);
```

## 4. Producer → Persistence → Renderer mapping

The transformation pipeline:

```
LLM extraction        → pilot JSON row              (per source)
  ↓ scripts/codify_vocab.py (normalize via aliases)
beach_policy_source   ← INSERT(row)                  (per beach via cascade)
beach_policy_source_temporal ← INSERT(temporal)      (when temporal != null)
  ↓ trigger
_zr_inject_from_policy_sources()                     (existing injector)
  ↓ collates by season/region/section + attaches time_windows
beach_dog_policy.zone_rules                          (the consumer-ready JSON)
  ↓ get_beach_detail edge function
ZoneRulesBlock.render()                              (UI)
```

**Key new injector responsibility:** when N bps rows share (region, section)
with DIFFERENT temporal windows → collate into `sections.{name}.time_windows[]`
on a single section, not N separate sections.

Example — Avila Beach (Port San Luis) pilot output → injector → zone_rules:

```
Pilot rows (3):                                  zone_rules.regions[0].sections.sand:
  region=Avila, sec=sand, daily=sunrise→10:00  ⇒  {
  region=Avila, sec=sand, daily=10:00→17:00       rule: "off_leash",     (most-favorable default)
  region=Avila, sec=sand, daily=17:00→sunset      time_windows: [
                                                    {start: "sunrise", end: "10:00", rule: "off_leash"},
                                                    {start: "10:00",   end: "17:00", rule: "not_allowed"},
                                                    {start: "17:00",   end: "sunset", rule: "off_leash"}
                                                  ],
                                                  evidence: {...} (from highest authority row)
                                                }
```

For meta-rules (rule ∈ {nuisance_restriction, waste_pickup_required, etc.}):
- bps row has `is_global_note = true`
- injector emits to top-level `zone_rules.global_notes[]`, not to a section.

## 5. Vocabulary management

`scripts/codify_vocab.py` (new — see schema design doc for full list):

```python
CANONICAL_RULES = {  # render-mapped to off_leash/on_leash/not_allowed/unknown
  'on_leash':                  {'render_as': 'on_leash'},
  'off_leash':                 {'render_as': 'off_leash'},
  'off_leash_voice_control':   {'render_as': 'off_leash'},
  'on_leash_or_voice':         {'render_as': 'on_leash'},
  'not_allowed':               {'render_as': 'not_allowed'},
  # meta-rules → global_notes instead of region/section
  'nuisance_restriction':         {'is_global_note': True},
  'waste_pickup_required':        {'is_global_note': True},
  'collar_tag_required':          {'is_global_note': True},
  'local_stricter_authorized':    {'is_global_note': True},
  'no_policy_published':          {'is_global_note': True},
}

CANONICAL_SECTIONS = {
  'sand', 'water', 'playground', 'turf', 'walkway', 'restroom',
  'parking_lot', 'picnic_area', 'tide_pool', 'dune_restoration',
  'pier', 'jetty', 'developed_recreation_site', 'swimming_beach',
  'snowy_plover_protection_area', 'global',
  # already in renderer label dict (keep aliases consistent):
  'trails', 'boardwalk', 'bluff', 'dunes', 'campground',
  'tide_pools', 'nesting_zones', 'water_swim',
  'restrooms', 'showers', 'restrooms_showers',
}

RULE_ALIASES = {
  'feces_removal_required':   'waste_pickup_required',
  'waste_disposal_required':  'waste_pickup_required',
  'waste_pack_out_required':  'waste_pickup_required',
  'collar_and_tag_required':  'collar_tag_required',
  'collar_and_tags_required': 'collar_tag_required',
}

SECTION_ALIASES = {
  'restroom':           'restrooms',
  'tide_pool':          'tide_pools',
  'water':              'water_swim',
  # renderer-side icon labels need updating in zone-rules-block.js
  # for genuinely-new sections (playground, turf, walkway,
  # swimming_beach, developed_recreation_site)
}
```

Writer normalizes via aliases before INSERT. Unrecognized rule/section →
INSERT anyway + INSERT vocab_review_queue row.

## 6. Sibling consumers — what else needs this data

`ZoneRulesBlock` is the primary consumer but not the only one. Siblings:

| consumer | what it needs | how shape adapts |
|---|---|---|
| `ZoneRulesBlock` (beach.html, mobile-beach.html, detail.html) | full nested JSON described above | direct |
| `get-beach-summary` edge fn (find.html cards) | flat "best rule today" summary — e.g. "off-leash 6-10am only" | derived: pick active season + region + section=sand → format primary rule + active time_window |
| `get-beach-detail` edge fn | full zone_rules + evidence | direct |
| `description-block.js` (if any) | prose summary (LLM-generated from zone_rules) | reads zone_rules + global_notes to write 1-2 sentence summary |
| `cautions-block.js` | global_notes filtered to caution-class meta-rules + seasonal closures | reads `global_notes` + scans for `not_allowed` rules with `temporal.season` |
| `beach-chat` (beach-chat edge fn) | grounding text for Q&A | concatenates all evidence_verbatim quotes |
| map/`dog_policy_zones` overlay | region_anchor → polygon | separate geo workstream; consumes `region_anchor` field |

## 7. Migration sequence (with rollback points)

1. **DB schema** (5 small ALTERs + 1 CREATE TABLE) — ~30 min, reversible
2. **Vocab module** (`scripts/codify_vocab.py`) — ~1 hr, standalone
3. **Production extract+load** (`scripts/extract_and_load_deep.py`) — ~2 hr
   - Wraps pilot script
   - Normalizes via vocab module
   - Writes bps + bps_temporal
   - Surfaces `_is_new` → vocab_review_queue
   - **Idempotent re-runs** (delete prior rows for ps_id + reinsert)
4. **Injector update** (`_zr_inject_from_policy_sources`) — ~2-3 hr
   - Collate by (season, region, section)
   - Build `sections.{name}.time_windows[]` from sibling bps rows
   - Route meta-rules to `global_notes[]`
   - **Test fixtures**: Del Mar / Fiesta Island / Avila Beach (each tests different aggregation)
5. **Renderer additions** (`src/zone-rules-block.js`) — ~1 hr
   - Add section labels for playground / turf / walkway / swimming_beach / developed_recreation_site / pier / jetty / tide_pool
   - Add `global_notes` rendering (small footer area)
6. **Dry-run on 3 beaches** → spot-check zone_rules JSON shape end-to-end
7. **Bulk run** all ~280 policy_source rows with full_text (~$20)
8. **Codify-side persist patch** (task #94) — write full_text on every codify run going forward
9. **Geometry workstream** (separate — region_anchor → polygon resolution)

## 8. Acceptance criteria

- [ ] Del Mar zone_rules has 4 named regions (north of 29th / 25th-29th band / Powerhouse Park summer prohibition / Powerhouse Park sub-zones)
- [ ] Avila Beach zone_rules has `sand.time_windows` with 3 entries (sunrise-10am / 10am-5pm / 5pm-sunset)
- [ ] Fiesta Island zone_rules has seasonal `seasons[]` for Mission Bay weekday hours (Oct-Mar vs Apr-Oct)
- [ ] Manhattan Beach community_attestation surfaces with `low_authority` flag (visual de-emphasis in renderer)
- [ ] Cross-source duplicates collapse: Siuslaw NF + Oregon Dunes NRA rows with same federal citation render as ONE row in zone_rules (consensus pass)
- [ ] No-text subtypes (mou/lease_agreement/tribal_code/operating_agreement) are tagged for codify-side text persistence backfill
- [ ] All 5 vocab_review_queue entries triaged before bulk rollout
- [ ] `ZoneRulesBlock.render()` displays new sections (playground/turf/walkway/etc.) with proper labels

## 9. Open questions

- Auto-promotion threshold for `_is_new` vocab → canonical (3 hits? 5? curator-only?)
- Re-extract existing 282 policy_source rows once vs only NEW codify going forward? (Recommend: bulk re-extract — $20 one-time)
- Authority tier mapping from subtype: confirm hierarchy
  - federal_regulation = 1, federal_statute = 1, superintendents_compendium = 2,
    state_regulation = 2, state_statute = 2, agency_administrative_policy = 3,
    municipal_code = 3, special_district_ordinance = 3,
    operator_posted_policy = 4, mou / lease_agreement = 4,
    community_attestation = 5, inferred = 5, withdrawn_rulemaking = 6
- Renderer support for `low_authority` visual de-emphasis (smaller text? muted color?)
- Geometry resolution timeline (separate workstream — likely Q3 deferred)
