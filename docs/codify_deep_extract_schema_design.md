# Codify Deep-Extract Schema Design

Per Franz 2026-05-20 pilot results. NOT yet implemented; design doc only.

## What we have today

- `policy_source` — codified law / Compendium / operator policy rows. 282 catalog-wide. 248 have `full_text`.
- `beach_policy_source` — many-to-many to fids with `region_name`, `section`, `rule`, `evidence_verbatim`, `operative_status`. ~2,800 rows total but **99% are 1-row-per-source** (no deep extraction).
- `beach_policy_source_temporal` — table exists with rich columns; **almost empty** in practice.
- No CHECK constraints on `rule` or `section` (free text — easy to extend).

## What the pilot produced (11 beaches, 110 rows)

Current pipeline produces ~28 rows for the same 10 beaches. Pilot 4×.

### New `rule` values discovered

Already-canonical (use as-is, expand prompt):
- `on_leash` `off_leash` `off_leash_voice_control` `on_leash_or_voice`
- `not_allowed`

New from pilots (need to formalize):
- `nuisance_restriction` — "no excessive barking / no harassment"
- `waste_pickup_required` — pick-up clauses (replaces drift: waste_disposal_required, waste_pack_out_required, etc.)
- `collar_tag_required` — license / collar / tag clauses
- `feces_removal_required` — older alias of waste_pickup_required; merge
- `local_stricter_rules_authorized` — meta-rule (ORS says cities may impose stricter)
- `no_policy_published` — meta-rule (operator page exists but is silent)

### New `section` values discovered

Already-canonical:
- `sand` `water` `restroom` `parking_lot` `picnic_area`
- `snowy_plover_protection_area`
- `global` (whole-jurisdiction catch-all)

New from pilots:
- `playground` `turf` `walkway` — Powerhouse Park / EBRPD section breakdown
- `swimming_beach` — WAC §352-32-060 carve-out
- `developed_recreation_site` — USFS taxonomy
- `pier` `jetty` `tide_pool` `dune_restoration` — appeared in prompt vocab, expect future hits

## Proposed changes

### 1. Vocabulary management (no DB schema change required)

Free-text rule/section continues. Formalize the canonical list in code:

```python
# scripts/codify_vocab.py
CANONICAL_RULES = {'on_leash', 'off_leash', 'off_leash_voice_control',
                   'on_leash_or_voice', 'not_allowed', 'nuisance_restriction',
                   'waste_pickup_required', 'collar_tag_required',
                   'local_stricter_rules_authorized', 'no_policy_published'}
CANONICAL_SECTIONS = {'sand', 'water', 'playground', 'turf', 'walkway',
                       'restroom', 'parking_lot', 'picnic_area', 'tide_pool',
                       'dune_restoration', 'pier', 'jetty',
                       'developed_recreation_site', 'swimming_beach',
                       'snowy_plover_protection_area', 'global'}
RULE_ALIASES = {  # alias → canonical
    'feces_removal_required':   'waste_pickup_required',
    'waste_disposal_required':  'waste_pickup_required',
    'waste_pack_out_required':  'waste_pickup_required',
    'collar_and_tag_required':  'collar_tag_required',
    'collar_and_tags_required': 'collar_tag_required',
}
```

Use in: writer side (normalize before INSERT), reader side (display alias).

### 2. Temporal storage — use the existing table

`beach_policy_source_temporal` is built for this. Each pilot row with `temporal != null` becomes 1 row here. Mapping:

| pilot field | temporal column | example |
|---|---|---|
| `temporal.season.start`     | `effective_from_md` (MM-DD) | "06-15" |
| `temporal.season.end`       | `effective_to_md`            | "Labor-Day" |
| `temporal.season` w/ anchor | `anchor_start` / `anchor_end` | "day-after-Labor-Day" |
| `temporal.daily.start`      | `daily_start` (TIME or text) | "dawn", "04:00" |
| `temporal.daily.end`        | `daily_end`                  | "08:00", "dusk" |
| `temporal.year_round`       | `window_kind = 'year_round'` | |
| (verbatim)                  | `season_label`               | "Day after Labor Day through June 15" |

One bps row may map to 0 or 1 temporal rows (multiple temporal layers split into separate bps rows — that's what the pilot does already).

### 3. Exemption clauses — new column on bps

Carlsbad's "prohibited except in 7 designated areas" pattern is 1 baseline + 7 exemptions. Today: 7 standalone rows with rule = `not_allowed_exemption`.

Better: add to `beach_policy_source`:

```sql
ALTER TABLE beach_policy_source ADD COLUMN parent_bps_id BIGINT
  REFERENCES beach_policy_source(id) ON DELETE CASCADE;
ALTER TABLE beach_policy_source ADD COLUMN exemption_type TEXT;
-- exemption_type ∈ { 'designated_area', 'service_animal', 'leash_required',
--                    'permit_holder', 'time_window', NULL }
```

Carlsbad pattern becomes:
- 1 row: rule = `not_allowed`, no parent
- 7 rows: rule = `off_leash` / `on_leash` (the actual exemption), parent_bps_id = baseline.id, exemption_type = 'designated_area'

### 4. New-vocab review queue — surface the `_is_new` flag

Pilot already emits `_is_new: true` for non-canonical values. Writer side: when INSERTing, if value not in canonical set:
- Persist anyway (don't block)
- Insert into `vocab_review_queue` for human review
- Or auto-promote if seen ≥ 3 times across distinct sources

```sql
CREATE TABLE vocab_review_queue (
  id BIGSERIAL PRIMARY KEY,
  vocab_type TEXT NOT NULL,  -- 'rule' | 'section'
  value TEXT NOT NULL,
  bps_id BIGINT REFERENCES beach_policy_source(id),
  policy_source_id BIGINT REFERENCES policy_source(id),
  context_snippet TEXT,  -- evidence_verbatim excerpt
  status TEXT DEFAULT 'pending',  -- pending | promoted | aliased | rejected
  resolution TEXT,  -- canonical value to alias to (when status='aliased')
  created_at TIMESTAMPTZ DEFAULT now()
);
```

### 5. Region geometry — punt to follow-up

Pilot writes `region_anchor` (verbatim boundary phrase). For now:
- Persist anchor as new `beach_policy_source.region_anchor TEXT` column
- Display in UI (curator can SEE "north of 29th St" without polygon)
- Geometry resolution = separate workstream (LLM-with-MapBox / manual / OSM landmark match)

Don't block on geometry. Most beaches with multi-region rules already have polygons in `dog_policy_zones` from prior manual curation.

## Migration order

1. `ALTER beach_policy_source ADD region_anchor TEXT;`
2. `ALTER beach_policy_source ADD parent_bps_id BIGINT REFERENCES beach_policy_source(id);`
3. `ALTER beach_policy_source ADD exemption_type TEXT;`
4. `CREATE TABLE vocab_review_queue (...);`
5. Add `scripts/codify_vocab.py` with canonical sets + aliases
6. Write `scripts/extract_and_load_deep.py` — wraps pilot script, NORMALIZES vocab via aliases, INSERTs to bps + bps_temporal, emits to vocab_review_queue when `_is_new`
7. Dry-run on 5 beaches → human spot-check → bulk run
8. Update `_zr_inject_from_policy_sources` to use exemption hierarchy (baseline preserved, exemptions layered)

## Estimated effort

- Schema migrations: ~30 min (small ALTERs)
- vocab + aliases module: ~1 hr
- extract_and_load_deep.py (wrapper around pilot script): ~2 hr
- _zr_inject_from_policy_sources update: ~2 hr (test fixture-by-fixture)
- Dry-run + spot-check + bulk: ~half day (Sonnet runs are minutes; human review is the bottleneck)
- Cost: ~$20 catalog-wide (Sonnet)

Total: ~1 day eng + Sonnet costs.

## Open questions

- **Auto-promotion threshold** for `_is_new` vocab → canonical (3 cross-source hits? 5? curator-only?)
- **Existing rows backfill** — re-extract all 282 policy_source rows once, or only NEW codify going forward?
- **Authority weighting** — pilot output is flat. Consumer surface needs ordering. Use `policy_source.subtype` → tier mapping (federal > state > local; statute > reg > admin > operator > attestation).
- **Cross-source dedup** — Pilot emits per-source duplicates intentionally. Consensus engine (`beach_policy_consensus`) exists but is sparsely populated. New deep-extract output will exacerbate cross-source noise — needs consensus logic before consumer surface trusts it.
