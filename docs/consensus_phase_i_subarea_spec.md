# Phase I — sub-area / spatial extension

Spec for the **sub-area / multi-region** extension to the consensus engine.
Sibling of Phase H (temporal). Producer-side gap identified 2026-05-17:
the entity promoter aggregates `beach_policy_source` rows for one beach
into ONE leash_policy answer, losing designated off-leash sub-areas
within otherwise on-leash beaches.

Status: spec draft. Not started.

---

## What broke

After today's Phase B+C trigger landed, the entity-promoted flat columns
for several famous off-leash beaches collapsed to `on_leash` because
their ps rows establish citywide leash law but don't structurally encode
the carve-out zone. Surfaced spot-checks (2026-05-17 post-trigger):

| fid | beach | reality | now (entity_promoted) |
|---:|---|---|---|
| 6202 | Coronado Dog Beach (Sand Pebble area) | OFF-LEASH (designated zone N of Sunset Park) | on_leash |
| 6622 | Carlsbad Lagoon Dog Beach | OFF-LEASH (city-designated dog beach) | on_leash |
| 8452 | Crown Memorial / Crab Cove | mixed (Crab Cove off-leash sub-area) | on_leash |
| 8226 | Ocean Beach SF | off-leash N of Sloat Blvd (GGNRA) | on_leash mixed |
| 6337 | Stinson Beach | mixed (county-park strip off-leash; GGNRA strip on-leash) | on_leash |

The "true" CA `any off-leash sand somewhere` count is meaningfully higher
than today's 28 — likely 35-45 once Phase I lands and surfaces these
sub-areas correctly.

---

## What the consumer already expects

The same shape that Phase H targets (see `consensus_phase_h_temporal_spec.md`):

```jsonc
"zone_rules": {
  "seasons": [{
    "regions": [
      { "name": "Sand Pebble Off-Leash Area",  // ← THIS is what Phase I emits
        "rule": "off_leash",
        "sections": { "sand": { "rule": "off_leash" } } },
      { "name": "Main beach (south of Ocean Blvd)",
        "rule": "on_leash",
        "sections": { "sand": { "rule": "on_leash" } } }
    ]
  }]
}
```

Consumer surface ALREADY:
- Walks `regions[]` and renders one tile-grid per region (`_zrRegions` in beach.html line 1282)
- Uses `region.name` for region card labels (`beach.html:1377-1383`)
- Falls back to `_ruleMapFromZoneRules` (most-restrictive-per-section) for the
  collapsed single-grid display (`beach.html:1257`)
- `_sandOrWaterProhibited` checks every region — a single off-leash region
  un-blocks the whole-beach "no-go" verdict (`beach.html:2259`)

So consumer is ready to render multi-region; producer needs to emit it.

The today-shipped `_zr_inject_from_policy_sources` injector ALREADY uses
`section` values to keep distinct rules in separate keys (proven by
Pebble Beach + Millerton Point's `sand` + `sand_nps_overlay` pattern).
But it folds everything into `regions[0]` — not multi-region.

---

## Producer-side gap

Three layers need attention:

| Layer | Gap | Today's behavior |
|---|---|---|
| `beach_policy_source` schema | No `region_name` column to disambiguate which zone a row applies to | All ps rows for one beach roll into one section under one region |
| `_canonical_dogs_from_policy_sources` (the view feeding the entity promoter) | Aggregates to single dogs_allowed / leash_policy | Picks max-restrictive; loses off-leash sub-area signal |
| `_zr_inject_from_policy_sources` (the zone_rules injector) | Always emits 1 region | Section names like `sand_dog_area` end up as a second SECTION in regions[0], NOT as a second region |

---

## Phase I — proposed work

### I1. Schema extension

Add ONE column to `beach_policy_source`:

```sql
ALTER TABLE public.beach_policy_source
  ADD COLUMN region_name text NULL;
COMMENT ON COLUMN public.beach_policy_source.region_name IS
  'When non-null, places this bps row into a named sub-area / zone within '
  'the beach. Injector emits one regions[] entry per distinct region_name; '
  'rows with NULL region_name fold into the beach''s default region.';
```

The PK (beach_fid, policy_source_id, section) stays — adding region_name
doesn't change the M:M structure. A beach with sub-areas just gets ps
rows with the same `section='sand'` but different `region_name` values.

Examples:
- Coronado Dog Beach: 1 bps row, region_name='Sand Pebble Off-Leash Area', section='sand', rule='off_leash'; another row region_name=NULL, rule='on_leash' for citywide.
- Crab Cove (Crown Memorial): bps row region_name='Crab Cove', rule='off_leash'; bps row region_name=NULL (or 'Main beach'), rule='on_leash'.
- Ocean Beach SF: bps row region_name='North of Sloat Blvd (GGNRA)', rule='off_leash'; bps row region_name='South of Sloat (DPR)', rule='on_leash'.

### I2. Backfill / extraction

Three sources of region data:
1. **Existing prose** — many bps `status_note` and `evidence_verbatim`
   fields name the sub-area ("Sand Pebble area", "north of Sloat", "Crab Cove",
   "designated dog area"). A Sonnet extractor (similar to H2) can lift
   these into `region_name`.
2. **Beach name itself** — when the beach is *named* as a dog beach
   ("Coronado Dog Beach", "Carlsbad Lagoon Dog Beach", "Dog Beach OB"),
   that's an implicit region_name = the beach name + "off-leash zone".
3. **Manual curation** — for the dozen-or-so canonical multi-zone CA
   beaches, hand-set region_name via a one-shot migration.

Recommend a mix: hand-set the 12-15 known cases via migration (fast +
correct), then run H2-style LLM extraction on the long tail as a second
pass.

### I3. Promoter extension

`_canonical_dogs_from_policy_sources` (the view feeding
`promote_entity_dogs_to_beach_dog_policy`) needs to detect multi-region
cases:

```
WHEN a beach has bps rows with ≥2 distinct region_name values AND any of them is off_leash:
  dogs_allowed   = 'mixed'
  leash_policy   = 'mixed'
  has_on_leash   = TRUE  (if any region has on_leash)
  has_off_leash  = TRUE  (if any region has off_leash)
  off_leash_flag = TRUE
```

This is the "Crown Memorial pattern" — multiple authorities / zones, mixed verdict.

### I4. Injector extension

`_zr_inject_from_policy_sources` extends:
- Group bps rows by `region_name` (NULL = default region)
- Emit one `regions[]` entry per distinct region_name
- Each region carries its own `sections{}`, `time_windows[]` (from H1), and `evidence`
- Preserves `region.name` field

### I5. tier-1 list re-derivation

Once I3 lands, the tier-1 (off-leash) count should jump from 28 → ~40-45
as previously-hidden carve-outs surface. Run the same query as the
2026-05-17 list to compare deltas.

### I6. Quality gates

- **Don't shadow** — a beach with NO region_name on any bps row should behave
  exactly as today (single region, single section).
- **Don't fabricate** — region_name only set when prose specifies the sub-area name.
- **Conflict handling** — if two bps rows have the same region_name but different
  rules (one says on_leash, one says off_leash for the same zone), flag for
  human review; don't auto-resolve.

---

## Where it fits

Same options as Phase H:
1. **One-shot migration** for the 12-15 known cases + script run for the long tail
2. **Triggers already exist** — adding region_name doesn't change the trigger fan-out (same INSERT/UPDATE on bps fires the existing promote-cascade)

### Sequencing relative to Phase H

I1 (schema) + I3 (promoter) are independent of Phase H. Could ship
before H2/H3 complete. But the H+I together unlock the full
multi-region × multi-season × time-window shape the consumer already
renders.

---

## Worked example — Coronado Dog Beach (fid 6202)

Current state (post-Phase B+C trigger):
- 1 ps row: Coronado MC Ch 32.08 (citywide leash law)
- bps section='sand', rule='on_leash', region_name=NULL
- bdp: leash_policy=on_leash, has_off_leash=false → consumer renders "leashed beach"

After I1 schema + I2 hand-backfill:
- 2 bps rows:
  - region_name='Sand Pebble Off-Leash Area', section='sand', rule='off_leash'
  - region_name=NULL (or 'Main beach'), section='sand', rule='on_leash'
- I3 promoter detects 2 regions → dogs_allowed='mixed', leash_policy='mixed', has_off_leash=TRUE, has_on_leash=TRUE
- I4 injector emits zone_rules with regions=[{name:'Sand Pebble Off-Leash Area', sections:{sand:{rule:off_leash}}}, {name:'Main beach', sections:{sand:{rule:on_leash}}}]
- Consumer renders 2 region cards: green off-leash card + amber on-leash card

---

## Effort estimate

- I1 schema: ~15 min (one ALTER + comment)
- I2 backfill: ~1 hr (handwritten migration for ~12-15 known beaches; Sonnet extractor for long tail)
- I3 promoter: ~45 min (update `_canonical_dogs_from_policy_sources` view + test)
- I4 injector: ~45 min (extend `_zr_inject_from_policy_sources` to group by region_name)
- I5 tier-1 re-derivation: ~15 min (rerun the count, compare)
- I6 quality gates: ~15 min

**Total: ~3 hours of focused work.**

---

## Known candidate beaches for I2 hand-backfill

| fid | beach | region_name(s) |
|---:|---|---|
| 6202 | Coronado Dog Beach | "Sand Pebble Off-Leash Area" + "Main beach" |
| 6622 | Carlsbad Lagoon Dog Beach | "Designated dog beach" + "Lagoon main" |
| 8452 | Crown Memorial / Crab Cove | "Crab Cove (EBRPD)" + "Main beach (DPR)" |
| 8226 | Ocean Beach SF | "North of Sloat (GGNRA off-leash)" + "South of Sloat (DPR on-leash)" |
| 6337 | Stinson Beach | "County park strip (off-leash)" + "GGNRA strip (on-leash)" |
| 6212 | HBDB / Dog Beach | already manual_curator — no change needed |
| 6411 | Rosie's Dog Beach | "Off-leash zone Granada to Roycroft" + "Surrounding LB beach" |
| 8560 | Del Mar Dog Beach | "North Beach (off-leash strip)" + "Main beach south of 29th" |
| 8635 | East Beach SF | (already off-leash; verify single-zone) |
| 6097 | Fort Funston | (already off-leash; verify single-zone) |
| 9716 | Fiesta Island | "Off-leash dog area" + "Rest of island (on-leash)" |
| 8358 | Dog Beach SD (Ocean Beach) | (already off-leash; verify single-zone) |
| 8968 | Benicia Pier & Beach | verify whether sub-area |

13 confirmed candidates; ~5-8 more likely from extractor pass.

---

## Related

- `docs/consensus_phase_h_temporal_spec.md` — temporal sibling (seasons, day-parts)
- `docs/consensus_engine_current_state.md` — discovery (incl. 2026-05-17 addendum)
- `docs/jurisdiction_policy_source_playbook.md` — upstream / producer playbook
- `beach.html` lines 1257-1450 — consumer multi-region renderer (already ready)
- `mobile-beach.html` same shape
- Phase B+C trigger migration (2026-05-17 commits) — the cascade Phase I plugs into
