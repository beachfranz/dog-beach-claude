# Consensus engine — current state (2026-05-16 discovery)

**Purpose:** Discovery output for the source-authority extension
([[consensus-source-authority]] / [[handoff-consensus-source-authority]]).
Maps the existing data flow + identifies where `source_authority`
needs to land.

---

## Data flow (high level)

```
Extraction sources (LLM, URL crawls, PAD-US, etc.)
       │
       ▼
beach_enrichment_provenance (BEP)
   - field_group='dogs', source, claimed_values, confidence,
     is_canonical, source_url
       │
       ▼  [consensus computation — exact path TBD; see "open question 1"]
       ▼
beach_field_consensus
   - per (gold_fid, field_name): winning_value, confidence,
     disagreement, vote_breakdown (jsonb)
       │
       ▼  [promote_canonical_dogs_to_beach_dog_policy()]
       ▼
beach_dog_policy
   - dogs_allowed, leash_policy, off_leash_flag,
     has_on_leash, has_off_leash, dogs_prohibited_start/end,
     source, consensus_confidence, disagreement_flag,
     zone_rules (jsonb)
       │
       ▼  [tg_after_change_dogs_refire_zone_rules trigger AFTER INSERT/UPDATE]
       ▼  [_promote_zone_rules_for_fid()]
       ▼  rebuilds zone_rules from scratch via three injectors:
       ▼     _zr_inject_perimeter        — hardcoded on_leash for parking/restrooms/showers/picnic
       ▼     _zr_inject_sand_from_policy — sand rule from flags + BEP top-confidence quote
       ▼     _zr_inject_sections_from_bep — sub-sections from BEP filtered to source='section_research_v1'
       │
       ▼  [tg_after_change_refresh_scoring_tier trigger AFTER INSERT/UPDATE — same event]
       ▼
beach_dog_policy.scoring_tier (recomputed)
```

## Tables relevant to source-authority

| Table | Role | Has `source_authority`? |
|---|---|---|
| `beach_enrichment_provenance` (BEP) | Raw extraction store; `source` is a text label (e.g. `pad_us_dogs_policy_v1`, `park_url`, `section_research_v1`); `is_canonical` boolean ~33% of rows; `confidence` numeric | No |
| `beach_field_consensus` | Per-field winner; `winning_value` text, `confidence`, `disagreement`, `vote_breakdown` jsonb | No |
| `beach_dog_policy` | Consumer-facing flags + `zone_rules`; `source` text ('auto_promoted_from_consensus' or 'manual_curator') | No |
| `consensus_field_config` | Routing map: `field_group` → `claim_key` (BEP json key) → `field_kind` (target column) | No |
| `dogs_verdict_override` | NOT the curator-override path I'd assumed — it's early-pipeline OSM-keyed triangulation; 24 rows, all `source='auto'` | n/a |

## Key existing protection mechanism

`promote_canonical_dogs_to_beach_dog_policy` has explicit protection:

```sql
protected as (
  select arena_group_id from public.beach_dog_policy
   where source = 'manual_curator'
)
-- excluded from the upsert
```

**Rows with `source = 'manual_curator'` are never overwritten by the
consensus engine.** This is the existing curator-trumps-engine
escape hatch — undocumented but present. Walkthrough-verified
writes can target this mechanism today by setting
`source = 'manual_curator'` on the beach_dog_policy row.

**Caveat / bug:** `_promote_zone_rules_for_fid` does NOT respect
the `manual_curator` source. It rebuilds `zone_rules` from BEP
+ flags regardless. So manual edits to zone_rules content are
overwritten; only the flag-level fields are protected. This is
the Rosie's bug from 2026-05-16.

## Current source landscape (BEP, field_group='dogs', ~5,000 rows)

Distribution by `source`:

| Source | Count | Proposed source_authority tier |
|---|---|---|
| `pad_us_dogs_policy_v1` | 1175 | 3 (agency_admin_policy) |
| `section_research_v1` | 884 | 4 (operator_posted_policy) |
| `old_school_llm` | 744 | 5 (inferred) |
| `operator_pad_us` | 651 | 3 |
| `park_url` | 477 | 3-4 |
| `city_policy` | 424 | 3 |
| `research` | 376 | 4 |
| `operator_city` | 375 | 4 |
| `text_repass_v1` | 171 | 5 |
| `zone_rules_derived_v1` | 155 | 6 (already-derived; feedback loop) |
| `beach_policy_v2_dogs` | 114 | 4 |
| `unified_v1` | 83 | 5 |
| `operator_policy_exceptions_v1` | 23 | 4 |
| `cpad_unit_dogs_policy_v1` | 12 | 3 |
| `json_explode` | 7 | 5 |
| `operator_dogs_policy_v1` | 6 | 4 |
| `manual` | 1 | 3 |
| `manual_curator` | 1 | 2 |

**Critical observation:** zero tier-1 statute-cited rows exist in
BEP today. The corpus is entirely tier-3-or-lower. The walkthroughs
will introduce the first tier-1 evidence the engine has ever seen
— **simplifying migration** because there's no risk of accidentally
demoting existing high-tier data.

## The three zone_rules injectors

| Injector | Sources | Authority-aware? |
|---|---|---|
| `_zr_inject_perimeter` | None — hardcodes `on_leash` for parking_lot/restrooms/showers/picnic_area from `beach_amenities` boolean flags | No |
| `_zr_inject_sand_from_policy` | Reads flags from beach_dog_policy + pulls top-confidence quote from BEP `field_group='dogs'` (any source, single row, `ORDER BY confidence DESC nulls last, updated_at DESC LIMIT 1`) | No |
| `_zr_inject_sections_from_bep` | Filters BEP to `source='section_research_v1'` ONLY; ignores other section-bearing sources | No — and the source filter is a hack that source_authority would replace |

## Trigger fan-out from `beach_dog_policy`

`AFTER INSERT/UPDATE` triggers:
1. `tg_after_change_dogs_refire_zone_rules` → `_promote_zone_rules_for_fid(arena_group_id)`
2. `tg_after_change_refresh_scoring_tier` → `tg_refresh_scoring_tier_on_dog_policy()`

So every `beach_dog_policy` mutation cascades to `zone_rules` rebuild
AND scoring-tier recomputation.

---

## Where source_authority needs to land

Working back from the consumer surface:

1. **`beach_enrichment_provenance.source_authority` (enum)** — the origin
   of all authority assertions. Each extraction row claims a tier.
2. **Whatever populates `beach_field_consensus`** must consume
   source_authority when weighting votes — high-tier rows must
   trump low-tier even when low-tier outnumbers them. *(Open
   question: where is this populator? Likely a `tg_*_consensus`
   function or a scheduled Dagster job that reads BEP and writes
   to beach_field_consensus.)*
3. **`promote_canonical_dogs_to_beach_dog_policy`** could keep its
   current logic if upstream consensus correctly respects authority.
4. **`_zr_inject_sections_from_bep`** should remove the
   `source = 'section_research_v1'` filter and instead order by
   `source_authority DESC, confidence DESC`. Filter-by-one-source
   is a workaround for not having authority ranking.
5. **`_zr_inject_sand_from_policy`** should also order by
   `source_authority DESC, confidence DESC` when picking the
   evidence quote.
6. **`_promote_zone_rules_for_fid`** should respect
   `beach_dog_policy.source = 'manual_curator'` — skip the rebuild
   OR preserve a `zone_rules_manual` overlay.

## Existing levers we don't need to invent

- `source = 'manual_curator'` protection — extend to
  `source = 'statute_verified'` or similar.
- `is_canonical` boolean on BEP — already singles out winning rows
  per beach; source_authority could feed into how this gets set.
- `consensus_field_config` — routing table; small, easy to extend
  with per-field authority rules.

## Open questions surfaced by discovery

1. **What populates `beach_field_consensus`?** The promoter reads
   from it but discovery didn't find the writer. Likely a Dagster
   job; needs locating. The writer is where source_authority
   weighting goes.
2. **`zone_rules_derived_v1` (155 BEP rows) is a feedback loop** —
   BEP rows whose source is "derived from zone_rules." That means
   zone_rules can feed back into BEP, then re-trigger
   consensus, then re-promote, etc. Likely benign but worth
   confirming no infinite-loop risk when source_authority
   inversions happen.
3. **What's the right default tier for backfill?** Proposed in
   the table above; needs confirmation. Conservative: tier 4
   for operator-prose sources; tier 3 for agency-data sources
   (pad_us, cpad, city_policy); tier 5 for LLM-only passes;
   tier 6 for self-derived (zone_rules_derived_v1).
4. **`consensus_field_config` field-scoped authority** — should some
   fields (e.g., `dogs_allowed`) be statute-authoritative while
   others (e.g., `parking_fee`) stay operator-authoritative?
   Probably yes; the routing table is the natural place to encode
   this.

## Validation test case

Rosie's Dog Beach (fid 6411) is the proof point:

- Current BEP for Rosie's `off_leash_exists`: weight 0.475 split
  across `pad_us_dogs_policy_v1` (0.275) and `state_dogs_policy_v1`
  (0.2), both voting "false". Confidence=1.0 because no opposing
  votes.
- After source_authority: a tier-3 `city_policy` row with the
  verbatim Long Beach Parks & Rec page quote
  ("The dog must be under visual and voice control at all times")
  should beat the tier-3 pad_us / state rows on tie-breaker
  (more recent + more specific to the unit).
- Even better: a tier-1 `statute` row citing the underlying Long
  Beach Municipal Code Chapter 6 section should win unambiguously.
- Verbatim quote + source_url should survive the
  `_promote_zone_rules_for_fid` rebuild and appear in
  `zone_rules.regions[0].sections.sand.evidence`.

## Next step

Design the schema migration. Lives in
`docs/consensus_engine_migration_plan.md` (next doc to write).
Proposes:
- `source_authority` enum type
- `ALTER TABLE beach_enrichment_provenance ADD COLUMN source_authority`
- backfill strategy per the table above
- `consensus_field_config` extension for per-field authority rules
- updates to the three injectors + the consensus populator

## Related pins

- [[consensus-source-authority]] — design pin
- [[handoff-consensus-source-authority]] — runbook (note: was
  written assuming a separate pipeline repo; correction:
  dog-beach-claude IS the pipeline repo)
- [[law-as-primary-source-ca]] — strategic context
- [[walkthrough-hbdb]], [[walkthrough-crystal-cove]],
  [[walkthrough-fort-funston]], [[walkthrough-crown-memorial]],
  [[walkthrough-dockweiler]] — walkthrough proof evidence
