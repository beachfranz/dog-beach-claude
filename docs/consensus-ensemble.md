# Consensus Ensemble — best-values-per-field spec

**Status:** Design memo, 2026-05-04. Not yet implemented. Replaces today's
winner-takes-all source picker with a weighted-consensus ensemble that
extracts more value from the data already collected.

**One-line:** Today we extract from 4 URLs × 4 sources per beach, then
select 1/16 of the signal. Consensus uses all of it via weighted voting,
producing better values plus a real confidence + disagreement signal.

---

## The problem

Today's per-(beach, field) selection is winner-takes-all at two layers:

```
Layer 1: populate_from_<source>_gold
  4 URLs × extraction → 4 candidate values per (beach, field)
  Pick: first conclusive value (skip unclear/null), then highest authority,
        then most recent.
  Output: 1 row in beach_enrichment_provenance per (beach, field_group, source).
  Discarded: 3 of 4 URL votes (sometimes 11 of 12 across all fields).

Layer 2: _resolve_field_group_gold
  N source-level claims → 1 canonical
  Pick: lowest source priority number wins.
  Output: 1 canonical row marked is_canonical=true.
  Discarded: All other source families' claims.
```

Net: of ~16 extractions per beach (4 URLs × 4 sources), only 1 value
per field reaches the consumer surface. **Per-field signal compression
ratio ~16:1.**

This is wasteful when sources agree (4 yes-votes treated as 1 vote at
fixed 0.70 confidence) and risky when sources disagree (one wrong outlier
can win if it's at the highest priority).

## The fix: two-layer ensemble

Both layers become **weighted consensus** instead of selectors. The
math at each layer:

### Layer 1 — within-source URL consensus

Replaces "first conclusive" in `populate_from_*_gold` functions.

For each (beach, field) within a single source:

```
candidates = all extractions for this (beach, field, source)

for each candidate value v:
  weighted_votes(v) = Σ (
    is_conclusive(extraction) ?
      authority_score(extraction.url) :
      0
  ) for extraction in candidates where normalize(extraction.parsed_value) == v

winning_value = argmax(weighted_votes)
total_votes  = Σ weighted_votes(v) for all v
agreement    = weighted_votes(winning_value) / total_votes
url_count    = count(candidates where conclusive)
```

Then per-source claim becomes:
```
beach_enrichment_provenance row:
  source            = <source>
  claimed_values    = {<field>: winning_value}
  confidence        = base_confidence(source) × agreement
                       (1.0 unanimous, 0.5 split, 0.0 all-disagree)
  notes             = vote breakdown (e.g., "3/4 URLs agree on yes")
  is_canonical      = false (set by resolver)
```

`base_confidence(source)` = per-field calibration accuracy if known
(research=0.91 for dogs_allowed, park_url=0.86, etc.) else 0.7 default.

### Layer 2 — cross-source claim ensemble

Replaces priority-based winner in `_resolve_field_group_gold`.

For each (beach, field) across all sources that have evidence:

```
sources_with_value = [(source, value, confidence, calibration_weight)]

for each candidate value v:
  weighted_votes(v) = Σ (
    confidence × calibration_weight(source, field)
  ) for (source, value, ...) in sources_with_value where value == v

winning_value = argmax(weighted_votes)
total_votes   = Σ weighted_votes(v) for all v
agreement     = weighted_votes(winning_value) / total_votes
disagreement_flag = (agreement < 0.65) OR
                    (any source with calibration ≥ 0.85 disagrees)
```

`calibration_weight(source, field)` comes from a new `field_source_calibration`
table seeded from existing `gold_*_calibration_binary` views.

Manual source always wins regardless: priority 1 gates ahead of consensus.

Output: a `beach_field_consensus` row per (beach, field) with the winning
value, confidence, disagreement flag, and vote breakdown.

## Schema changes

### New table: `field_source_calibration`

Seeded from `gold_dogs_allowed_calibration_binary` etc. Tunable as
calibration data accumulates.

```sql
create table public.field_source_calibration (
  field_name      text not null,
  source          text not null,         -- 'research', 'park_url', etc.
  calibration_acc numeric not null check (calibration_acc between 0 and 1),
  sample_size     integer,
  computed_at     timestamptz default now(),
  notes           text,
  primary key (field_name, source)
);

-- Seed:
insert into field_source_calibration values
  ('dogs_allowed',  'research',        0.91,  35, '2026-05-03 cross-field calibration'),
  ('dogs_allowed',  'park_url',        0.86,  35, ...),
  ('dogs_allowed',  'old_school_llm',  0.69,  49, ...),
  ('dogs_allowed',  'governing_body',  0.75,  24, ...),
  ('leash_policy',  'research',        0.83,   6, ...),
  ('leash_policy',  'old_school_llm',  0.77,  22, ...),
  ('leash_policy',  'park_url',        0.75,   8, ...),
  ('leash_policy',  'governing_body',  0.50,  10, '50% — coin-flip; demote'),
  ('has_lifeguards','park_url',        0.90,  10, ...),
  ('has_lifeguards','old_school_llm',  0.82,  11, ...);
```

### New table: `beach_field_consensus`

Per-(beach, field) winning value + agreement signal. Replaces the
implicit "canonical row in beach_enrichment_provenance" abstraction
with explicit per-field aggregation.

```sql
create table public.beach_field_consensus (
  gold_fid          bigint not null references beaches_gold(fid),
  field_name        text   not null,
  field_group       text   not null,
  winning_value     text,                  -- normalized canonical value
  confidence        numeric,               -- weighted agreement (0..1)
  disagreement      boolean default false, -- flag for curator review
  vote_breakdown    jsonb,                 -- {value: {sources: [...], weight: 0.x}, ...}
  source_count      integer,               -- how many sources contributed
  url_count         integer,               -- how many URLs total
  computed_at       timestamptz default now(),
  primary key (gold_fid, field_name)
);
```

### Extend `beach_dog_policy` + `beach_amenities`

Add `confidence numeric` + `disagreement_flag boolean` columns so the
auto-promoter can flow consensus signal to the consumer surface.
Nullable for backward compatibility.

## Implementation plan

### Phase A — Layer 1 ensemble (within-source URL consensus)

Rewrite `populate_from_unified_v1_gold` to use weighted URL voting:

```sql
create or replace function public.populate_from_unified_v1_gold(...)
returns ... as $$
  -- For each (beach, field):
  --   1. Gather all extractions
  --   2. Normalize parsed_value via field-specific _norm_* function
  --   3. Compute weighted votes per value (weight = authority_score for conclusive, 0 for unclear)
  --   4. Pick winning value; confidence = winning_weight / total_weight
  --   5. UPSERT beach_enrichment_provenance with normalized value + confidence
  ...
$$;
```

Backward-compatible: still writes to existing `beach_enrichment_provenance`,
just with smarter selection + better-calibrated confidence.

Apply same pattern to other populators: `populate_from_research_gold`,
`populate_from_park_url_gold`, etc. Each gets its own normalization rules.

### Phase B — Layer 2 ensemble (cross-source claim consensus)

New function: `compute_beach_field_consensus(p_fid bigint default null)`.
Reads beach_enrichment_provenance, applies cross-source weighted voting,
writes to `beach_field_consensus`.

The existing `_resolve_field_group_gold` keeps running for backward
compatibility (marks `is_canonical` on provenance rows). The consensus
table is the new authoritative read for the auto-promoter.

### Phase C — Auto-promoter reads consensus

Update `promote_canonical_*_to_*` to read from `beach_field_consensus`
instead of (or alongside) the canonical-marked provenance rows.

Adds `confidence` + `disagreement_flag` columns to consumer tables.

### Phase D — Curator UI surfaces disagreement

`admin/beach-editor-gold.html` shows the vote breakdown for fields with
`disagreement_flag=true`. Curator decides; manual edit always overrides.

## Phased rollout plan

1. **Day 1**: Add `field_source_calibration` table + seed. Add
   `beach_field_consensus` table.
2. **Day 1**: Rewrite `populate_from_unified_v1_gold` with Layer 1
   ensemble (URL-level weighted voting). Validate on probe data —
   compare consensus output to today's "first conclusive" output;
   investigate disagreements.
3. **Day 2**: Build `compute_beach_field_consensus` function (Layer 2).
   Compare its picks to the existing resolver's `is_canonical` picks —
   investigate disagreements.
4. **Day 2-3**: Extend other populators (`populate_from_research_gold`
   etc.) with Layer 1 ensemble. Each gets a per-field normalizer.
5. **Day 3**: Update auto-promoter to read consensus table. Add
   `confidence` + `disagreement_flag` columns to consumer tables.
6. **Day 4**: Curator UI shows disagreements. Soak window.
7. **Day 5**: Once stable, deprecate the old `is_canonical` marker (still
   computed for backward compat but no longer the source of truth).

Total: ~5 days for the full rollout. ~Half day for Layer 1 alone (which
captures ~70% of the value).

## What this gets us

### Today vs. consensus, on a 4-URL beach

```
Field: dogs_allowed
URLs:  parks.ca.gov   → "yes" (auth=4)
       sandiego.gov   → "yes" (auth=4)
       bringfido.com  → "yes" (auth=1)
       random-blog    → "no"  (auth=1)

TODAY:
  populate_from_unified_v1 picks ONE: parks.ca.gov "yes" (auth wins)
  resolver picks unified_v1 canonical (single source for this beach)
  beach_dog_policy.dogs_allowed = "yes"
  confidence = 0.7 (hardcoded)

CONSENSUS:
  Layer 1: votes = {yes: 4+4+1=9, no: 1}; agreement = 9/10 = 0.90
           winning value = "yes", confidence = 0.90
  Layer 2: only unified_v1 has evidence; passes through
           beach_field_consensus.confidence = 0.90, disagreement=false
  beach_dog_policy.dogs_allowed = "yes" (same)
  beach_dog_policy.confidence = 0.90 (NEW — real signal)

Difference: same value, but a 0.90 confidence vs hardcoded 0.70 means
the auto-promoter knows this is a strong consensus, not a guess.
```

### Today vs. consensus, on a disagreement

```
Field: dogs_allowed
Sources:
  research      → "yes" (calibration 0.91)
  park_url      → "no"  (calibration 0.86)
  old_school_llm → "yes" (calibration 0.69)
  governing_body → "yes" (calibration 0.75)

TODAY:
  resolver picks research canonical (priority 3 lowest)
  beach_dog_policy.dogs_allowed = "yes"
  no signal that park_url disagreed

CONSENSUS:
  votes = {yes: 0.91+0.69+0.75=2.35, no: 0.86}
  winning = "yes" with weight 2.35 / 3.21 = 0.73
  3-of-4 sources agree but a high-calibration source disagrees
  → disagreement_flag = TRUE
  beach_dog_policy.dogs_allowed = "yes"
  beach_dog_policy.confidence = 0.73
  beach_dog_policy.disagreement_flag = TRUE
  → surfaces in curator UI for review

Difference: same value, but flagged for human review because a
trustworthy source dissented. Today this is silently picked and
nobody knows.
```

### Today vs. consensus, on a tied disagreement

```
Field: leash_policy
Sources:
  research       → "on_leash" (calibration 0.83)
  old_school_llm → "off_leash" (calibration 0.77)
  park_url       → "on_leash" (calibration 0.75)
  governing_body → "off_leash" (calibration 0.50, coin-flip)

TODAY:
  resolver picks research canonical
  beach_dog_policy.leash_policy = "on_leash"

CONSENSUS:
  votes = {on_leash: 0.83+0.75=1.58, off_leash: 0.77+0.50=1.27}
  winning = "on_leash" with weight 1.58 / 2.85 = 0.55
  → disagreement_flag = TRUE (split)
  → governance body's vote nearly canceled by its low calibration
  beach_dog_policy.leash_policy = "on_leash"
  confidence = 0.55, flag = TRUE

Difference: today silently picks on_leash (research wins). Consensus
arrives at same value but flags it as a real coin-flip. Curator can
investigate.
```

## Open design questions

1. **Per-field calibration weights when sample size is small.** governing_body's
   leash_policy was calibrated at 50% on n=10. Trust it as a coin-flip,
   or downweight further until sample grows?
2. **Within-source URL voting weight.** Should URL authority alone drive
   weight, or should we also factor in extraction model (Sonnet vs Haiku)?
3. **Tie-breaking when consensus splits exactly even.** Today: prefer
   research over park_url over old_school_llm by calibration. Encode that
   explicitly or fall back to source priority?
4. **Manual override semantics.** Today manual_curator wins absolutely.
   In consensus: it's a single high-weight source. Keep "manual = always
   wins" as a hard override, or trust the math?

Lock answers before building.

## Cross-references

- `project_unified_pipeline_spec.md` — Layer D fits this in
- `project_extraction_calibration.md` — source weights come from here
- `project_url_discovery_scope.md` — confidence flow-through (already shipped)
  feeds Layer 1 consensus
- `project_unified_v1_probe_findings.md` — the empirical evidence
  motivating this redesign
