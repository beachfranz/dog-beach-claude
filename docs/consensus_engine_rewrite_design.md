# Consensus engine rewrite — entity-based design

**Purpose:** Design doc for the consensus-engine rewrite informed
by the 2026-05-16 audit ([[consensus_engine_audit_2026_05_16]])
and aligned with the entity model from [[law-as-primary-source-ca]]
and [[entity-modeling]].

**Status:** Design in progress 2026-05-16. Not yet implemented.

**Scope (what's in):**
- Replace the BEP → field_consensus → promote chain with an
  entity-aware resolution model
- Add `policy_source` entity as a first-class consensus subject
- Make extractions evidence-FOR a policy_source rather than
  free-floating claims
- Resolution: highest-authority policy_source wins per
  (beach, section); confidence is for "did we extract this
  correctly" not for "is this rule right"
- Extend the `manual_curator` protection to cover `zone_rules`
  rebuilds

**Scope (what's out — per [[no-silent-narrowing]]):**
- Refactoring extraction pipelines themselves (they emit
  what they emit; we're rewriting the consumer of their output)
- Touching the scoring tier recompute logic
- Other consensus engines (e.g., the amenities consensus); only
  the `field_group = 'dogs'` chain is in scope

---

## The reframing — entities, not vote-counted rows

Today: extractions are free-floating CLAIMS in BEP. The engine
counts/weighs them to pick a winner per field. The math breaks
because identical extractions from one source masquerade as
multiple independent measurements.

Tomorrow: extractions are EVIDENCE-FOR a `policy_source` entity.
Authority is a property of the policy_source (statute vs MOU vs
operator page vs inferred), not a property of the extraction.
Multiple extractions of the same policy_source confirm or refute
the policy_source's content but don't multiply its weight.

The shift in mental model is:

| Today | Tomorrow |
|---|---|
| "What does the BEP majority vote say?" | "What's the highest-authority policy_source that speaks to this section, and what does it say?" |
| Confidence: how sure are we this is the right answer (mixed with vote weight) | Confidence: how sure are we we read this source correctly. Authority: separate, intrinsic to the source. |
| 5 extractions = 5 votes | 5 extractions = 5 reads of the same source; one source's claim |
| Inference can outvote statute if confident enough | Inference can never outvote statute; lower tier only when higher tier is silent |

Aligns with [[entity-modeling]]: subjects (policy_source) get
entity rows; measurements (extractions) point at entities and
age out; derivations (zone_rules) recompute from current state.

---

## Schema — additions and rewrites

### NEW: `policy_source` entity table (already designed in [[law-as-primary-source-ca]])

```sql
CREATE TYPE policy_source_subtype AS ENUM (
  'statute', 'federal_regulation', 'state_regulation',
  'state_statute', 'municipal_code', 'special_district_ordinance',
  'mou', 'lease_agreement', 'operating_agreement', 'concession_lease',
  'agency_administrative_policy', 'superintendents_compendium',
  'court_ruling', 'withdrawn_rulemaking', 'tribal_resolution',
  'operator_posted_policy', 'promotional_listing',
  'community_attestation', 'news_article',
  'inferred', 'unknown'
);

CREATE TABLE policy_source (
  id BIGSERIAL PRIMARY KEY,
  subtype policy_source_subtype NOT NULL,
  citation TEXT NOT NULL,             -- "HBMC §13.08.070", "36 CFR §2.15"
  issuing_agency_id BIGINT,            -- FK to agency entity when ready
  scope TEXT[],                        -- ["dog_policy", "operations"] etc.
  source_url TEXT,
  full_text TEXT,                      -- verbatim when available
  effective_date DATE,
  last_verified TIMESTAMPTZ,
  parent_citation_id BIGINT REFERENCES policy_source(id),  -- §7.97(d) → §2.15
  withdrawn_at TIMESTAMPTZ,            -- nullable; non-null for withdrawn_rulemaking
  metadata JSONB                       -- subtype-specific fields
);

-- Authority tier is intrinsic to subtype; compute, don't store.
CREATE FUNCTION policy_source_authority(s policy_source_subtype) RETURNS int AS $$
  SELECT CASE s
    WHEN 'statute' THEN 1
    WHEN 'federal_regulation' THEN 1
    WHEN 'state_regulation' THEN 1
    WHEN 'state_statute' THEN 1
    WHEN 'municipal_code' THEN 1
    WHEN 'special_district_ordinance' THEN 1
    WHEN 'mou' THEN 2
    WHEN 'lease_agreement' THEN 2
    WHEN 'operating_agreement' THEN 2
    WHEN 'concession_lease' THEN 2
    WHEN 'agency_administrative_policy' THEN 3
    WHEN 'superintendents_compendium' THEN 3
    WHEN 'court_ruling' THEN 3
    WHEN 'tribal_resolution' THEN 3
    WHEN 'operator_posted_policy' THEN 4
    WHEN 'withdrawn_rulemaking' THEN 5
    WHEN 'promotional_listing' THEN 5
    WHEN 'community_attestation' THEN 5
    WHEN 'news_article' THEN 5
    WHEN 'inferred' THEN 6
    ELSE 7
  END
$$ LANGUAGE SQL IMMUTABLE;
```

### CHANGED: `beach_enrichment_provenance` becomes evidence-for-source

Add a `policy_source_id` FK so each extraction row identifies the
policy_source it claims to have read. Keep the existing columns
(source, confidence, claimed_values) so the migration can happen
incrementally.

```sql
ALTER TABLE beach_enrichment_provenance
  ADD COLUMN policy_source_id BIGINT REFERENCES policy_source(id),
  ADD COLUMN relevance_verified BOOLEAN DEFAULT FALSE;  -- did the URL substantiate the claim?
```

Existing rows have `policy_source_id = NULL`. The rewrite consumes
those rows in fallback mode (tier 4 default = operator_posted_policy).

### NEW: `beach_policy_source` (m:m — which sources speak to which beach × section)

```sql
CREATE TABLE beach_policy_source (
  beach_fid BIGINT NOT NULL,
  policy_source_id BIGINT NOT NULL REFERENCES policy_source(id),
  section TEXT NOT NULL,              -- 'sand', 'parking_lot', 'tide_pools', etc.
  rule TEXT NOT NULL,                 -- 'off_leash', 'on_leash', 'not_allowed', 'off_leash_voice_control'
  rule_modifier JSONB,                -- temporal (seasonal closures), spatial (carve-outs), eligibility (one dog per adult)
  evidence_verbatim TEXT,             -- the quote that supports this rule
  evidence_url TEXT,
  extracted_at TIMESTAMPTZ,
  last_verified TIMESTAMPTZ,
  PRIMARY KEY (beach_fid, policy_source_id, section)
);
```

**This replaces the `regions[].sections{}` JSONB structure in
`zone_rules` as the source-of-truth.** zone_rules becomes a
derived view (per [[entity-modeling]] principle — derivations
stay views).

### CHANGED: `beach_dog_policy` loses some columns, keeps user-facing rollup

`zone_rules` JSONB stays for backward-compat with the consumer
surface, but it's derived from `beach_policy_source` rows at
rebuild time. The boolean flags (`has_on_leash`, `has_off_leash`,
`dogs_allowed`, etc.) also become derived.

The `source = 'manual_curator'` protection STAYS and is the
intended bypass for high-authority writes that haven't been
modeled as `policy_source` entities yet. **Adds: protection now
extends to `zone_rules` JSONB content** (the Rosie's-bug fix).

---

## Resolution algorithm — entity-aware

For each `(beach_fid, section)`:

```
1. Pull all beach_policy_source rows for this (beach, section).
2. For each, look up policy_source.subtype → authority tier.
3. Filter to highest-authority tier present.
4. If 1 row: that's the answer.
5. If multiple rows in highest tier:
     a. Prefer most recent effective_date.
     b. Prefer most recent last_verified.
     c. Surface as "rule_conflict" if still ambiguous —
        UI can render both with provenance.
6. If 0 rows: fall through to lower tiers; if all empty,
   return 'inferred' with no evidence.
```

**No vote weighting. No multiplication of confidence by source
count.** Each policy_source's claim either applies or doesn't;
authority resolves conflicts.

**Where multi-extraction confidence DOES matter:** when reading
ONE policy_source's content. If five LLM passes all extracted
"§13.08.070 says leashed" from the same PDF, our confidence that
WE READ THE SOURCE CORRECTLY is high. That's a quality signal on
the extraction, not on the rule.

---

## The trigger rebuild — `_promote_zone_rules_for_fid` v2

```
1. If beach_dog_policy.source = 'manual_curator' AND zone_rules
   has non-null content, skip rebuild (preserve human edit).
2. Otherwise, rebuild zone_rules from:
   a. For each section in the section vocabulary:
      - resolve rule via algorithm above
      - emit rule + evidence_verbatim + evidence_url +
        policy_source.citation
   b. Run perimeter injector for sections that have no
      beach_policy_source rows (parking_lot, restrooms, showers,
      picnic_area still get hardcoded on_leash defaults at
      tier 6).
   c. Compute boolean rollups (has_on_leash, has_off_leash,
      dogs_allowed) from the section-level rules.
3. Write zone_rules + boolean rollups in a single UPDATE.
```

**The injector pattern stays** (it's a good substrate); each
injector becomes authority-aware. The "filter by source name"
hack in `_zr_inject_sections_from_bep` disappears — the section
walker reads `beach_policy_source` ordered by authority.

---

## The relevance gate

The audit found 61% of BEP dog rows have no verbatim evidence;
22% of canonical rows have none. The rewrite treats this as an
input-quality gate, not a downstream filter:

- **Extraction rows without verbatim quote** are marked
  `relevance_verified = FALSE` and are TIER 6 (inferred) by
  default regardless of their source label.
- **Extraction rows whose URL is cited for >50 distinct beaches**
  (the "generic state-level page" pattern) are marked
  `relevance_verified = FALSE` until manually downgraded or
  per-beach-verified.
- **Walkthrough-verified rows** (where a human has read the
  source and confirmed the claim) are marked
  `relevance_verified = TRUE` and the policy_source carries the
  intrinsic authority tier.

---

## Migration path

This is a real schema change with real backfill work. Stage:

**Phase 1 — Add structure, no behavior change (one day):**
- Create `policy_source` table + auth function
- Add `policy_source_id` + `relevance_verified` to BEP
- Create `beach_policy_source` table
- All new tables empty; existing engine continues to work

**Phase 2 — Seed policy_source from walkthroughs (one day):**
- Insert ~30-50 policy_source rows from the six walkthrough docs
  (HBMC §13.08.070; 36 CFR §2.15, §7.97; 1979 GGNRA Pet Policy;
  2025 GOGA Compendium; parks.ca.gov/Dogs catalog; etc.)
- Insert `beach_policy_source` rows for the six walkthrough
  beaches
- Existing zone_rules unchanged; new entity rows exist alongside

**Phase 3 — Switch the rebuild trigger (one day):**
- Replace `_promote_zone_rules_for_fid` with v2 (entity-aware)
- For beaches with `beach_policy_source` rows, the new resolver
  runs
- For beaches without any new rows (most beaches), the rebuild
  falls back to the old BEP-and-flags path (compatibility)
- Validate against the six walkthrough beaches

**Phase 4 — Backfill BEP→policy_source mapping (1-2 weeks):**
- For each existing BEP row, infer policy_source: probably tier 4
  default for operator-prose extractions; tier 3 for `pad_us_*`
  and `cpad_*`; tier 5 for `inferred` / `derived_*`
- Set `relevance_verified` based on presence of verbatim quote
  and URL-fanout audit
- Once all beaches have entity-aware coverage, retire the
  fallback path

**Phase 5 — Retire the vote-weighting math:**
- Drop `beach_field_consensus` (or repurpose for audit-trail only)
- Simplify `promote_canonical_dogs_to_beach_dog_policy` to read
  directly from `beach_policy_source`
- `vote_breakdown` JSONs become read-only history

---

## Validation cases

Six walkthroughs provide the regression suite. Each beach exercises
a specific resolution pattern:

| Beach | Tests |
|---|---|
| **HBDB** (fid 6212) | Three-source layered stack (statute leashed; MOU permissive; operator "leash optional"). Tier-1 statute wins as canonical. Tiers 2+3 preserved as supplementary evidence. |
| **Crystal Cove** (fid 8330) | 2-layer parent_citation (DPR-wide → park-unit). Park-unit override of campground prohibition vs DPR baseline. |
| **Fort Funston** (fid 6097) | 4-layer parent_citation (§2.15 → §7.97(d) → 1979 Pet Policy → 2025 Compendium). Voice-control rule value. Withdrawn-rulemaking handling (the 2016 NPRM has tier 5 and shouldn't affect resolution). |
| **Crown Memorial** (fid 8452) | Cross-entity operator (DPR-owned, EBRPD-operated). DPR's policy governs; EBRPD's Ordinance 38 doesn't apply on DPR land — tests geography-scoped applicability. CDFW MPA sub-area at Crab Cove. |
| **Dockweiler** (fid 8477) | Three-party lease chain (State → City of LA → LA County). Operator silent on dog policy; DPR catalog row alone is operative. |
| **Rosie's** (fid 6411) | THE PROOF POINT. Pre-rewrite: `off_leash_exists=false @ conf 1.0` (wrong). Post-rewrite: city operator-published policy at tier 3-4 says off-leash voice-control; engine returns true with verbatim quote. |

**Each walkthrough produces a regression test:** SQL snapshot
of (beach_fid → expected zone_rules JSON) that the new
resolver must match within tolerance.

---

## Open questions

1. **`is_canonical` becomes redundant?** The BEP boolean was used
   for picking single-winner-per-field; entity-aware resolution
   doesn't need a per-row winner marker (the winner is the
   policy_source). Drop or repurpose?
2. **Field-scoped authority** — `consensus_field_config` was
   pointing at this. Some fields (`dogs_allowed`) need statute
   authority; others (`parking_fee`) live forever in operator
   posted info. Per-field authority floors might still be useful.
3. **Cross-state generalization** — the 6-tier hierarchy is
   CA-shaped (because CA has the most CA-specific statutes).
   Other states have different code-platform conventions;
   subtype list may need extension when expanding.
4. **Tribal authority and sovereign jurisdictions** —
   `tribal_resolution` is in the subtype list but not yet
   exercised. Tribal tier placement TBD when first tribal
   beach is walked.
5. **Performance** — the per-section authority walk runs every
   time zone_rules rebuilds (per trigger). For ~4,000 beaches ×
   ~8 sections that's 32k walks. Should be fast (indexed FK
   lookups) but worth profiling before Phase 3 rollout.

---

## Effort estimate (revised)

- **Phase 1 (schema add):** 1 day
- **Phase 2 (seed walkthroughs):** 1 day
- **Phase 3 (trigger swap + validation):** 2-3 days including
  testing against the six walkthroughs
- **Phase 4 (backfill):** 1-2 weeks of part-time work — depends
  on confidence in default tier assignment and how aggressively
  we re-extract for relevance verification
- **Phase 5 (retire vote-weighting):** 2-3 days

**Total: ~3 weeks part-time, or 1.5 weeks full-time focused.**

Walkthroughs stay paused throughout. Re-resume when Phase 3 lands;
Phase 4 can run concurrently with new walkthrough work.

---

## Related pins

- [[law-as-primary-source-ca]] — strategy this serves
- [[entity-modeling]] — modeling principle
- [[consensus-source-authority]] — original (too-narrow) design;
  this doc supersedes
- [[handoff-consensus-source-authority]] — original handoff; also
  superseded by this
- [[consensus_engine_audit_2026_05_16]] — empirical evidence
- All six walkthrough pins — regression test cases
- [[no-silent-narrowing]] — scope discipline
- [[trust-working-pipeline]] — don't expand to other engines
