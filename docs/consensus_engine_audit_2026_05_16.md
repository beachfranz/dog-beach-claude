# Consensus engine — diagnostic audit (2026-05-16)

**Purpose:** Quantify Franz's four concerns about the dog-policy
consensus engine before designing the source-authority extension.
Each concern was checked with a SQL audit; numbers below.

**Verdict:** **All four concerns are confirmed; one is
catastrophic.** The engine produces high-confidence wrong answers
~42% of the time on dog-policy fields, and "confidence" is mostly
a per-source constant rather than a per-extraction measurement.
This is a deeper problem than source-authority alone solves.

---

## Concern 1: Low-confidence sources sum to high confidence

**Method:** For each `beach_field_consensus` row, compare its
`confidence` to the MAXIMUM `src_conf` of any individual voter in
its `vote_breakdown`. If consensus_conf > max_voter_conf, the
engine has manufactured certainty from weaker inputs.

**Result (18,890 dogs-consensus rows):**

| Metric | Count | % |
|---|---|---|
| Consensus confidence > max individual voter | **17,068** | 90.4% |
| Materially inflated (consensus_conf ≥ max_voter + 0.1) | **14,425** | 76.4% |
| High-confidence consensus (≥0.8) backed by ALL WEAK votes (≤0.5) | **8,032** | 42.5% |
| Average inflation when present | +0.405 | |

**Verdict:** ✅ confirmed and catastrophic. **42% of consensus
rows assert high confidence on weak evidence alone.** This is a
statistical-math bug, not a missing-feature bug.

---

## Concern 2: Irrelevant links / sources without supporting evidence

**Method:** Count BEP rows missing `source_url` or missing any
verbatim evidence (`source_quote` or `evidence` in
`claimed_values`). Then look for `source_url` re-use across many
beaches (copy-paste smell — generic state-level page cited as
specific evidence for hundreds of unique beaches).

**Result:**

| Metric | Count | % of 12,033 |
|---|---|---|
| Total BEP dog rows | 12,033 | 100% |
| No `source_url` at all | 1,371 | 11.4% |
| No verbatim evidence (no quote / no evidence object) | **7,357** | 61.1% |
| **Canonical** rows with no verbatim evidence | **2,699** | (22.4% of canonical) |
| **Canonical** rows with no source URL | 390 | (3.2% of canonical) |

**Top single-URL fan-out (one URL cited as evidence across many beaches):**

| Source URL | Distinct beaches cited |
|---|---|
| `parks.ca.gov/?page_id=23973` (generic CA state-parks dogs page) | **751** |
| `michigan.gov/.../parks-trails-pets` | 680 |
| `parks.wa.gov/about/policies-rules/pets` | 429 |
| Sandy Neck Park policy PDF (Barnstable, MA) | 389 (1,165 rows) |
| `michigan.gov/.../state-parks/pet-friendly` | 183 |
| `dnr.maryland.gov/.../pets.aspx` | 155 |
| `oregon.gov/.../Pets-Beaches.aspx` | 152 |

**Verdict:** ✅ confirmed. The extraction layer does not gate on
relevance — generic state-level dog-policy pages are being cited
as specific evidence for hundreds of unique beaches as if the page
actually said something specific about each beach. 61% of all
dog-rows carry no verbatim quote at all. 22% of the canonical
(picked-winner) rows carry no verbatim evidence.

---

## Concern 3: Sources feeding back into prior sources

**Method:** Count BEP rows whose `source` name suggests
derived-from-prior-output (`*derived*`, `*_v2*`, `*repass*`,
`*unified*`, `*json_explode*`). Check whether they have canonical
status (meaning they influence the picked winner).

**Result:**

| Source | Rows | Canonical | Beaches |
|---|---|---|---|
| `beach_policy_v2_dogs` | 114 | **28 canonical** | 114 |
| `unified_v1` | 83 | **21 canonical** | 83 |
| `text_repass_v1` | 171 | **10 canonical** | 171 |
| `zone_rules_derived_v1` | 155 | 0 canonical | 155 |
| `json_explode` | 7 | 0 canonical | 7 |
| Total | **530** | **59** | |

**Verdict:** ✅ confirmed but moderate. **59 canonical rows
across 530 derived-source rows** — derived sources are
influencing winner picks for 59 beaches. The `zone_rules_derived_v1`
feedback loop (155 rows) doesn't directly produce canonical rows,
but these rows still feed into `vote_breakdown` weighting and
inflate consensus on the canonical-from-elsewhere rows. Not the
biggest fire; should be cleaned up during the rewrite.

---

## Concern 4: Data destruction (manual edits reverted)

**Method:** Count `beach_dog_policy.source` values; count
disagreement-flagged rows; count `gold_evidence_audit` rows.

**Result:**

| Field | Count | Note |
|---|---|---|
| Total `beach_dog_policy` rows | 3,963 | |
| `source = 'auto_promoted_from_consensus'` | 3,896 (98.3%) | |
| `source = 'public.beaches'` | 62 | legacy |
| `source = 'manual_curator'` | **5** (0.1%) | the protection mechanism exists but is barely used |
| Rows with `disagreement_flag = true` | **970** (24.5%) | engine itself flags ~1 in 4 outputs as conflict-laden |
| `gold_evidence_audit` rows | 35,808 across 3,904 beaches | usable audit trail for revert investigation |

**Verdict:** ✅ partial. Direct evidence of revert events would
require diffing audit-trail history (separate query). What's
clear: (a) the curator-protection mechanism is essentially
unused, so manual edits are at risk by default;
(b) 24.5% of all output rows carry a known-disagreement flag that
isn't surfaced anywhere consumer-facing; (c) the Rosie's incident
2026-05-16 is one documented revert.

---

## Concern 5 (new — surfaced by audit): Confidence is a per-source CONSTANT, not a measurement

**Method:** For each `source` in BEP, count the number of distinct
`confidence` values.

**Result (top sources, field_group='dogs'):**

| Source | Unique conf values | Total rows | Range |
|---|---|---|---|
| `section_research_v1` | **1** (constant) | 884 | (single value) |
| `operator_pad_us` | **1** | 651 | 0.70 flat |
| `research` | **1** | 376 | 0.85 flat |
| `operator_city` | **1** | 375 | 0.50 flat |
| `old_school_llm` | 3 | 744 | 0.50–0.80 |
| `beach_policy_v2_dogs` | 4 | 114 | 0.40–0.90 |
| `park_url` | 16 | 477 | 0.15–0.95 |
| `text_repass_v1` | 18 | 171 | 0.00–0.85 |
| `city_policy` | 37 | 424 | 0.00–1.00 |

**Verdict:** ✅ confirmed and structural. Four major sources
(2,286 rows / ~19% of all dog BEP) emit the SAME `confidence`
value on every row — meaning their confidence isn't a measurement
of extraction quality but a source-level prior baked in at
emission time. The vote-weighting engine then takes this constant
as if it were a per-row quality score and combines multiple
copies as if they were independent measurements.

**This is the root cause of Concern 1.** "Five sources at 0.70"
isn't five independent assessments — it's the same 0.70 echoed
five times because the emitter doesn't compute per-row quality.

---

## The Rosie's spot-check (the smoking gun)

`gold_fid = 6411` — Rosie's Dog Beach, the most famous off-leash
beach in LA County. Per the consensus engine:

| Field | Winning value | Consensus confidence | Max individual voter conf |
|---|---|---|---|
| `allowed` | yes | 0.850 | 0.950 |
| `has_off_leash` | **false** | 0.500 | 0.950 |
| `has_on_leash` | true | 1.000 | 0.700 |
| `leash_required` | on_leash | 0.500 | 0.950 |
| `off_leash_exists` | **false** | **1.000** | **0.550** |

**The engine declared `off_leash_exists = false` at consensus
confidence 1.000 — based on a single source with 0.55 confidence.**
That's not "consensus uncertain"; that's "manufactured certainty
from weak input." And this is the operative output that drove the
user-facing `has_off_leash = false` on Rosie's, which was wrong,
which got caught by the walkthrough only because Franz happened to
pick it as a test beach.

If this exact pattern is happening on 8,000+ other beaches (the
high-conf-from-weak-votes count from Concern 1), the data
quality across the consumer surface is much worse than current
disagreement-flag tracking shows.

---

## What this means for the engine design

The original [[consensus-source-authority]] design proposed adding
a `source_authority` enum that statute outranks operator. That's
necessary but **insufficient** for what the audit revealed:

1. **Concern 1 (manufactured high confidence) is the biggest
   issue.** Source_authority alone won't fix it because the vote
   weighting math is broken at the math level — adding a tier
   field but keeping the weighted-sum-of-constants approach
   leaves the same kind of inflation.
2. **Concern 5 (confidence-as-constant) is upstream.** The
   extraction emitters need per-row quality signals or to stop
   pretending to emit confidence at all. A single tier per
   source (which is what source_authority effectively is) is more
   honest than fake per-row confidence — but the architecture
   needs to acknowledge that's what it is.
3. **Concern 2 (relevance gating) needs a verification gate.**
   An extraction claim without a verbatim quote shouldn't carry
   evidence weight. The 61% of rows with no quote should either
   be dropped or marked tier 6 (inferred).
4. **Concern 3 (feedback loops) is a labeling problem.** Derived
   sources need to be tagged as derived so they can't be weighted
   alongside primary sources.
5. **Concern 4 (data destruction) needs the `manual_curator`
   protection extended to `zone_rules` rebuild and made the
   default for walkthrough-verified data.**

**Implication for full-B scope:** the rewrite needs to address
the math + the input shape + the relevance gate + the labeling +
the protection mechanism. That's a real engine rewrite, not a
column addition. **Probably 2 weeks of work, not 1 day.** Worth
doing because:

- The current engine produces high-confidence wrong answers at
  scale (8,000+ rows by audit count).
- The fixes are coherent: source_authority + better confidence
  semantics + relevance gating + protection are one unified
  architecture, not five separate features.
- Continuing to ship walkthroughs (or any new evidence) into the
  current engine just adds to the noise floor.

---

## Recommended next-step shape

1. **Pin this audit as a memory pin** so future sessions inherit
   the empirical baseline.
2. **Decide whether full-B is now actually a-rewrite-but-without-
   panicking.** Worth Franz's explicit confirmation before
   spending the next two weeks on it. The original
   source-authority extension scope was 1-2 days; the audit
   surfaced enough that we should re-scope honestly.
3. **Design the rewrite** in a new doc (`docs/consensus_engine_
   rewrite_design.md`) building on this audit. Should land in a
   pause-walkthroughs-while-rewriting posture or a parallel-
   track-with-feature-flag posture — both have tradeoffs.
4. **Walkthroughs stay paused** during the rewrite (per Franz's
   2026-05-16 decision).

## Related pins

- [[consensus-source-authority]] — original design pin (needs
  re-scoping based on this audit)
- [[handoff-consensus-source-authority]] — original handoff
  (also needs re-scoping; assumed source-authority alone was
  the fix)
- [[law-as-primary-source-ca]] — the strategic initiative this
  serves
- All six walkthrough pins — provided the verified evidence that
  surfaced the problem (and will provide the test cases for the
  rewrite validation)
