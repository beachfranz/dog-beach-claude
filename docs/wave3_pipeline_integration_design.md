# Wave 3 → Pipeline Integration — design

**Date:** 2026-05-16 (drafted same evening as Wave 3 completion).
**Status:** Design draft. Builds on [[consensus_engine_rewrite_design]] and [[consensus_engine_current_state]]. Not yet implemented.

**Purpose:** Define how the statute-backed dog-policy infrastructure created by Wave 1–3 (agency / policy_source / beach_policy_source / operator / beach_operator tables, ~430+ beach links, ~162 CA operators) integrates into the existing 10-phase pipeline ([[pipeline-overview]]) without breaking the BEP-based extraction-and-consensus path that 95% of beaches still depend on.

---

## What Wave 1–3 produced

Five new entity tables populated, mostly for CA:

| Table | Rows (post-apply) | Role |
|---|---:|---|
| `agency` | ~196 | Statute-issuing entities (cities, counties, state depts, federal, tribal, special districts). Each policy_source has `issuing_agency_id` FK to this. |
| `policy_source` | ~140+ | First-class entities for the laws, MOUs, lease agreements, admin policies, and operator-posted rules that govern beaches. Has authority tier intrinsic to subtype. |
| `beach_policy_source` | ~430 | M:M — which sources speak to which (beach, section) with rule + verbatim + URL. |
| `operator` (new) | ~162 | Curated operators with `agency_id` parent + `delegated_authority_via` FK to MOU/lease. Replaces legacy `operators` for consensus-engine purposes. |
| `beach_operator` | 3 (will grow) | M:M — which operator runs which beach with precedence rank. |

Plus the consensus engine rewrite design ([[consensus_engine_rewrite_design]]) already sketches a Phase-1–5 schema migration that *consumes* these tables. **This doc is about what the pipeline does with that consumption end-to-end.**

---

## Current pipeline (the 10 phases, dog-policy lens)

Per [[pipeline-overview]]:

```
Phase 0 — External ingestion (PAD-US, OSM, CCC, TIGER, NOAA)
Phase 1 — Landing-stage attribution (county/cpad/place PIP on landings)
Phase 2 — Arena clustering (dedup landings)
Phase 3 — Promotion to beaches_gold + governance resolver
Phase 4 — BEP cascade + resolvers       ← dog_policy lives here today
Phase 5 — Promotion to beach_dog_policy ← consensus + zone_rules built here
Phase 6 — Operator-level LLM enrichment (Tavily + Sonnet)
Phase 7 — Descriptions + photos
Phase 8 — Daily scoring
Phase 9 — Edge Function consumer feeds
Phase 10 — Orchestrators (run_state_pipeline, daily-refresh)
```

Dog-policy data flows:

```
LLM extractors / PAD-US / CPAD / inferred
        ↓
BEP rows (~12k for dogs domain; ~1900 distinct beaches)
        ↓ (consensus computation, vote-weighted)
beach_field_consensus (winning_value per (fid, field))
        ↓ (promote_canonical_dogs_to_beach_dog_policy)
beach_dog_policy (zone_rules JSONB; boolean flags; consensus_confidence)
        ↓ (trigger _promote_zone_rules_for_fid)
zone_rules rebuilt from injectors
        ↓ (trigger refresh_scoring_tier)
scoring_tier recomputed
```

**The new infrastructure is built but unwired.** `beach_policy_source` exists alongside but is invisible to `promote_canonical_dogs_to_beach_dog_policy`. The consensus engine doesn't know to look at it.

---

## Question 1 — Consensus engine input *(highest leverage)*

**Question:** Should the new `beach_policy_source` rows enter the consensus engine as additional evidence (blended with BEP), or as **authoritative facts that supersede BEP entirely for those beaches**?

**Options:**

**(1a) Blend.** Promoter reads both BEP and `beach_policy_source`. Vote-weighting math is extended to include policy_source authority tier as a weight. BEP from `pad_us_dogs_policy_v1` (currently ~confidence 0.6) competes with `beach_policy_source` rows from `municipal_code` (tier 1 → weight 1.0).

- **Pro:** No data is thrown away; LLM extractions still add evidence on top of statute.
- **Con:** Vote-counting still happens. The audit found that BEP rows with no verbatim and generic state URLs were overweighted — blending doesn't fix that. A single LLM hallucination can outvote one statute citation if extractor confidences are >> tier weight.

**(1b) Supersede.** For any (beach_fid, section) with at least one `beach_policy_source` row at authority tier ≤ 3 (statute, MOU, admin policy), ignore BEP entirely. Engine returns the highest-tier `beach_policy_source` row's rule, period.

- **Pro:** Aligns with the consensus_engine_rewrite_design's "no vote weighting, no multiplication of confidence by source count" principle. Statute is authoritative; LLM noise from extractors can't override it.
- **Con:** Operator-posted rules (which often have time-of-day / seasonal nuance) live in tier 3-4 today. If statute is silent on something the operator page covers, we need to fall through to the operator. Have to make the layering crisp.

**(1c) Statute-locks-rule; lower tiers add metadata.** Highest-tier source dictates the `rule` (on_leash, off_leash, etc.). Lower tiers contribute `rule_modifier` (seasonal closures, hours, off-leash zones within an otherwise-prohibited beach). Distinct fields, distinct responsibilities.

- **Pro:** Captures the real-world layering pattern (HBDB: tier-1 leash statute + tier-2 MOU permitting off-leash at this specific beach + tier-3 operator page with hours = "off-leash 6am-9am per MOU, otherwise prohibited per statute"). Matches what walkthroughs found.
- **Con:** Adds complexity. Need to define which fields are tier-locked vs additive.

**Recommendation: (1c).** It's the only option that survives the HBDB / Fort Funston / Pacifica / SD Mission Bay cases where multi-tier evidence really does say different things about different aspects. Implementation:
- `rule` resolves to the **highest-tier source for that (beach, section, period)**
- `rule_modifier` (hours, season, exception zones) accumulates from **all sources at any tier**
- Disagreements within a tier surface as `rule_conflict` for manual review

**Estimated cost:** 3–5 days. Rewrite of `promote_canonical_dogs_to_beach_dog_policy` to consult both tables; new resolution function; backfill audit.

---

## Question 2 — Governance resolver compatibility

**Question:** Does the existing governance resolver (`_resolve_governance_gold` and the unified governance populator) need to consult the new `agency` table, or can it keep its current CPAD/PAD-US/jurisdictions-driven logic?

**Current state:** The resolver assigns `(beach_fid, authority_domain) → agency_id` based on spatial containment + a denylist for cases like "BLM polygon contains the beach but CPAD says it's actually state park." This populates `beach_agency` with precedence ranks.

**Friction with new agency table:**

- The new `agency` table has explicit type values (`city`, `county`, `state_department`, `federal_department`, `federal_military`, `tribal`, `special_district`). The resolver outputs `agency_id` references but the resolver's source data (CPAD names, PAD-US codes) doesn't always cleanly map to these types.
- The new `agency` table is **richer** — it has every CA city + county + state dept + federal mgmt unit relevant to beaches. The resolver could short-circuit name-matching against this table.

**Recommendation: keep resolver as-is; add a post-resolver name-normalization step.** The resolver does spatial work well; renaming what it spits out is cheaper than rewriting it. New step:

```sql
-- After governance resolver writes beach_agency, normalize agency_id
-- references to the canonical agency table by ILIKE-matching on name.
UPDATE beach_agency ba
   SET agency_id = canonical.id
  FROM agency canonical
 WHERE canonical.name ILIKE strip_prefix(ba.raw_agency_name)
   AND canonical.type = expected_type_for(ba.authority_domain)
   AND ba.agency_id IS DISTINCT FROM canonical.id;
```

**Estimated cost:** 1–2 days. Build the prefix-stripper + expected-type function; run against existing `beach_agency` rows; validate.

**Bonus benefit:** the operator migration from legacy `operators` → new `operator` (commit `9ab86be`) ran an analogous name-match. Lessons learned there transfer.

---

## Question 3 — Operator dual-table reconciliation

**Question:** The pipeline references `public.operators` (plural, legacy, 9,275 rows). The consensus engine and consumer-side code is being designed around `public.operator` (singular, ~162 CA rows post-migration). What's the long-term state?

**Three paths:**

**(3a) Deprecate legacy, migrate fully.** Move all consumers of `operators` to `operator`. Drop `operators` (plural). One source of truth.

- **Pro:** Simple model. No "which table do I read?" question.
- **Con:** Legacy `operators` has 9,275 rows (national scope, future state expansion); new `operator` has 162 (CA-beach-touching only). Dropping legacy means re-doing OR/WA/etc. expansion. Also: legacy has geometry + jurisdiction_id + CPAD attribution that the new schema doesn't capture.

**(3b) Keep both; legacy is the inventory, new is the curated subset.** Legacy = "all gov entities that touch parks anywhere"; new = "entities whose policy_sources we cite." Consumers pick.

- **Pro:** No data loss. Future state expansion stays cheap (legacy already has 9k rows nationally; migration script copies the relevant subset state-by-state).
- **Con:** Dual-table cognitive load. Names will drift unless we add a sync trigger (legacy → new on insert).

**(3c) Keep both; new `operator` becomes a VIEW over legacy `operators` with type mapping.** Same shape as today but no separate data; the migration becomes a view definition + materialized refresh.

- **Pro:** Single source of truth at the data level, dual API at the schema level.
- **Con:** Loses the `delegated_authority_via` FK (would need to be a side-car table). Loses the `metadata` JSONB shape control. View-based migrations are slower for `beach_operator` JOINs unless materialized.

**Recommendation: (3b) with a sync trigger.** Practical and incremental. Concretely:

1. Keep both tables.
2. Add a trigger on `operators` (legacy) INSERT/UPDATE: if a row's geometry overlaps an active CA/OR/WA beach AND the corresponding (name, mapped_type) doesn't yet exist in `operator` (new), insert it with the same metadata-preservation pattern as commit `9ab86be`.
3. For OR/WA state expansion: re-run the `INSERT INTO public.operator SELECT … FROM operators WHERE state_code IN ('OR','WA') AND …` migration template.
4. `delegated_authority_via`, `agency_id` populated only on the new table (legacy keeps its CPAD attribution).
5. Consumers: pipeline scripts that need the geometric overlap data read `operators`; consensus engine + consumer-facing UI read `operator`.

**Estimated cost:** 1 day to write + test the sync trigger.

---

## Question 4 — LLM extraction cost optimization

**Question:** Today's pipeline runs LLM extractors (Phase 6: Tavily + Sonnet) on every active beach. With statute-backed coverage for ~430 beaches now, should we skip LLM extraction for those?

**Cost shape:**

- Per Tavily call: ~$0.008
- Per Sonnet call: depends on prompt; ~$0.01–0.05 per beach typical
- Per full beach extraction round: ~$0.05–0.10
- 1,900 active CA beaches × $0.07 average × N runs per year = real money

**Options:**

**(4a) Skip statute-backed beaches entirely.** Pre-extraction check: if `EXISTS (SELECT 1 FROM beach_policy_source WHERE beach_fid=g.fid)`, skip Phase 6 for this beach.

- **Pro:** Significant cost savings (~25% reduction for CA at current coverage; will grow as Wave 3 progresses).
- **Con:** Loses LLM-extracted nuance (hours, season, signage variations) that the codified ordinance may not specify. Operator pages often have details statutes don't.

**(4b) Skip only at tier 1 (statute) and tier 2 (MOU/lease).** If we have tier-1 statute evidence, statute wins for the `rule` column anyway; LLM extraction on operator pages doesn't change the canonical rule. But still run LLM if we only have tier-3+ evidence.

- **Pro:** Captures the "statute is the truth" insight without losing extraction depth for everything else.
- **Con:** Slightly more complex skip predicate. Need to handle case where tier-1 covers only `rule` but not `rule_modifier`.

**(4c) Run extractors but downgrade their outputs.** LLM extractions continue to land in BEP, but BEP rows for fids that have tier-1/2 evidence are auto-marked `relevance_verified = TRUE` only if they corroborate the statute. Disagreeing rows are flagged as candidates for human review (statute might be outdated, operator might be making unauthorized changes).

- **Pro:** Best of both — keeps the extraction signal alive as audit trail / change detection.
- **Con:** Doesn't save the LLM cost. May be worth it for quality, but not for the cost question.

**Recommendation: (4b)** for the cost-vs-quality tradeoff that respects the source-authority principle. Implementation: gate Phase 6 enrichment per beach on the absence of tier-1/2 beach_policy_source for that beach (or for any of its sections). When tier-1/2 exists, the rule is locked; LLM noise can't move it.

**Estimated cost:** 0.5 days. One predicate change in the Phase 6 orchestrator + monitoring to track cost reduction.

**Estimated savings:** $5–15/month at current coverage; grows linearly with Wave 3 expansion to OR/WA + remaining CA cities.

---

## Question 5 — Refresh + freshness signal

**Question:** Today's pipeline has no "re-check policy_source URLs for updates" loop. Codes change. How do we keep beach_policy_source rows fresh?

**Concrete examples of staleness risk:**

- Laguna Beach Ord. 1729 (adopted 2026-04-28) updated §6.16.020. If we'd captured §6.16.020 a year earlier, we'd be wrong now.
- San Clemente pilot dog beach (approved 2025-08-19) needs walkthrough to confirm CCC approval status.
- Animallaw.info Santa Cruz mirror is "Last Checked: May, 2012" — 14-year-old text — explicitly flagged in our migration's status_note.

**Options:**

**(5a) No refresh; rely on manual re-fetch when descriptions stale.** Status quo. Acceptable for stable code (most CA dog ordinances haven't changed in years) but risky for the 5–10% of cities actively legislating.

**(5b) Quarterly URL re-fetch + diff.** Add a Phase 0.5 job that fetches every `policy_source.source_url`, computes content hash, flags any changes for human review. Doesn't auto-update; surfaces the deltas.

- **Pro:** Catches all source changes within a quarter. Lightweight if we cache content hashes.
- **Con:** Adds operational complexity. Cloudflare-protected sites (Coronado, Monterey municipal.codes) keep blocking automated fetches; ~10% of our sources can't be auto-refreshed.

**(5c) Set `last_verified` timestamps + soft-expire after N months.** Sources `last_verified` > 12 months ago get a soft warning surfaced in admin UI. No auto-fetch; manual re-walkthrough triggered by curator.

- **Pro:** Minimal infrastructure. Trust the human curation pattern that worked for Wave 1-3.
- **Con:** Doesn't actively detect changes; only flags possible-stale-by-time.

**Recommendation: (5c) + (5b) for tier-1 sources only.** Soft-expire at 18 months for all sources (warning in admin UI); add quarterly hash-diff job specifically for the ~140 tier-1 statute URLs. Operator pages and admin-policy pages (tier 3-4) update more often but matter less for the canonical rule; rely on curator judgment.

**Estimated cost:** 1–2 days for the hash-diff job; 1 day for the admin UI staleness indicator.

---

## Phased integration plan

This builds on (does not duplicate) the consensus_engine_rewrite_design Phases 1–5.

**Phase A — Plumb the read path (1 week).** Per Question 1.
- Implement the tier-locked rule resolution (1c) in a new function `resolve_canonical_dog_policy(fid, section)`.
- Wire it into `_promote_zone_rules_for_fid` in shadow mode: compute the new answer, log both old and new, but use the old answer for actual writes.
- Run shadow for 1 week against the six walkthrough beaches + the Wave 3-covered cities.
- Validate: agreement rate, disagreement-pattern analysis.

**Phase B — Normalize governance resolver outputs (3 days).** Per Question 2.
- Build the prefix-stripper + expected-type function.
- Backfill-update `beach_agency.agency_id` to point at canonical `agency.id` rows where confident matches exist.
- Audit the matches; surface unmatched rows for manual review.

**Phase C — Activate read path (2 days).** Per Question 1.
- Switch `_promote_zone_rules_for_fid` to use new resolver for the ~30% of beaches that have any `beach_policy_source` rows.
- Fall through to old BEP-based path for the rest.
- Trigger a full rebuild for affected beaches.
- Validate against the walkthrough regression suite.

**Phase D — Operator dual-table sync (1 day).** Per Question 3.
- Add the legacy→new operator sync trigger.
- Document the operator vs operators distinction for future maintainers.

**Phase E — Phase 6 cost gate (0.5 days).** Per Question 4.
- Skip LLM enrichment for beaches with tier-1/2 evidence.
- Monitor cost reduction over the next month.

**Phase F — Freshness loop (1 week).** Per Question 5.
- Add hash-diff cron for tier-1 statute URLs.
- Soft-expire indicator in admin UI.

**Phase G — Cleanup (1-2 weeks, optional).** Per consensus_engine_rewrite_design Phase 5.
- Retire vote-weighting math.
- Drop `beach_field_consensus` or repurpose as audit-only.
- Make `beach_policy_source` the unconditional canonical source for any beach that has it.

**Total estimate: 3–4 weeks of focused work for Phases A–F.** Phase G is a follow-on cleanup that depends on Wave 3 (and later OR/WA) reaching enough beach coverage.

---

## Risks + mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Shadow-mode disagreements reveal correctness bugs we didn't know about | Medium | That's the point of shadow mode. Triage disagreements case-by-case; some are bugs in the new path, some are bugs in the old path, and Wave 3 walkthroughs are the regression suite for the new path. |
| `manual_curator` override pattern breaks | Low | Phase A explicitly preserves the manual_curator bypass per consensus_engine_rewrite_design — the new resolver respects it. |
| Scoring tier recomputes thrash on shadow-mode A/B comparisons | Medium | Shadow mode WRITES nothing to canonical; logs only. Trigger fires only on canonical writes. |
| Cost gate (Phase E) loses operator-page detail that was nuancing the rule | Medium-low | Per Question 4 recommendation (4b), gate only when tier 1/2 exists; that's where statute already locks the rule. We weren't relying on operator pages there anyway. |
| Hash-diff cron false-positives on minor page edits (footer changes, etc.) | High | Use selector-scoped hash (only the body content of the section page, not the whole page) — same trick the fetch_municode.py already uses. |
| Operator dual-table sync trigger races on bulk inserts | Low | Use STATEMENT-level trigger, not row-level. Test with the OR/WA expansion migration. |

---

## Non-goals (out of scope for this design)

- **Refactoring LLM extractors** (Tavily, Sonnet prompts) — they emit what they emit; this design is about consuming their output.
- **Amenities consensus** — only `field_group = 'dogs'` is in scope. Amenities-side consensus has its own deferred rewrite ([[deferred-amenities-consensus]]).
- **Description / photo / scoring generation logic** — those consume `beach_dog_policy.zone_rules` as before; the new resolver writes the same shape, just from new sources.
- **Cross-state extension before A–F complete** — this design ships first for CA, then OR/WA reuse the patterns.

---

## Open questions for Franz

Things this design defers and that need a decision before implementation:

1. **Phase E (cost gate)**: are we comfortable skipping LLM extraction for tier-1-covered beaches and treating statute as authoritative, or do we want to keep extraction running as a quality/change-detection signal? (Pure cost answer says skip; quality answer says keep.)

2. **Phase B's manual review queue**: for governance resolver name-matches that don't auto-resolve, how do we route to a human? New admin UI table? Email queue? Just a SQL view someone reads?

3. **Phase G timing**: is the 30%-coverage threshold for retiring vote weighting reasonable, or do we want higher (50%, 80%)? Lower threshold ships sooner; higher threshold makes the cutover safer.

4. **Refresh cadence**: quarterly hash-diff feels right for tier-1, but worth confirming. Manual re-walkthrough could be triggered by the curator instead of by elapsed time.

5. **Cross-cutting**: are there consumer-side surfaces (the dogbea.ch / dog-beach-claude UI) that already cite policy_source citations? If so, this design unlocks more sophisticated display ("per HBMC §13.08.070 the city default is leashed; per MOU between City of HB and PSHDB, HBDB is the off-leash carve-out"). Worth a separate doc.

---

## Related docs

- [[pipeline-overview]] — current 10-phase pipeline reference
- [[consensus_engine_current_state]] — current consensus engine data flow
- [[consensus_engine_rewrite_design]] — schema rewrite design Phases 1–5
- [[consensus_engine_audit_2026_05_16]] — audit that motivated rewrite
- [[wave3-complete-2026-05-16]] — what Wave 3 actually shipped
- [[operator-not-pseudo-agency]] — entity model decision driving the operator table design
- [[entity-modeling]] — first-principles framing
- [[law-as-primary-source-ca]] — initiative this all serves
- [[state-expansion-playbook]] — generalize patterns to OR/WA after CA stabilizes
