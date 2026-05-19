# Operate Pipeline — Spec v3

**Status:** REWRITTEN 2026-05-18 late evening after discovering the two-table architecture.
**Supersedes:** v2 (which oversold codify-overlap based on cross-table ID collisions) + v1 ("418 stranded gold mine" framing). The actual gap is bigger than v2 thought and structurally different.
**Companion:** [`jurisdiction_policy_source_playbook.md`](jurisdiction_policy_source_playbook.md).

## v3 headline finding (the one that matters)

**There are TWO operator tables and they DO NOT share an ID space:**
- `public.operators` (plural, **9,275 rows**) — broad US-wide catalog the EXTRACTOR uses. `operator_dogs_policy.operator_id` + `operator_policy_extractions.operator_id` FK → here.
- `public.operator` (singular, **153 rows**) — curated subset wired into cascade. `beach_operator.operator_id` + `policy_source.issuing_operator_id` FK → here.

V2 audit JOINed via `operator_id` against the singular table — coincidental cross-table ID collisions produced false-positive corruption findings. The corrected audit (operators-plural) reveals:

| Classification | v2 (wrong join) | v3 (correct join) |
|---|---:|---:|
| Operators with extraction at conf≥0.7 | 22 | **274** |
| net_new (legitimate, no existing ps row) | 0 | **104** |
| corrupt (URL/name mismatch) | 15 (68%) | **60 (~22%)** |
| low_conf | 6 | 109 |
| duplicative_clean | 0 | 1 |

**Real Operate gap:** 104 net-new extractions worth bridging + 9,122 plural operators without extraction worth probing if they manage beaches. The v2 "76% codify-duplicate" finding was about the curated 153, not the extraction-side 9,275 — irrelevant to the real gap.

**Bridge today produces zero output** even after the table fix: of 165 conf≥0.7 candidates, 68 have a matching singular-operator row but no beach_operator link (Phase 2 needed); 97 have no singular row at all (Phase 1d: curator-create singular row, like today's TCLT pattern).

---

## TL;DR (v2)

Operate is the third track of the product alongside Codify (built) and Cascade (existing) — but it's **much narrower than the v1 spec assumed**. After today's audit:

- **~76% of `operator` rows are codify-duplicates** (cities + counties acting as their own operators). For these, the operator's posted-policy page is just the lawmaking entity announcing its own codified rule → that's `subtype='agency_administrative_policy'` in Codify (Step 6.7 / 6.8 path we built today), not a separate pipeline.
- **The real Operate cohort is ~13 entity-specific operators** where the operating authority is NOT the lawmaking authority: 11 special_districts (port districts, conservancies) + 2 nonprofits (PSHDB, TCLT) + a handful of partially-codified federal sub-units. Plus all ~6,163 dog parks (where Operate is the PRIMARY pipeline because there's no codified-statute alternative).
- **The 418-row extraction infrastructure is structurally broken** (Tavily search-precision bug pairs operators with wrong URLs at high LLM confidence). Not "stranded gold mine" — un-bridgable as-is.
- **Real Operate v1 is essentially done** at 4 well-bridged ps rows. Net-new coverage requires fixing the extractor + dog-park expansion + selective entity-specific curator-fixed bridges.

Operate v2's primary value is **dog parks** + the small set of entity-specific operators. Almost everything labeled "operator" in the existing infrastructure is actually Codify under another name.

---

## 1. The sharper distinction (vs Codify)

| | Codify | Operate (sharper v2 framing) |
|---|---|---|
| Source type | Codified statute / regulation / agency administrative policy | Operator-posted policy where the OPERATOR ≠ THE LAWMAKER |
| Authority basis | Legal (city council, state legislature, federal agency rule-making) — covers the lawmaking entity's own published policy too | Operational (the operator stewards the facility under delegated authority); the city/state authorizes it but doesn't write the rules |
| Scope shape | Polygon (jurisdictions / counties / PAD-US units) | Entity (`beach_fid`, `dog_park_id`) |
| Examples | Bainbridge Island MC, OAR 736-021, 36 CFR §2.15, NPS Olympic Compendium, "Pets at Manhattan Beach" parks-dept page (subtype=agency_admin_policy) | PSHDB at Huntington Dog Beach (nonprofit operator); Port of Astoria dock rules; TCLT at Houda Point; Rosie's Dog Beach operator-posted policy (City of Long Beach as operator); EVERY dog park's posted hours/breed/license rules |
| Coverage at scale | ~95%+ of MVP+ beaches (CA/OR/WA) | ~4 beaches today; potential ~13 entity-specific + 1,241 dog parks (CA 747 + OR 198 + WA 296, verified via counties spatial join 2026-05-18; only 122 have an OSM operator tag) (CA/OR/WA scope) |

### The 76% codify-overlap claim — receipt

Today's audit of the `operator` table (152 rows):

| operator.type | Count | Codify-duplicate? | Real Operate? |
|---|---:|---|---|
| city | 73 | Yes — same legal entity as Codify city jurisdiction | No |
| county | 42 | Yes — same as Codify county | No |
| state | 7 | Mostly — Codify `--state-agency-rule` already covers | No |
| county_department | 4 | Mostly — same as parent county Codify | No |
| federal_department | 12 | Mostly — Codify CFR baselines + Compendium overrides cover NPS/USFWS/USFS | Partial — sub-units rarely |
| federal_military | 1 | USCG — Codify could cover | No |
| **special_district** | **11** | **No** — port districts, conservancies, regional authorities | **Yes** |
| **nonprofit** | **2** | **No** — PSHDB, TCLT | **Yes** |
| **TOTAL** | **152** | ~115 (76%) | **~13** |

When "City of Manhattan Beach" operator publishes a "Pets at the Beach" page on cityofmanhattanbeach.gov, that's the City announcing its OWN municipal code. The right modeling is **one Codify ps row** (subtype=`agency_administrative_policy` or `municipal_code`) issued by the City as agency. The "operator" row for the City is a redundant entity for the same legal authority.

**Per [[walkthrough-hbdb]] the real Operate pattern is layered authority:** city authorizes, nonprofit/special-district stewards, posted policy reflects the steward's operational rule (which may override the city's general ordinance for the specific entity). That's PSHDB, TCLT, port districts. Not generic city/county operators.

### When operator overrides codified (the layered case)

When a real entity-specific operator (PSHDB-class) IS distinct from the lawmaking entity:

1. Codify provides territorial floor (HB citywide leash law → on_leash for most HB beaches)
2. Operate provides entity-specific override (PSHDB posted off-leash for Dog Beach specifically)
3. Consensus engine sees both; operator wins for the SPECIFIC entity
4. Other beaches in same polygon stay at codify floor

The walkthrough-hbdb pattern is the prototype. Today's PSHDB curator-fix migration (ps_287) ships this exact pattern.

---

## 2. What exists today (final audit, 2026-05-18 evening)

### Database state

| Table | Rows | Status |
|---|---:|---|
| `operator` | 152 (153 after TCLT add today) | ~13 are real entity-specific; ~115 are codify-duplicates |
| `beach_operator` | 3 | Sparse; today's PSHDB + Crystal Cove Conservancy entries |
| `operator_dogs_policy` | 418 | **CORRUPT** — Tavily search-precision bug paired most operators with wrong URLs (15 of 22 conf≥0.7 operators classified as `corrupt` by [[scripts/audit_operator_extractions.py]] 2026-05-18) |
| `operator_policy_extractions` | 722 | Raw artifacts; same corruption |
| `operator_policy_exceptions` | 393 | Exception detail; same corruption likely propagates |
| `policy_source` (subtype=`operator_posted_policy`) | 12 | See classification table below |

### The 12 operator_posted_policy ps rows (post-Phase-1a backfill)

| Classification | Count | Rows | Status |
|---|---:|---|---|
| `well_bridged` (FK + beaches) | **4** | ps_20 PSHDB-original, ps_22 LB Rosie's, ps_233 TCLT Houda Pt, ps_287 PSHDB curator-fix | The real Operate state. Driving consensus correctly. |
| `real_but_no_bps` | **1** | ps_21 EBRPD Crown Beach | Real policy; needs beach_operator link (Phase 1b candidate) |
| **BEP source-class anchors — LOAD-BEARING, DO NOT TOUCH** | **7** | ps_26 Park URL Extractor, ps_28 Operator Dogs Policy v1, ps_33 Beach Policy V2 Dogs, ps_34 Operator City Extractor, ps_35 Section Research Extractor v1, ps_38 Research (curator), ps_42 Operator Policy Exceptions v1 | Anchor rows tagging ~12,033 BEP (beach_field_consensus) dog-policy rows with `policy_source_id` FK so source-class tier logic flows through the consensus engine. Per `supabase/migrations/20260516_consensus_phase4_backfill_bep.sql`. |

### The extraction infrastructure (Dagster pipeline)

Live consumers of `operator_dogs_policy.summary`:
- `scripts/dagster/dog_beach/dog_beach/assets/per_fid_enrichment.py` — Haiku-driven per-section beach rule extraction
- `scripts/audit/state_population_audit.py` — joins for state coverage audits
- `scripts/audit_artifacts.py` — audit references

Pipeline assets:
- `scripts/extract_operator_dogs_policy.py` — extractor (Tavily search → URL → 3-pass LLM)
- `scripts/one_off/merge_operator_dogs_policy.py` — extractions → canonical dogs_policy
- `scripts/dagster/dog_beach/dog_beach/assets/operator_llm_cascade.py` — orchestrator (Phases 26+27)

**The bug:** the extractor accepts Tavily's top URL without validating that the operator name appears in the URL host or fetched page. Same failure-mode pattern as the Municode-silent-redirect issue we fixed this morning via the catalog pre-check.

---

## 3. Real Operate v2 scope

**Drop from scope:** the ~115 city/county/state/county_department operators that are codify-duplicates. Their "operator pages" are codify-side via `agency_administrative_policy` subtype (Step 6.7/6.8 already handles this).

**In scope:**

### A. Entity-specific non-government operators (~13 today, more discoverable)

- 11 special_districts (port districts, conservancies, water districts, regional authorities)
- 2 nonprofits (PSHDB, TCLT) — likely more discoverable via PAD-US `mng_name` + OSM operator tags
- Per-unit federal sub-units NOT covered by codify CFR baseline + Compendium overrides (small handful)

For each: discover → fetch operator-published page → extract rule → bridge via `beach_operator` link.

### B. Dog parks (~6,163 entities)

Per [[dogpark-rules-are-operator-posted]]: dog parks have NO codified-statute alternative. Operate is THE pipeline. Discovery + extraction needed for hours, capacity, license, breed rules.

Today's dog-park coverage in `operator_dogs_policy` / `policy_source`: essentially zero. This is the largest greenfield Operate opportunity.

### C. Walkthrough-hbdb-class layered-authority overrides

When city ordinance says X but a specific entity within the city operates differently (PSHDB off-leash carve-out from HB on-leash; Rosie's off-leash carve-out from LB on-leash; Cape Lookout leash override from OR Ocean Shore baseline). The carve-outs themselves are Operate-side even when stewarded by city departments.

---

## 4. Data model — minimal changes (unchanged from v1)

Same as v1; reuse `operator` + `beach_operator` + `policy_source(subtype=operator_posted_policy)` + `beach_policy_source` + (optional new) `dog_park_operator`.

Phase 2 backfill of `beach_operator` is the structural unlock.

---

## 5. Pipeline architecture — sharpened

### For entity-specific operators (cohort A above)

Five phases, mirrors Codify:

1. **Discovery** — find the operator entity (PAD-US `mng_name`, OSM `operator` tag, curator-fed CSV)
2. **URL discovery** — find operator's published rule page (per-operator-type patterns; web_search fallback)
3. **Extract** — Tavily / Anthropic LLM (the existing 3-pass extractor with **operator-name validation added** — the missing piece)
4. **Bridge** — ps + bps via `beach_operator` link
5. **Cascade** — automatic per playbook tenet #5

### For dog parks (cohort B above)

1. **Catalog** — already exist via `osm_dog_parks` (6,163 rows)
2. **Operator discovery** — many have `operator` tag from OSM (669); others need web_search
3. **URL + extract** — per-dog-park pets policy page (often a sign photo or city parks page)
4. **Bridge** via `dog_park_operator` (new table) + cascade

### For layered-authority overrides (cohort C above)

Per-case curator-fixed bridges (PSHDB pattern from ps_287 today). Not bulk-pipelineable; each is a known operator with a known carve-out.

---

## 6. Migration plan (v2 — REVISED from v1)

**Phase 0 — extractor fix:**
1. Add operator-name validation to `scripts/extract_operator_dogs_policy.py` — reject Tavily URLs where host + fetched title don't contain operator-name tokens. Analog of the Municode pre-check pattern from this morning.
2. Re-run extractor on the 15 corrupted-classified operators (cheap; ~$0.10-0.30 LLM each)
3. Verify post-fix audit drops `corrupt` classification to ≈0

**Phase 1 — done.** 4 well-bridged ps rows; one orphan (ps_21 EBRPD) needs Crown Memorial SB bridge.

**Phase 2 — beach_operator backfill (the structural unlock):**
1. Backfill from PAD-US `mng_name` for federal+state-park sub-units
2. Backfill from OSM `operator` tag for tagged beaches (669 sources)
3. Curator-fed CSV for known nonprofit/special-district operators
4. Target: every beach with a NAMED entity-specific operator has ≥1 `beach_operator` link

**Phase 3 — dog parks (the greenfield):**
1. Build `dog_park_operator` (or extend `beach_operator` to `entity_operator`)
2. Run Phase B+C+D pipeline against the 6,163 OSM dog parks
3. Cost estimate: 1,241 × 3 LLM calls × $0.005 avg = ~$19 for MVP+ states (vs full US ~$92)
4. Many dog parks have no published policy (just on-site signage) — defer rate will be high

**Phase 4 — Codify Step 6.7 demotion:**
1. Step 6.7 (agency-policy fallback) currently produces `agency_administrative_policy` ps rows
2. Refactor: when the operator IS the lawmaking entity, keep as agency_admin_policy in Codify
3. When the operator is entity-specific (PSHDB-class), promote to Operate
4. Largely cosmetic — same data, different track label

---

## 7. Quality gates (unchanged from v1 §8)

Same Op-1 through Op-4 from v1.

---

## 8. CLI design (slimmed from v1)

```
scripts/bridge_operator_to_cascade.py
  --operator-ids 7,52,160 --label phase1_fix
  --all-bridgable
  --dry-run
  --confidence-threshold 0.7

scripts/audit_operator_extractions.py
  --confidence-threshold 0.7
  # produces tmp/operate_phase1c_audit_<ts>.{jsonl,md}

# Phase 0 (TODO): extend the extractor
scripts/extract_operator_dogs_policy.py
  --validate-operator-name    # NEW; analog of Municode pre-check
  --re-extract-operator-ids <list>
```

---

## 9. Success metrics (v2 — REVISED)

- Phase 0 fix: post-fix audit shows `corrupt` ≤ 5% (was 68% pre-fix)
- Phase 1: 4 well_bridged ps rows + 1 EBRPD bridge → 5 total
- Phase 2: every beach with a real entity-specific operator (~13 today, perhaps 30-50 after discovery) has `beach_operator` link
- Phase 3: ≥50% of MVP+ state dog parks have an operator_posted_policy ps row
- Op-3 consensus correctness: when operator and codify disagree, consensus picks operator (per-entity override correctness)

Drop the v1 metric "all 418 extractions bridged" — that was an artifact of the misframing. The 418 includes many city/county operators that are codify-duplicates; they shouldn't be bridged at all.

---

## 10. Worked examples

### Example A — PSHDB / Huntington Dog Beach (shipped today)

The prototype. PSHDB nonprofit operates Dog Beach via partnership with City of HB. City's general leash ordinance applies citywide; PSHDB's posted off-leash optional rule overrides for the carve-out. Today's ps_287 is the canonical Operate row.

### Example B — Port of Astoria (in scope, not yet shipped)

Port of Astoria (special_district operator) manages docks + adjacent shoreline. Has its own posted rules different from City of Astoria's general ordinance. Real Operate target — entity-specific authority that overrides territorial codify for that entity.

### Example C — City of Manhattan Beach (NOT in scope)

The "operator" row for City of Manhattan Beach IS codify-duplicate. The City's "Pets at the Beach" page on cityofmanhattanbeach.gov is just the City announcing its own MC. Properly modeled as Codify's `agency_administrative_policy` subtype, NOT Operate. Drop from Operate scope.

### Example D — Any random dog park (greenfield)

E.g., a city dog park in Portland. No codified-statute layer. Posted hours (6am-10pm), breed restrictions, license requirements — all operator-posted by the city parks dept (acting as operator, not lawmaker). Operate is THE pipeline because Codify has nothing to say.

---

## 11. Closed decisions (was §13 in v1)

| Question | Resolution |
|---|---|
| Naming | **Operate** ✓ (Franz 2026-05-18) |
| Bridge mechanism | **Python script + migration files** ✓ (Franz 2026-05-18) |
| Phase sequencing | Phase 1 bridge first (done — outcome was sharper scope, not coverage gain) |
| Dog-park scope | **MVP+ states only** (CA/OR/WA) for v1 — recommend 1,241 dog parks (CA 747 + OR 198 + WA 296, verified via counties spatial join 2026-05-18; only 122 have an OSM operator tag) (full 6,163 deferred) |
| HOA / AV-flagged | Skip by default; `--include-av-flagged` opt-in |
| Per-batch approval | Same as Codify ✓ (continued discipline) |
| 418-stranded gold-mine | **MISFRAMED in v1** — they're mostly redundant city/county extractions, and the corrupt-attribution bug means even non-redundant ones are unreliable |
| Operator overlap with codify entities | **~76% are codify-duplicates** — drop from Operate scope, model as agency_admin_policy in Codify |

---

## 12. Related

- [[operators-as-first-class]] — the parent insight that triggered Operate as a distinct track
- [[dogpark-rules-are-operator-posted]] — dog parks are Operate's primary value
- [[walkthrough-hbdb]] — concessionaire / layered-authority pattern; the canonical real-Operate case
- [[walkthrough-crown-memorial]] — special-district pattern (EBRPD)
- [[dont-classify-unknowns-for-deletion]] — applies to the BEP source-class anchors
- [[operator-not-pseudo-agency]] — non-gov operators stay as operators with delegated_authority_via FK
- [[never-solve-same-problem-twice]] — applied to today's audit: the 76% codify-overlap was already implicit in [[operator-not-pseudo-agency]] but not surfaced until Franz asked
- `scripts/bridge_operator_to_cascade.py` — bridge mechanism (Phase 1)
- `scripts/audit_operator_extractions.py` — audit tool (Phase 1c, revealed corruption)
- `scripts/extract_operator_dogs_policy.py` — extractor (Phase 0 fix target)
- `scripts/dagster/dog_beach/dog_beach/assets/operator_llm_cascade.py` — Dagster orchestrator

---

## 13. What today's session produced

- This spec (v2 after audit-driven rewrite)
- `scripts/bridge_operator_to_cascade.py` (mechanism)
- `scripts/audit_operator_extractions.py` (audit tool, revealed extractor corruption)
- 2 migrations applied: PSHDB curator-fix (ps_287) + FK backfill (3 ps + 1 new operator TCLT)
- 1 new HARD pin [[dont-classify-unknowns-for-deletion]]
- This refined framing of Operate (76% overlap insight) — the most-load-bearing learning of the Operate work

**Real Operate state (2026-05-18 evening):** 4 well-bridged ps rows + 1 EBRPD orphan + extractor needs Phase-0 fix before scaling. Real coverage gain is Phase 2/3 work, not bridging.
