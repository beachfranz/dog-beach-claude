# Operate Pipeline — Spec v1

**Status:** DRAFT 2026-05-18 — for Franz's review on his return from AFK.
**Author:** Codify v1 build context (this session).
**Companion:** [`jurisdiction_policy_source_playbook.md`](jurisdiction_policy_source_playbook.md) (Codify v1 playbook — pattern this spec mirrors).

---

## TL;DR

Operate is the third track of the dog-beach data product alongside Codify (built) and Cascade (existing). It targets **operator-posted policies** that govern beach/dog-park behavior — rules posted at the entity by the people who run it (concessionaires, park departments, NPS superintendents, HOA boards, port districts, dog-park managers).

Operate produces `(entity, rule, ...)` tuples; Codify produces `(polygon, rule, ...)` tuples. Both feed the same Cascade (consensus engine → `beach_dog_policy.zone_rules`).

**Big finding from today's audit:** an Operate **extraction layer** already exists (`operator_dogs_policy` 418 rows; `operator_policy_extractions` 722 rows; `operator_policy_exceptions` 393 rows — 3-pass LLM extraction). It's NOT connected to the consensus engine: only **11 `policy_source` rows** of `subtype='operator_posted_policy'` exist. The extracted data is sitting in a parallel structure that the cascade can't see.

The Operate pipeline must therefore do **two things**:
1. **Bridge** existing extraction → policy_source / beach_policy_source (one-time migration)
2. **Ongoing pipeline** to find new operator entities, fetch their rules, extract, attach to beaches

---

## 1. Vocabulary — the three-track model

| Track | What it produces | Source layer | Scope shape | Examples |
|---|---|---|---|---|
| **Codify** (built) | `(polygon, rule, subtype, domain)` | Issuing agencies — city / county / state legislators / federal regulators | TERRITORIAL — applies to all beaches inside polygon X | Bainbridge Island MC §6.04.030, 36 CFR §2.15, OAR 736-021-0070 |
| **Operate** (this spec) | `(entity, rule, subtype, domain)` | Entity operators — parks depts, concessionaires, NPS superintendents, HOA boards, port districts, dog-park managers | ENTITY-SPECIFIC — applies to that exact beach / dog park / facility | Cape Lookout SP leash override, Rosie's Dog Beach off-leash sign, HBDB concessionaire rules, Crissy Field Compendium, dog-park hours sign |
| **Cascade** (existing) | `beach_dog_policy.zone_rules` (consumer surface) | Trigger chain on bps INSERT/UPDATE — `promote_entity_dogs_to_beach_dog_policy` → `_promote_zone_rules_for_fid` | Per-beach consensus | What end users see in the app |

Per [[codify-cascade-vocabulary]] + [[operators-as-first-class]] — Operate is the name. Pin [[codify-operate-cascade-vocabulary]] when adopted.

### Why Operate differs from Codify

| Dimension | Codify | Operate |
|---|---|---|
| Source type | Codified statute / regulation | Operator-posted policy (sign, web page, lease, MOU) |
| Authority basis | Legal (city council, state legislature, federal agency rule-making) | Operational (the operator runs the facility; their posted rules govern) |
| Discovery | URL templates per platform (Municode, ecode360, etc.) | Per-operator: agency homepage, parks dept, concessionaire site, posted-sign photo OCR, NPS Compendium PDF |
| Scope | Polygon (jurisdictions / counties / PAD-US units) | Entity (`beach_fid`, `dog_park_id`) |
| Spatial join | `ST_Intersects(beach.geom, polygon.geom)` | NONE — operator already knows its entities (via `beach_operator`, `dog_park_operator`) |
| Resolver rank | High for codified law; medium for agency_administrative_policy | High for entity-specific operator rules (they override territorial codified for that entity); medium for general posted rules |

### When operator overrides codified

Per [[walkthrough-hbdb]] + [[walkthrough-crown-memorial]] layered-authority: operator rules at a SPECIFIC entity override territorial codified rules **for that entity only**. Example: City of Long Beach's general leash ordinance doesn't override Rosie's Dog Beach posted off-leash rule.

The Cascade consensus engine ranks:
1. Per-entity operator rule (Operate) — HIGHEST for that entity
2. Per-entity codified rule (Codify deep-dive — e.g., NPS Compendium) — same tier as Operate; consensus engine adjudicates
3. Territorial codified rule (Codify standard) — fills in beaches without entity-specific overrides
4. State baseline (per [[state-baseline-coverage]]) — bottom layer

---

## 2. What exists today (the audit Franz keeps asking about)

### Database state

| Table | Rows | Purpose | Connected to cascade? |
|---|---:|---|---|
| `operator` | 152 | Operator entities (HOAs, concessionaires, NPS supt offices, port districts) | Partially — via `beach_operator` |
| `beach_operator` | **3** | beach × operator mapping w/ scope_section, precedence_rank | YES (table exists) but **massively under-populated** |
| `operator_dogs_policy` | **418** | 3-pass LLM extraction output (pass_a default_rule, pass_b temporal, pass_c exceptions) | **NO — disconnected** |
| `operator_policy_extractions` | **722** | Raw extraction artifacts (per-source fetches, per-pass token counts) | NO — disconnected |
| `operator_policy_exceptions` | **393** | Per-exception details (off-leash carve-outs, seasonal closures) | NO — disconnected |
| `policy_source` (subtype='operator_posted_policy') | **12** (was 11; +PSHDB curator-fix 2026-05-18) | Operator rules surfaced to consensus engine | YES — but see audit below; only **4 are real operator policies** |
| `policy_source` (subtype='superintendents_compendium') | 6 (today's NPS shipments) | NPS unit-specific operator-style overrides | YES |
| `policy_source` (subtype='agency_administrative_policy') | 67 (CA) + today's new | Agency-published operator pages (parks.ca.gov/Dogs, fws.gov refuge pages) | YES |

**The gap (REVISED 2026-05-18 evening after PSHDB Phase 1 apply + 12-row audit):**

The "418 stranded" framing in the v1 draft was TOO SIMPLE. The actual picture:

**Of 12 `operator_posted_policy` ps rows (audit by classification):**

| Classification | Count | Rows | Meaning |
|---|---:|---|---|
| `well_bridged` (has FK + has beaches) | **1** | ps_287 PSHDB (today's curator-fix) | Properly attributed + driving consensus |
| `bridged_but_no_op_fk` (has beaches but issuing_operator_id NULL) | **3** | ps_20 PSHDB-original, ps_22 LB Rosie's, ps_233 Trinidad Coastal Land Trust | Real operator policies driving consensus, just lacking FK metadata |
| `op_fk_but_no_beaches` | 0 | — | — |
| `orphaned` (extraction-infrastructure leftovers, not real policies) | **8** | ps_21 EBRPD Crown (real, no beaches), ps_26/28/33/34/35/38/42 — labeled "Park URL Extractor", "Operator Dogs Policy v1", "Section Research Extractor", "Operator Policy Exceptions v1", "Research (curator)", "Beach Policy V2 Dogs" | Meta/placeholder rows from extraction infrastructure setup; NOT real operator policies. Should be CLEANED UP. |

**REAL operator-pipeline coverage today: 4 beaches** (Dog Beach fid 6212, 2 Rosie's Dog Beach fids, 1 Houda Point/Camel Rock fid via Trinidad Coastal Land Trust).

**The 418 stranded extractions are PARTIALLY DUPLICATIVE** of the 4 real ps rows above:
- PSHDB (operator_id=7) has BOTH an extraction (operator_dogs_policy row, corrupt) AND a ps row (ps_id=20, correct)
- The 418 extractions don't 1:1 map to "operators with zero ps coverage"

**Real Operate gap, sharpened:** operators with ZERO ps rows AND ZERO bps links today AND a real operator-posted policy worth extracting. The cohort is smaller than 418; needs audit per-operator to identify true gap.

**Plus** ~1,500 dog parks (per [[dogpark-rules-are-operator-posted]]) where Operate is the PRIMARY pipeline because there's no codified-statute alternative — these are largely uncovered today.

### Code state

| Asset | Status |
|---|---|
| `operator` + `beach_operator` schemas | exist |
| 3-pass extractor (pass_a/b/c) | Was built (evidence: 418 rows w/ confidence per pass) — script may be in `scripts/` somewhere; needs audit |
| Bridge to `policy_source` | **MISSING** |
| Discovery layer (find operator pages) | Partial — Codify Step 6.7 does this for agency_administrative_policy |
| Per-entity URL templates | Codify has them for codified platforms; Operate needs per-operator-type patterns |
| CLI mode | None — needs `scripts/derive_operator_posted_policy.py` (analog of derive_policy_source) |
| `--list-operator-coverage` helper | None |
| Cascade integration | Cascade already handles `operator_posted_policy` subtype if bps rows exist |
| Quality gates | None operator-specific |

---

## 3. Data model — minimal changes

### Reuse, don't add

Per [[never-solve-same-problem-twice]] + Franz "leverage existing tables wherever possible" 2026-05-18:

- `operator` (152 rows) — KEEP as canonical entity table
- `beach_operator` (3 rows) — KEEP; needs massive backfill (target: every beach with a non-jurisdictional operator)
- `policy_source` (subtype='operator_posted_policy') — KEEP; this is the cascade-facing structure
- `beach_policy_source` — KEEP; operator policies attach to beaches via this table the same way codified ones do
- `operator_dogs_policy` (418 rows) — KEEP as the **extraction-artifact table**; consider it the operator analog of `policy_source.full_text` + structured extraction. Bridge to policy_source on-demand.
- `operator_policy_extractions` (722 rows) — KEEP as the raw-LLM-output audit trail
- `operator_policy_exceptions` (393 rows) — KEEP as the structured-exception detail (off-leash zones, seasonal closures); analog of `beach_policy_source_temporal`

### What to add

| Object | Purpose | Notes |
|---|---|---|
| `dog_park_operator` table (or rename `beach_operator` → `entity_operator` with entity_type column) | dog-park-to-operator mapping; today `beach_operator` only handles beaches | Dog parks are the PRIMARY Operate target ([[dogpark-rules-are-operator-posted]]) |
| Trigger: `operator_dogs_policy` UPSERT → derive `policy_source` (operator_posted_policy) + `beach_policy_source` for each linked entity | Auto-bridge extraction → cascade | Idempotent; NOT EXISTS / ON CONFLICT pattern from Codify v1 |
| Optional: `operator_url_templates` table (similar to `STATE_PLATFORM_PRIORITY` from Codify) | Per-operator-type URL discovery patterns (NPS pets.htm, USFWS rules-policies, OPRD per-park) | Could be Python dict; ETL later if it grows |

---

## 4. Pipeline architecture (mirrors Codify v1)

Five phases, analogous to Codify's Phase A/B/C/Step 6/Cascade:

### Phase A — Operator entity discovery

For a target state (or set), enumerate operator entities that own/manage beaches:

```sql
-- Beaches in state X that lack any beach_operator row
-- → candidates for operator discovery
SELECT b.fid, b.name, b.state
FROM beaches_gold b
WHERE b.state = ANY(:states) AND b.is_active
  AND NOT EXISTS (SELECT 1 FROM beach_operator bo WHERE bo.beach_fid = b.fid)
ORDER BY b.name;
```

Discovery sources (priority order):
1. **OSM `operator` tag** on beach POIs + dog-park polygons (already loaded; 669 dog-park operators captured)
2. **PAD-US `mng_name`** when the managing org is an operator (e.g., NPS Superintendent's Office for a unit)
3. **CCC YourCoast** (per [[ca-coastal-beaches-inventory-research]]) — has operator metadata for CA
4. **Curator-fed** via CSV (`beach_fid, operator_name, operator_type`)
5. **Agent-discovered** via web_search ("Who manages <beach name>")

CLI:
```bash
python scripts/derive_operator_posted_policy.py --discover-operators --states OR,WA
```

### Phase B — Operator URL discovery

For each operator entity, find the operator's published pet/rule URL:

Per-operator-type patterns:
- **State park operator** (OPRD per-park, CDPR per-park, WSPRC per-park): try `stateparks.<state>.gov/<park-slug>` then `<state>parks.gov/?page=<id>`
- **NPS superintendent**: `nps.gov/<4letter>/learn/management/superintendents-compendium.htm` + `nps.gov/<4letter>/planyourvisit/pets.htm` (Codify v1 already builds these — share logic)
- **USFWS refuge**: `fws.gov/refuge/<refuge-slug>/visit-us/activities/dog-walking` or `/rules-policies`
- **Port district**: per-district website (no template; web_search)
- **HOA / private operator**: per-operator (no template; AV-flagged per [[deferred-canyon-lake]])
- **Concessionaire** (HBDB Friends, Stinson Beach Restaurant, etc.): per-operator
- **Dog-park operator**: city parks dept page or onsite-sign photo

CLI:
```bash
python scripts/derive_operator_posted_policy.py --discover-urls --operator-ids 12,34,56
```

Output: `operator.rules_url` filled in (or staged in `operator_dogs_policy.source_url` if pre-existing pattern).

### Phase C — Extract operator rule (3-pass already exists)

The existing 3-pass extractor (evidence: pass_a/b/c columns in `operator_dogs_policy`) does:
- **Pass A**: default_rule + applies_to_all + leash_required + quotes + confidence
- **Pass B**: time_windows + seasonal_closures + spatial_zones + quotes + confidence
- **Pass C**: exceptions + ordinance_reference + summary + quotes + confidence

This is essentially Codify's Step 6 (LLM rule decision) + Codify's `extract_temporal_from_policy_source.py` collapsed into one driver. **Audit existing script; potentially keep as-is.**

If the existing script is functional, just need to:
1. Locate it (probably in `scripts/`)
2. Verify it can run against new operators
3. Add CLI argument compatibility with the codify v1 patterns (--from-csv, --rescue, etc.)

### Phase D — Bridge to cascade (NEW; the main gap)

**This is the missing piece** — current 418 extraction rows don't see the consumer surface.

Build a script + trigger that does:

```sql
-- For each operator_dogs_policy row with pass_c_confidence >= 0.7,
-- create / find a policy_source row of subtype='operator_posted_policy'
-- (issuing_agency_id NULL; issuing_operator_id = operator_dogs_policy.operator_id)
-- then INSERT beach_policy_source rows for every beach_operator link.
```

Pseudo-code for the bridge:

```python
def bridge_operator_to_cascade(operator_id):
    odp = supa("/operator_dogs_policy?operator_id=eq.{operator_id}", limit=1)
    if not odp or odp[0]['pass_c_confidence'] < 0.7:
        return  # skip low-confidence
    rule = _map_default_rule_to_enum(odp[0]['default_rule'], odp[0]['leash_required'])
    ps = upsert_policy_source(
        subtype='operator_posted_policy',
        citation=odp[0].get('summary') or f"{operator.name} posted policy",
        issuing_operator_id=operator_id,
        source_url=odp[0]['source_url'],
        full_text=odp[0].get('summary'),
    )
    for beach_link in supa("/beach_operator?operator_id=eq.{operator_id}"):
        upsert_bps(
            beach_fid=beach_link['beach_fid'],
            policy_source_id=ps.id,
            section=beach_link.get('scope_section', 'sand'),
            rule=rule,
            evidence_verbatim=odp[0]['pass_a_quotes'][0],
            evidence_url=odp[0]['source_url'],
        )
    # Optionally: convert operator_policy_exceptions → beach_policy_source_temporal
```

Or — better — make it a SQL trigger on `operator_dogs_policy` so the bridge fires automatically on extraction. Per playbook tenet #5 design.

### Cascade integration

Same as Codify — `tg_stmt_ins_beach_policy_source` fires automatically on the bridge's bps INSERTs. No new cascade code needed. The 11 existing operator_posted_policy ps rows prove this works.

---

## 5. Quality gates (mirrors Codify §8)

```sql
-- (Op-1) Operator-coverage: every active beach in MVP+ states has at least
--        one bps OR is covered by codify
SELECT b.state, COUNT(*) FILTER (WHERE n_bps = 0) AS uncovered
FROM beaches_gold b
LEFT JOIN (SELECT beach_fid, COUNT(*) AS n_bps FROM beach_policy_source GROUP BY 1) c USING (fid)
WHERE b.state IN ('CA','OR','WA') AND b.is_active
GROUP BY b.state;

-- (Op-2) Operator URL deep-link discipline (analog to Codify URL gate)
--        operator_posted_policy can use page-level URLs (per playbook §8); enforce
--        that the URL isn't a bare domain.

-- (Op-3) Operator-vs-codify rule conflict surfaced
--        Where a beach has both operator AND codify ps rows w/ DIFFERENT rules,
--        verify the consensus engine picked the operator (per-entity override
--        principle from walkthrough-hbdb).
SELECT bps_op.beach_fid, g.name,
       bps_op.rule AS operator_rule, bps_codify.rule AS codify_rule,
       bdp.zone_rules->'regions'->0->'sections'->'sand'->>'rule' AS consensus_rule
FROM beach_policy_source bps_op
JOIN policy_source ps_op ON ps_op.id = bps_op.policy_source_id
  AND ps_op.subtype = 'operator_posted_policy'
JOIN beach_policy_source bps_codify ON bps_codify.beach_fid = bps_op.beach_fid
  AND bps_codify.section = bps_op.section
JOIN policy_source ps_codify ON ps_codify.id = bps_codify.policy_source_id
  AND ps_codify.subtype IN ('municipal_code','federal_regulation','state_regulation')
JOIN beach_dog_policy bdp ON bdp.arena_group_id = bps_op.beach_fid
JOIN beaches_gold g ON g.fid = bps_op.beach_fid
WHERE bps_op.rule != bps_codify.rule;
-- For each row, consensus_rule should match operator_rule (operator wins per-entity).

-- (Op-4) operator_dogs_policy → policy_source bridge coverage
--        How many extracted rows haven't been bridged yet?
SELECT COUNT(*) AS extracted_total,
       COUNT(*) FILTER (WHERE EXISTS (
         SELECT 1 FROM policy_source ps
         WHERE ps.issuing_operator_id = odp.operator_id
           AND ps.subtype = 'operator_posted_policy'
       )) AS bridged
FROM operator_dogs_policy odp
WHERE odp.pass_c_confidence >= 0.7;
-- Today's expected output: 0 bridged (everything queued).
```

---

## 6. CLI design (mirrors Codify v1)

Inspired by the patterns built today (`--state-agency-rule`, `--manual-url`, `--from-csv`, `--rescue`, `--list-federal-coverage`):

```
python scripts/derive_operator_posted_policy.py
  --discover-operators  --states OR,WA            # Phase A
  --discover-urls       --operator-ids 12,34,56   # Phase B
  --extract-from-url    --operator-id 99 --url <U>  # Phase C single
  --extract-state       --state OR --pilot 20     # Phase C batch
  --bridge-to-cascade   --operator-ids 12,34,56   # Phase D
  --bridge-all-extracted                          # Phase D backfill
  --list-coverage       --states OR,WA            # Discovery helper
  --rescue-from-jsonl   <path>                    # Rescue stubborn defers
  --operator-rule       --operator-id 12 --rule on_leash --url <U> --citation <C>
                                                  # Curator one-shot (analog of --state-agency-rule)
```

Outputs:
- `supabase/migrations/<date>_operate_<label>.sql` (per playbook §7)
- `tmp/operate_<label>_<ts>_outcomes.jsonl` (per-operator outcome tracker)
- `tmp/operate_<label>_<ts>_post_apply.sh` (temporal extractor invocation; reuses #2 from Codify)
- `tmp/operate_<label>_<ts>_REVIEW.sql` (curator-review queue; reuses #12 from Codify)

---

## 7. Migration plan from today's state

**Phase 0 — audit** (1-2 hours) — PARTIALLY DONE 2026-05-18:
1. ✓ Located + tested the bridge mechanism (`scripts/bridge_operator_to_cascade.py`)
2. ✓ Sampled `beach_operator` 3 rows: 1 HBDB (PSHDB→Dog Beach) + 2 Crystal Cove (Conservancy→Crystal Cove SB) — small but coherent
3. ✓ Audited 12 `operator_posted_policy` ps rows (see §2 audit table): 4 real + 8 cleanup candidates
4. ✗ Locate + verify existing 3-pass extractor script — still TODO
5. ✗ Per-operator audit of the 418 extractions vs existing ps rows — still TODO

**Phase 1 — bridge** — MOSTLY DONE, FINDING IS BIGGER THAN ANTICIPATED (2026-05-18):
1. ✓ Bridge script built (`scripts/bridge_operator_to_cascade.py`)
2. ✓ PSHDB curator-fixed migration applied (ps_id=287) → fid 6212 Dog Beach properly attributed; consensus already correct via pre-existing ps_id=20
3. ⚠ Mechanical bridge of 418 extractions is NOT the right next move — the audit revealed many extractions are corrupt + many operators already have ps rows. Phase 1 EVOLVES INTO:
   - 1a. **Cleanup** the 8 orphaned "Extractor"/"v1" ps rows (DELETE or label as deprecated)
   - 1b. **Backfill `issuing_operator_id`** on the 3 real-but-FK-missing ps rows (ps_20, ps_22, ps_233)
   - 1c. **Per-operator audit** of the 418 extractions: which are corrupt? which duplicate existing ps? which represent net-new operator coverage?
   - 1d. **Targeted curator-fixed bridges** like today's PSHDB pattern for net-new operators that have extractable policy

**Phase 2 — beach_operator backfill (5-10 hours):**
1. The 3-row `beach_operator` table remains the bottleneck for scaling Operate
2. Build a backfill from PAD-US `mng_name` + OSM `operator` tag + curator-fed CSV
3. Target: every active beach with a NAMED operator has ≥1 beach_operator link
4. Per-state batch via existing tools

**Phase 3 — dog-park Operate (10-20 hours):**
1. Add `dog_park_operator` (or extend `beach_operator` to `entity_operator`)
2. Run Phase B (URL discovery) + Phase C (extract) for the 6,163 OSM dog parks
3. Cost estimate: 6,163 × 3 LLM calls × $0.005 avg = ~$92 — affordable
4. Dog parks become the FIRST class where Operate is the PRIMARY codification (no codify alternative)

**Phase 4 — Codify Step 6.7 demotion:**
1. Step 6.7 (agency-policy fallback) was the seed for Operate; should become Operate-as-primary for entity-scoped queries
2. Refactor: codify defers to operate-pipeline for unit-specific overrides (e.g., NPS Compendiums move from Codify to Operate)
3. Today's Compendium ps rows (subtype=superintendents_compendium) can stay where they are; new ones go through Operate

---

## 8. Open questions for Franz

1. **Bridge mechanism: script or trigger?**
   - Script (Python, batch-driven, per-batch approval) — matches Codify v1 discipline; explicit per-bridge migrations
   - Trigger (SQL, auto-fires on operator_dogs_policy UPSERT) — magical, less auditable, harder to debug
   - **Recommend script** for parity with Codify v1 + auditability

2. **Naming: Operate vs Post?**
   - "Operate" alliterates w/ Codify + Cascade
   - "Post" describes what operators DO (post rules)
   - **Recommend Operate** per the [[operators-as-first-class]] suggestion

3. **Bridge confidence threshold:** pass_c_confidence >= 0.7? 0.5? 0.8?
   - Codify v1 uses 0.7 for auto_commit, 0.4-0.7 for human_review
   - **Recommend mirror: >= 0.7 auto-commit; 0.4-0.7 review queue; < 0.4 defer**

4. **Operator-vs-codify resolver:** does operator ALWAYS win per-entity?
   - Per walkthrough-hbdb yes (Rosie's overrides Long Beach leash)
   - Edge case: state law explicitly overrides operator (e.g., Endangered Species Act seasonal closure overrides operator's permissive rule)
   - **Recommend tiered: operator wins for entity-specific rules; state baseline wins for protected-species/health-safety overrides**

5. **Dog-park-only operator scope:** should we batch-run all 6,163 dog parks in v1?
   - Cost ~$92; wall time ~6-8 hours at 5 parallel agents
   - Many dog parks have no published policy (just a sign) — defers expected
   - **Recommend: start with WA + OR + CA dog parks only (~1,800?) for MVP+; defer rest until needed**

6. **HOA / private operator handling:** per [[deferred-canyon-lake]] AV-flagged sources are deferred
   - **Recommend: AV-flagged operators get an `operator.av_flagged=true` column; pipeline skips them by default; `--include-av-flagged` opt-in**

7. **Bridge artifact format:** SQL migrations per the codify pattern, or direct DB writes?
   - **Recommend SQL migrations + per-batch approval (matches today's discipline + auditable)**

---

## 9. What this spec does NOT cover

- Per-tribal Operate (deferred per Franz 2026-05-17)
- HOA-private operator codify (AV-flagged; per [[deferred-canyon-lake]])
- Operator photo / sign OCR (would need vision model integration; future)
- Operator-amenity-extraction (already exists per `operator_amenity_claims` table; separate concern)

---

## 10. Worked examples

### Example A — Cape Lookout SP leash override (OR)

Cape Lookout SP is operated by OPRD. The OAR 736-010-0030 baseline says strict 6-ft leash on park property; Cape Lookout's operator-posted policy (stateparks.oregon.gov/index.cfm?do=v.page&id=79) adds explicit "Pets must be on a leash on the beach in front of Cape Lookout State Park."

Operate flow:
1. **Phase A**: Cape Lookout SP is identified as an OPRD-operated entity (PAD-US: STAT/SPR/Cape Lookout State Park)
2. **Phase B**: URL = `https://stateparks.oregon.gov/index.cfm?do=v.page&id=79#cape-lookout` (or a per-park URL if it exists)
3. **Phase C**: extract rule=on_leash + spatial_zones=[Cape Lookout beach front] + confidence 0.95
4. **Phase D**: bridge → policy_source(subtype='operator_posted_policy', issuing_operator_id=<oprd op id>) + beach_policy_source for the Cape Lookout SP beach fids
5. **Cascade**: bps INSERT auto-fires; consensus engine sees operator_posted_policy + codified OAR 736-010-0030 + OAR 736-021-0070 (Ocean Shore); operator-posted wins for entity-specific override

### Example B — Rosie's Dog Beach (Long Beach CA)

Rosie's Dog Beach is operated by City of Long Beach Parks & Rec; posted off-leash. Long Beach has a citywide leash ordinance.

Operate flow:
1. **Phase A**: Rosie's identified as a Long Beach P&R-operated entity (OSM operator tag + city PAD-US)
2. **Phase B**: URL = `https://www.longbeach.gov/park/park-and-facilities/parks-centers-pier/dog-beach-zone/`
3. **Phase C**: extract rule=off_leash + spatial_zones=[Rosie's polygon] + posted-hours window
4. **Phase D**: bridge → policy_source(operator_posted_policy, issuing_operator_id=<long-beach-pnr>) + bps for Rosie's beach_fid
5. **Cascade**: operator_posted wins for Rosie's specifically; Long Beach citywide leash still applies to OTHER Long Beach beaches

### Example C — Henlopen Acres Block W Beach (DE; already in extraction data)

Per the 418 operator_dogs_policy rows: `https://henlopenacres.delaware.gov/files/2020/12/HAPOC-Rules-for-Use-of-the-Block-W-Beach.pdf` was extracted with default_rule=restricted + leash_required=true + pass_c_confidence=0.99. This data EXISTS but isn't bridged.

Operate Phase D bridge:
1. Read operator_dogs_policy row
2. Find or create operator row for "Henlopen Acres" (HOA / nonprofit type)
3. Find or create beach_operator row linking the HAPOC-Block-W beach (assuming beach_fid known)
4. Generate policy_source (subtype='operator_posted_policy', source_url=the PDF) + bps row
5. Cascade fires; Block W Beach's zone_rules now reflects the HAPOC rule

This single row demonstrates the bridge pattern that needs to scale to 418 rows.

---

## 11. Success metrics (when to call Operate "done v1")

REVISED 2026-05-18 evening after the audit:

- All **REAL** operator_posted_policy ps rows have `issuing_operator_id` FK populated (today: 1 of 4; target 4 of 4)
- 8 orphaned "Extractor/v1/Research" ps rows cleaned up (deleted or labeled deprecated)
- Per-operator audit of 418 extractions complete: classify clean / corrupt / duplicative
- Net-new operator coverage from curator-fixed bridges (target: 20-30 operators, primarily NPS Compendium-style + named-beach operators that today have ZERO ps rows)
- ≥ 90% of MVP+ state (CA/OR/WA) beaches have either a codify ps OR an operate ps (Phase 2)
- Dog parks in MVP+ states have ≥ 50% Operate coverage (Phase 3 — many won't have published rules)
- Quality gate Op-3 passes: when operator and codify disagree on a beach, consensus picks operator (per-entity override correctness)
- Quality gate Op-4 sharpened: NO orphan ps rows of subtype='operator_posted_policy' that are "extractor placeholders"

---

## 12. Related pins + docs

- [[operators-as-first-class]] — the parent insight
- [[dogpark-rules-are-operator-posted]] — dog parks are operator-primary
- [[codify-cascade-vocabulary]] — extends to three-track
- [[walkthrough-hbdb]] — concessionaire pattern
- [[walkthrough-crown-memorial]] — special district + MPA overlay
- [[walkthrough-fort-funston]] — superintendents_compendium pattern
- [[no-human-visibility-principle]] — operators are infrastructure, not user-surfaced
- [[deferred-canyon-lake]] — HOA / AV-flagged exclusion
- [[operator-not-pseudo-agency]] — operator ≠ agency; use delegated_authority_via FK
- [[never-solve-same-problem-twice]] — leverage existing 418-row extraction infrastructure
- [`jurisdiction_policy_source_playbook.md`](jurisdiction_policy_source_playbook.md) — Codify playbook this spec mirrors
- [`codify_pip_resolver_architecture.md`](codify_pip_resolver_architecture.md) — needs an Operate section after this spec is approved

---

## 13. Decision summary requested from Franz

Before implementation starts, confirm:
1. **Naming**: Operate (recommended) or Post?
2. **Bridge mechanism**: Python script + migration files (recommended) or SQL trigger?
3. **Phase 0 audit scope**: full audit of existing extractor before Phase 1, or skip-and-rebuild?
4. **Dog-park batch v1**: WA+OR+CA only (recommended) or full ~6,163?
5. **HOA / AV-flagged**: skip by default (recommended) or include w/ flag?
6. **Sequencing**: Phase 1 bridge first (fastest consumer-surface impact, recommended) or Phase 2 beach_operator backfill first?
7. **Per-batch approval discipline**: same as Codify (per-batch DB apply) or autonomous-ship-on-confidence?

After approvals, I'll spawn the build per the playbook discipline (one phase at a time + per-batch DB approvals + commit per phase).
