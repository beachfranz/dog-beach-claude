# CA codification — lessons for the state-agnostic v1 process

What 18 months of CA codify work (171 policy_source rows, 904 beach_policy_source rows, 141 temporal rows, 105 issuing agencies) tells us about how the state-agnostic Codify v1 should default. Sourced from `_snap_2026_05_18_ca_*` regression baselines.

Read this BEFORE building any new state-agnostic logic — these defaults are dataset-validated.

---

## 1. Subtype distribution — 91% is two subtypes

| Subtype | n_ps | % | avg beaches/ps | avg full_text len |
|---|---:|---:|---:|---:|
| **municipal_code** | 89 | 52% | 6 | 1,031 |
| **agency_administrative_policy** | 67 | 39% | 4 | 365 |
| federal_regulation | 7 | 4% | 12 | 832 |
| operator_posted_policy | 3 | 2% | 1 | 1,184 |
| special_district_ordinance | 2 | 1% | 4 | 2,025 |
| superintendents_compendium | 1 | <1% | 20 | — |
| tribal_code | 1 | <1% | 1 | — |
| mou | 1 | <1% | 1 | — |

**Implication for v1:** the city/county codify path (which produces `municipal_code` subtypes) and the state-agency-per-unit path (which produces `agency_administrative_policy` subtypes) are the two workstreams that deserve first-class scaffolding. Everything else is a long tail.

## 2. Per-jurisdiction depth — 89% of agencies have ONE ps row

| ps rows per agency | n_agencies |
|---|---:|
| **1 ps** | **93** |
| 2 ps | 7 |
| 3-5 ps | 4 |
| 11+ ps (CA DPR with 50) | 1 |

**Implication for v1:** Step 1 (`scope_check`) should default-expect `create_new`, not `supplement_existing`. Multi-row jurisdictions are rare and concentrated in big state agencies (CA DPR). For city/county codify, one ps row per agency is the norm.

## 3. Counties are the high-coverage workhorses

Top 10 ps rows by beach coverage (excluding the 36 CFR §2.15 federal baseline at 49 beaches):

| Citation | Beaches |
|---|---:|
| San Diego County Code §62.669+§62.668 | 43 |
| Sonoma County Code §20-8+§20-8.5 | 37 |
| Santa Barbara County Code §26-49+§26-49.1 | 35 |
| Santa Cruz County Code §6.12.020 | 33 |
| Monterey County Code Chapter 8.20 | 32 |
| San Mateo County Code Title 6 | 28 |
| Ventura County Code §4461+§4466 | 27 |
| Laguna Beach Municipal Code §6.16.010 | 25 |

**Implication for v1:** prioritize **counties** for first codification passes in each state — they yield 5-10× the beach attribution per unit of work vs cities. WA codify should hit Skagit/Jefferson/Snohomish/Kitsap/King counties FIRST, then drill into incorporated cities. Same for OR (Tillamook/Lincoln/Clatsop/Coos counties first).

## 4. Municode doc_slug distribution (CA)

35 CA Municode URLs:
- **`code_of_ordinances`** — 29 (83%)
- `municipal_code` — 4 (Long Beach, Marin County, Del Mar, Manhattan Beach pattern)
- `ordinance_code` — 1 (Contra Costa County)
- `code` — 1 (Shasta County)

**Implication for v1:** Codify v1's Municode builder should try **4 doc_slugs** in priority order: `code_of_ordinances` → `municipal_code` → `ordinance_code` → `code`. Current code only tries 2; expanding adds ~5% coverage without much cost.

## 5. County Municode slugs ALWAYS use `_county` suffix (CA: 100%)

Every county Municode URL in CA: `los_angeles_county`, `san_mateo_county`, `marin_county`, `contra_costa_county`, etc. No county was found at the bare-name slug.

**Implication for v1:** when classifying a `county`, Municode candidate should **prioritize** the `_county`-suffixed slug, not try both. Saves one Playwright call per county (~12s).

## 6. Page-level URL discipline — historical 40% miss rate

URL depth distribution (167 ps rows with non-null source_url):

| URL type | count | % |
|---|---:|---:|
| municode nodeId (deep) | 38 | 23% |
| parks.ca.gov page_id (deep) | 49 | 29% |
| **root/other (no deep link)** | **66** | **40%** |
| static .html | 8 | 5% |
| PDF | 4 | 2% |
| fragment anchor | 2 | 1% |
| qcode topic (deep) | 1 | <1% |

**Implication for v1:** the page-level > agency-level tenet (per playbook tenet 1) was violated in 40% of CA codification. New pipeline should **fail hard** on root/agency URLs at the quality-gate step — but expect that historical data has many of these. Regression check should not require the new pipeline to fix the old root URLs (they're already in DB); just don't produce new ones.

## 7. status_note is mostly empty or leash-detail

| Category | n_bps | % |
|---|---:|---:|
| (none) | 342 | 38% |
| leash detail (e.g., "6-foot leash") | 297 | 33% |
| other | 190 | 21% |
| seasonal carve-out | 42 | 5% |
| time-of-day | 10 | 1% |
| layered authority | 9 | 1% |
| posted-signage | 8 | <1% |
| service-dog exception | 5 | <1% |
| enforcement nuance | 1 | <1% |

**Implication for v1:** the rule-decision LLM (Step 6) should be encouraged to leave status_note minimal — leash-length detail or empty is the norm. Don't generate elaborate status_notes for the 70% of rows that don't need them. The "other" 21% bucket is a black-box; worth a follow-up audit.

## 8. region_name and section are rarely used

- `region_name` populated in 9 of 904 bps (1%) — sub-area encoding is genuinely an edge case
- `section`: 887 of 904 (98%) are `sand`; rest are `paved_walkways`, `sand_nps_overlay`, `bike_path`, `bank_swallow_closure`, `snowy_plover_protection_area`, etc.

**Implication for v1:** **default to `section='sand'` + `region_name=NULL`**. Sub-area encoding via region_name is an explicit decision the rule-decider should only make when the rule text explicitly narrows to a sub-area. Resolver handles multi-region beaches via Phase I logic already.

## 9. Rule + operative_status defaults

| rule | n | % |
|---|---:|---:|
| **on_leash** | 678 | 75% |
| not_allowed | 186 | 21% |
| off_leash_voice_control | 30 | 3% |
| off_leash | 10 | 1% |

| operative_status | n | % |
|---|---:|---:|
| **operative** | 898 | 99% |
| non_enforced | 3 | <1% |
| superseded_by_lower_tier | 3 | <1% |

**Implication for v1:** defaults are `rule='on_leash'` + `operative_status='operative'`. Off-leash and layered-authority overlays are the exceptions, not the norm. Step 6 should treat anything else as requiring explicit text justification.

## 10. Temporal extraction has ~6% hit rate

Of 171 CA ps rows, only 11 (6.4%) had any temporal carve-outs extracted by the H2 extractor. Concentrated in `municipal_code` and `agency_administrative_policy` subtypes.

**Implication for v1:** the temporal extractor (`extract_temporal_from_policy_source.py`) is already correctly filtered (it only fires on ps rows whose full_text contains temporal keywords). Codify v1 just needs to invoke it as Step §9.5 after new ps rows land; no architectural change.

---

## The "default ps row" — what the state-agnostic builder should produce by default

Combining the above:

```sql
INSERT INTO policy_source (
  subtype, citation, full_text, issuing_agency_id, scope, source_url
) VALUES (
  'municipal_code',                          -- or 'agency_administrative_policy' for state agencies
  '<Jurisdiction> <Code Type> §<section>',   -- canonical-citation format
  '<300-2000 char verbatim quote>',          -- sweet spot per the size distribution
  <agency_id_lookup>,
  ARRAY['dog_policy'],
  '<deep-linked URL with nodeId/section/page_id>'  -- FAIL hard if root/agency-level
);

INSERT INTO beach_policy_source (
  beach_fid, policy_source_id, section, rule, operative_status,
  evidence_verbatim, evidence_url, status_note, region_name
) VALUES (
  <fid>, <ps_id>,
  'sand',                    -- 98% default
  'on_leash',                -- 75% default
  'operative',               -- 99% default
  '<per-beach quote>',
  <ps.source_url>,
  NULL,                      -- 38% default; or short leash-length note
  NULL                       -- 99% default; only set for explicit sub-area
);
```

Everything outside this default is an explicit decision the rule-decider should justify with the verbatim text.

---

## Where CA results inform priorities, not just defaults

1. **Codify counties first** in each new state — best return per unit of effort.
2. **State-agency-per-unit pattern** (CA DPR with 50 ps rows) is its own workstream — needs explicit support for WSPRC (WA), OPRD (OR), MA DCR, etc. Per-park URLs analogous to `parks.ca.gov/?page_id=N`.
3. **Federal regulations** (36 CFR §2.15) carry the highest avg beaches-per-ps (12) because they're true blanket regulations — codify federal early in each state to set the baseline.
4. **Page-level URL discipline** is something the new pipeline should enforce harder than the historical CA work did (40% root-URL rate is the floor we're trying to improve on).
5. **Don't over-engineer status_note** — 70% of rows have minimal or none.

---

## Related

- `_snap_2026_05_18_ca_*` regression baseline tables (kept in prod DB)
- `docs/codify_pip_resolver_architecture.md` — the state-agnostic architecture this informs
- `docs/codify_cascade_v1_runbook.md` — original runbook (Track 1 sub-task defaults should be revised per these findings)
- `docs/jurisdiction_policy_source_playbook.md` v3 — algorithm
- [[codify-v1-governance-aware-design]] — what dissolved + what stayed
- [[url-resolution-field-guide]] — per-platform URL anatomy
