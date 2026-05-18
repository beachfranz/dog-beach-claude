# Codify WA — audit + CA→WA deltas

Light-touch audit of WA's current `policy_source` coverage to surface what
Codify v1 must handle differently than CA. **No new migrations**; analysis
only. Per the 2026-05-17 evening conversation: training exercise to stretch
the playbook before Codify v1 ships.

Run date: 2026-05-17 evening.

---

## 1. Coverage snapshot

| metric | value |
|---|---:|
| Total active WA beaches | 429 |
| Beaches with ≥1 ps link | 429 (100%) |
| Distinct ps rows touching WA | **19** |
| Beaches per ps row (avg) | ~22 |

For comparison: CA = 200+ ps / 744 beaches (~3–4/ps); OR = 6 ps / 152 beaches (~25/ps; flagged in [[or-homogeneity-gap]]).

**Verdict:** WA is closer to OR's homogeneity gap than CA's nuanced coverage. The 100%-linked headline masks substantive thinness.

### The 19 ps rows

| ps_id | subtype | citation | beaches |
|---:|---|---|---:|
| 173 | state_regulation | **WAC §352-32-060 (Pets and Other Animals)** | 429 |
| 118 | municipal_code | King County §7.12.225 (Pet animals) | 53 |
| 119 | municipal_code | Island County §6.08.090 (Control off premises) | 49 |
| 120 | municipal_code | Kitsap County §10.12.070 (Animals in playfields or parks) | 45 |
| 121 | municipal_code | San Juan County §12.08.220 (Animals running at large) | 44 |
| 122 | municipal_code | Jefferson County §6.07.060 (Animal at large) | 35 |
| 174 | municipal_code | Seattle SMC §18.12.080 (Pets in parks) | 33 |
| 123 | municipal_code | Pierce County §14.08.070 (Animals) | 33 |
| 124 | municipal_code | Clallam County §23.03.070 (Animals) | 33 |
| 125 | municipal_code | Skagit County §9.41 (Parks and Recreation Code — Animals) | 24 |
| 126 | municipal_code | Snohomish County §9.14.030 (Dogs off premises to be on a leash) | 20 |
| 127 | municipal_code | Mason County §4.08.050 (Animals at large) | 13 |
| 128 | municipal_code | Whatcom County §6.04 (Animal Control) | 13 |
| 134 | municipal_code | Pacific County Ordinance No. 36 (Leash Law) | 10 |
| 129 | municipal_code | Chelan County §7.24.010(3) (Animals — Park Regulations) | 9 |
| 130 | municipal_code | Clark County §8.15.020 (Dogs off premises to be on leash) | 6 |
| 131 | municipal_code | Thurston County §9.10.050(C) (Unleashed pet animal on public property) | 5 |
| 132 | municipal_code | Lewis County §12.05.110 (Animals — County Park Use) | 2 |
| 133 | municipal_code | Spokane County §5.04.070 (Control of dogs) | 1 |

= 1 statewide WAC + 17 county codes + 1 city (Seattle).

### What's MISSING (the codify scope)

- **84 WA coastal cities/CDPs have ≥1 beach with NO city-level ps row.** Of those, ~15 are incorporated cities worth codifying (Bainbridge Island, Oak Harbor, Edmonds, Port Townsend, Bremerton, Gig Harbor, Tacoma, Vancouver-WA, Everett, Washougal, Anacortes, Westport, Bellingham, Burien, Leavenworth). The rest are CDPs already covered by county code.
- **Zero federal ps rows for WA** — Olympic NP coastal strip (~73 miles), Olympic Coast National Marine Sanctuary, NWRs (Dungeness, Nisqually, Protection Island, +others), USFS Olympic NF coastal portion, USCG bases. Same federal gap CA had pre–Bucket D.
- **Zero state-park per-park overrides** — CA DPR has 30+ per-park `?page_id=N` rows; WSPRC has 0. Some WA state parks (e.g., Cape Disappointment with its NWR/lighthouse complex) may warrant per-park entries.
- **Zero tribal codes** (deferred 2026-05-17 per Franz; Quinault / Makah / Quileute / Hoh / Lummi / Suquamish / Tulalip is a meaningful share of WA's outer coast).
- **Zero port-district / special-district codes** — Port of Seattle, Port of Tacoma, Port of Bellingham, Port of Port Townsend likely operate beachfront and have their own rules.

---

## 2. CA→WA pattern shifts that matter for Codify v1

### Shift 1 — Parks-not-Animals title

EVERY WA county code we cited puts dog rules in a **Parks** or **Recreation** title, not an Animals title:

- Skagit §9.41 ("Parks and Recreation Code — Animals")
- Chelan §7.24.010(3) ("Animals — Park Regulations")
- Lewis §12.05.110 ("Animals — County Park Use")
- King §7.12.225 ("Pet animals" — Title 7 is "Health and Welfare" but pets section sits inside it)
- Kitsap §10.12.070 ("Animals in playfields or parks")

The CA playbook §3 navigation tries `TIT<N>AN` / `CH<N>AN` first. **Those patterns miss every WA county.** The discovery step needs a second pass that greps `PARK|REC|PUBLIC|HEALTH` titles for dog/pet/animal sub-sections.

**Codify v1 fix:** add a per-state platform/title heuristic table. WA defaults to Parks-first. Or smarter: probe the TOC for ANY nodeId whose name contains animal-related abbreviations (`AN|ANIMAL|PET|DOG|LEASH`) regardless of parent title.

### Shift 2 — Unit of analysis is COUNTY, not CITY

WA's coastline is largely unincorporated. Camano (12 beaches), Vashon (8), Brinnon (5), Hansville (4), Birch Bay (3), etc. are all CDPs governed entirely by county code. CA's fragmented-city pattern doesn't apply.

**Codify v1 fix:** for a beach with no incorporated-city polygon, skip city-discovery entirely (don't waste an LLM call). The county-code row already covers it. Surface as `coverage=county_baseline_only` in the report.

### Shift 3 — Statewide-baseline pattern (same as OR)

WAC §352-32-060 covers ALL state-park beaches. It's already in DB (ps 173, 429 beaches). Similar shape to OR's OAR §736-021-0070.

**Codify v1 fix:** explicit "is this state-park-administered? If yes, baseline already exists — only codify per-park IF there's a posted carve-out (per-park URL with override text)." Don't blanket-attribute when the WAC already covers.

Note the parallel: CA DPR has per-park `?page_id=N` for each unit with potential carve-outs. WSPRC doesn't — all parks fall under the single WAC. Either:
(a) WSPRC genuinely has no per-park overrides — WAC is the truth; OR
(b) WSPRC has per-park rules buried in posted-signage / park brochures that aren't online. Field research needed for sample parks (Cape Disappointment, Deception Pass, Olympic).

### Shift 4 — Idiosyncratic platforms more common

CA's URL platform priority is Municode > amlegal > qcode > ecode360 > codepublishing > county.codes. WA's actual platform distribution:

| Platform | WA jurisdictions (today) |
|---|---|
| codepublishing.com | Kitsap, San Juan, Jefferson, Skagit, Chelan, Clark, Snohomish, Lewis, Whatcom, Thurston, Spokane (most!) |
| Municode | Island, Mason, Pierce, Clallam (some) |
| King County clerk portal | `aqua.kingcounty.gov/council/clerk/code/10_Title_7.htm` (idiosyncratic) |
| Raw PDF | Pacific County (Ordinance 36 hosted as PDF) |
| Seattle | likely codepublishing OR municipal-code-hosted |

**Codify v1 fix:** WA's platform priority should be `codepublishing → Municode → clerk-portal → raw-PDF`. Today's playbook tries Municode first (CA-default), which would 404 on most WA counties. Either:
(a) per-state default priority table (add to playbook §2), OR
(b) probe all platforms in parallel and take the first valid response.

### Shift 5 — Federal gap is 100% in WA

CA had Bucket D today: USFWS Don Edwards, CDWR Castaic, BLM King Range, DoD San Onofre. WA has NONE.

**Codify v1 scope:** WA federal pass should at minimum cover:
- **Olympic NP coastal strip** (NPS) — `nps.gov/olym/planyourvisit/pets.htm` + Compendium
- **Olympic Coast National Marine Sanctuary** (NOAA) — `olympiccoast.noaa.gov` (rules for tide-pool / wildlife conduct)
- **USFWS coastal refuges** — Dungeness, Nisqually, Protection Island, Quillayute Needles, Copalis, Flattery Rocks — most prohibit dogs
- **Cape Disappointment SP + Lewis and Clark NHP** boundary — overlap between WSPRC + NPS
- **USFS Olympic NF** coastal sub-units

**Codify v1 fix:** the federal-discovery pattern from CA Bucket D (NPS Compendium PDFs + Federal Register supplementary rules + USFWS per-refuge pages) is reusable; just point it at the WA-specific URLs.

### Shift 6 — Tribal coastline (deferred but flagged)

WA outer coast is materially tribal:
- Quinault Indian Nation (Quinault Reservation coast)
- Makah Tribe (Cape Flattery / Neah Bay)
- Quileute Tribe (La Push)
- Hoh Tribe (Hoh Reservation)
- Lummi Nation (Lummi Peninsula)
- Suquamish Tribe (Port Madison Reservation)
- Tulalip Tribes (Tulalip Bay area)
- + others around Puget Sound

Deferred per Franz 2026-05-17. When reopened, expect:
- Heterogeneous publication channels (most tribes don't use commercial codifiers)
- Some codes via codepublishing or amlegal (varies by tribe)
- CPRA-equivalent (tribal council records request) for some
- Operator-posted policies on tribal-tourism / parks pages as a partial path

---

## 3. Implications for Codify v1 (the runbook in `docs/codify_cascade_v1_runbook.md`)

| Track 1 step | WA-specific extension |
|---|---|
| §2 Discover platform | Per-state default platform priority table (WA: codepublishing-first; CA: Municode-first). Or probe-all-in-parallel. |
| §3 Navigate to operative chapter | TOC scrape should match `AN|ANIMAL|PET|DOG|LEASH` anywhere in the nodeId NAME, not just under Animals titles. Add Parks-title fallback. |
| §5 Map beaches | Detect unincorporated CDPs — skip city-discovery; mark coverage as `county_baseline_only`. |
| §6 Decide rule | When the source is a Parks-title sub-section, the rule applies to **county-operated park beaches only**, not all beaches in the county. Captures vs. CA where Animals-title is jurisdictional-wide. Need to validate per-beach whether the beach is a county park or unrelated public access. |
| §9 Commit + apply | No WA-specific changes |

---

## 4. Implications for the URL field guide

Already captured today: encodeplus, escribemeetings, Federal Register supplementary rules, truststore Python idiom. WA-specific additions warranted:

- **codepublishing.com WA jurisdiction list** (the current 11 confirmed) + the URL pattern note (WA tends to be `www.codepublishing.com/WA/<Jurisdiction>/html/<Jurisdiction><TITLE>/<chapter>.html`)
- **King County clerk portal** explicit entry — `aqua.kingcounty.gov` is non-standard; document fetch quirks (looks like static HTML; should grep for `Title 7 - Health and Welfare` to find the dog section)
- **WAC index** for the statewide-baseline pattern — `app.leg.wa.gov/wac/default.aspx?cite=352-32-060` (already in the field guide)
- **WSPRC per-park** — if/when posted rules are discovered, document the URL pattern (`parks.wa.gov/Parks/<slug>` may have posted rules)

---

## 5. Next-session work targets (when Codify v1 ships)

### Tier 1 — incorporated cities (the highest-leverage WA codify backfill)

15 incorporated WA cities with ≥2 beaches and NO city ps row. Expected effort: ~3-4 hours with parallel agents (mirror today's Wave 3.5 pattern):

| city | beaches | likely platform |
|---|---:|---|
| Bainbridge Island | 9 | codepublishing or Municode |
| Oak Harbor | 5 | codepublishing |
| Edmonds | 4 | codepublishing |
| Port Townsend | 4 | codepublishing |
| Bremerton | 4 | codepublishing |
| Gig Harbor | 3 | codepublishing |
| Tacoma | 3 | codepublishing |
| Vancouver (WA) | 3 | Municode? |
| Everett | 3 | codepublishing |
| Washougal | 2 | codepublishing |
| Anacortes | 2 | codepublishing |
| Westport | 2 | codepublishing or PDF |
| Bellingham | 2 | codepublishing |
| Burien | 2 | codepublishing |
| Leavenworth | 2 | codepublishing |

### Tier 2 — federal lands

| Authority | Expected coverage |
|---|---|
| Olympic NP (NPS) | coastal strip — 73-mile wilderness shore |
| Olympic Coast NMS (NOAA) | coastal waters + intertidal rules |
| Dungeness / Nisqually / Protection Island NWRs (USFWS) | per-refuge no-dogs |
| Cape Disappointment / Lewis & Clark NHP | NPS+WSPRC overlap |
| USFS Olympic NF coastal portion | rare; verify |

### Tier 3 — state parks (per-park overrides)

Only IF posted-rule reconnaissance finds carve-outs. Otherwise WAC 352-32-060 is sufficient. ~3-5 candidate parks to investigate (Cape Disappointment, Deception Pass, Olympic State Parks, Fort Worden, Lime Kiln).

### Tier 4 — port districts (special districts)

Port of Seattle, Port of Tacoma, Port of Bellingham, Port of Port Townsend, Port of Bremerton — most operate beachfront / shoreline with their own posted rules.

### Tier 5 — tribal (DEFERRED per Franz 2026-05-17)

Quinault / Makah / Quileute / Hoh / Lummi / Suquamish / Tulalip + others. Re-open when bandwidth + tribal-relations approach decided.

---

## 6. Open questions / decisions for Franz

1. **Per-state platform priority** — bake into Codify v1 as a config table (`platform_priority_by_state`), or probe all in parallel and take first-valid?
2. **Parks-title heuristic** — search for `AN|ANIMAL|PET|DOG|LEASH` anywhere in TOC, OR keep Animals-title priority but add Parks-title fallback?
3. **WAC + statewide-baseline pattern** — when codifying a state-park beach, should the per-park codify step explicitly check for posted-rule overrides, or assume WAC is sufficient and only revisit when a counter-example surfaces?
4. **CDP detection** — skip city-discovery for unincorporated CDPs? (Saves time + LLM cost; risks missing a CDP that has a special-district code.)
5. **Tier 2 federal scope for first WA codify pass** — full federal coverage in one batch, or NPS-only first (Olympic NP is the dominant case)?

---

## 7. Related

- [[or-homogeneity-gap]] — the parallel finding that triggered this audit
- [[wave4-wa-complete-2026-05-16]] — what Wave 4 actually shipped
- [[codify-cascade-vocabulary]] — naming
- `docs/codify_cascade_v1_runbook.md` — the v1 spec WA findings feed into
- `docs/jurisdiction_policy_source_playbook.md` v3 — the playbook WA findings revise
- `feedback_url_resolution_field_guide.md` — the field guide WA-specific additions warrant
