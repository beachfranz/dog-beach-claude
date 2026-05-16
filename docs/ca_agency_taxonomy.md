# CA Agency Taxonomy — v0 Draft

**Purpose:** Inventory of the agencies that govern dog policy on
California tier-1+2 beaches (~300), in support of the
**law-as-primary-source** initiative ([[law-as-primary-source-ca]]).

**Scope of this document:** taxonomy validation, not schema. We
enumerate agency *types* with their authority domains, PIP-layer
cross-references, and typical statute sources. Instances per type
(e.g., the ~50 CA cities with beaches) are listed where they matter
for sequencing the agency-type waves; full instance population
happens in migration + seed work.

**Not in scope:** Operator entities, statute entity rows, the
`beach_agency` / `beach_operator` join tables. Those come after the
taxonomy is reviewed.

---

## Agency types

Each row: type → typical authority domain(s) → PIP polygon source →
statute source pattern → instance count (CA).

| # | Type | Authority domains | PIP layer | Statute source | CA instances |
|---|------|-------------------|-----------|----------------|---|
| A | **CA State Parks** (DPR) | `dog_policy`, `operations`, `parking`, `fire` | `pad_us` (state park unit polygons) | **CCR Title 14** §4319 (dogs in units) | 1 agency, ~30 coastal park units |
| B | **CA State Lands Commission** | `tidelands`, `dog_policy` (below MHW) | `cslc_sovereign_lands` (or PAD-US sovereign-lands) | **Public Resources Code §6001+**, CCR Title 2 | 1 agency, statewide |
| C | **CA Coastal Commission** | `coastal_access` (rarely `dog_policy`) | coastal-zone polygon | Coastal Act, **PRC §30000+** | 1 agency, statewide. Mostly access not policy. |
| D | **CA Dept of Fish and Wildlife** | `wildlife_protection`, sometimes `dog_policy` in reserves/MPAs | `cdfw_mpa`, `cdfw_ecological_reserves` | **CCR Title 14** §630, §632 | 1 agency, ~125 MPAs statewide |
| E | **County health department** | `water_quality`, `bacteria_advisories` | `counties` (county boundary, applied to coastal frontage) | County health code, **CA Health & Safety Code §115880+** (AB 411) | 16 coastal counties |
| F | **County parks / beaches department** | `dog_policy`, `operations`, `parking` (for county-managed beaches) | `county_parks` (sub-layer of `counties` or its own PIP) | County code (per county) | ~10 (not all counties manage beaches directly) |
| G | **LA County Dept of Beaches & Harbors** | `dog_policy`, `operations` | `lacounty_beaches_harbors` | **LA County Code Title 17** | 1 agency, ~30 LA County beaches |
| H | **City parks / recreation department** | `dog_policy`, `operations`, `parking` (incorporated city beaches) | `jurisdictions` (city polygon) | Municipal code (ecode360 / municode / amlegal / codepublishing.com / city's own) | ~50 incorporated CA cities with beaches |
| I | **NPS** (National Park Service) | `dog_policy`, `wildlife_protection` | `nps_units` (subset of `pad_us`) | **36 CFR §2.15** + unit-specific Superintendent's Compendium | 5-6 CA units with beach (Point Reyes, GGNRA, Channel Islands NP, Cabrillo NM, Santa Monica Mountains NRA, Pt Pinole) |
| J | **USFWS** (Fish & Wildlife Service) | `wildlife_protection`, `dog_policy` (often prohibited) | `usfws_refuges` | **50 CFR §26.21** | ~5 CA refuges with beach (Don Edwards, Salinas River, Humboldt Bay, San Pablo Bay, Sacramento) |
| K | **BLM** | `dog_policy`, `operations` (rare) | `blm_units` (subset of `pad_us`) | **43 CFR §8365** | 1 major (King Range NCA / Lost Coast) + scattered |
| L | **USFS** | `dog_policy` (usually permissive) | `usfs_units` (subset of `pad_us`) | **36 CFR §261** | Limited coastal (Los Padres NF, Six Rivers NF, Klamath NF) |
| M | **DoD** (Navy, USMC, Coast Guard) | `access_restriction` (typically closed) | `military_lands` | DoD regs, base-specific | Camp Pendleton, Vandenberg, Naval Base Coronado, MCRD SD, Pt Mugu, Pt Loma |
| N | **NOAA** (National Marine Sanctuaries) | `water_quality`, `wildlife_protection` (offshore — rarely dog_policy) | `noaa_sanctuaries` | **15 CFR §922** | 4 CA sanctuaries (Monterey Bay, Channel Islands, Greater Farallones, Cordell Bank) |
| O | **Tribal** | `dog_policy`, `access_restriction` | `tribal_lands` | Tribal code | ~5 CA tribal coastal areas |
| P | **Special districts** (regional park / lifeguard / harbor) | varies | `special_districts` (custom) | District-specific | East Bay Regional Park District (Crown Beach, Pt Isabel), Monterey Peninsula Regional Park District, Marin County Open Space, Mendocino Land Trust, etc. |

**Total agency *types*: 16.** Total **instances** (rows that'd land
in the `agency` table): roughly **80-100** for full CA tier-1+2
coverage.

---

## Wave sequencing — which types unlock which beaches

Maps the [[law-as-primary-source-ca]] wave plan to taxonomy rows:

| Wave | Agency types in play | Beaches unlocked (est) |
|------|---------------------|---------------------|
| 1 | B (State Lands) | All coastal — adds the tideland citation layer beneath every beach. Doesn't fully *cite* any beach alone but is foundational. |
| 2 | A (State Parks) | ~30 CA state beach units (Pfeiffer, Garrapata, Asilomar, Half Moon Bay SB, Doheny, San Onofre, Crystal Cove, Will Rogers, Robert H. Meyer, etc.) |
| 3 | G (LA Beaches & Harbors) + E (LA County Health) + H (City of LA, City of Santa Monica, Manhattan/Hermosa/Redondo/Long Beach) | ~80 LA-region beaches |
| 4a | E (OC Health) + H (Huntington, Newport, Laguna, Dana Point, San Clemente) + F (OC Parks) | ~30 OC beaches |
| 4b | E (SD Health) + H (Oceanside, Carlsbad, Encinitas, Solana Beach, Del Mar, SD, Coronado, Imperial Beach) | ~25 SD beaches |
| 4c | E + H for Ventura, SB, SLO, Monterey, Santa Cruz, San Mateo, Marin, Sonoma, Mendocino, Humboldt, Del Norte counties + their cities | ~80 beaches |
| 5 | I, J, K, L, M, N (federal) + O (tribal) | ~30 federally-managed beaches (Point Reyes, GGNRA, Channel Islands, Camp Pendleton-adjacent) |

**Wave totals** sum to ~275 — close enough to 300 that the last 25
are likely operator-only with tier-2 designation, addressable
beach-by-beach in cleanup.

---

## Sample agency rows (illustrative — for schema validation)

What instance rows would look like in the proposed `agency` table:

```yaml
- name: "California Department of Parks and Recreation"
  short_name: "CA State Parks"
  type: "state"
  hierarchy: ["California"]
  authority_domains: ["dog_policy", "operations", "parking", "fire"]
  web_url: "https://www.parks.ca.gov"
  code_archive_url: "https://govt.westlaw.com/calregs/Browse/Home/California/CaliforniaCodeofRegulations?guid=I12B8B5605C5D44109842EFA5081DDDFA"  # CCR Title 14
  primary_statute_citation: "CCR Title 14 §4319"
  pip_layer: "pad_us"
  pip_filter: "owner_type = 'STATE' AND manager LIKE '%Parks and Recreation%'"

- name: "California State Lands Commission"
  short_name: "CA State Lands"
  type: "state"
  hierarchy: ["California"]
  authority_domains: ["tidelands", "submerged_lands"]
  web_url: "https://www.slc.ca.gov"
  code_archive_url: "https://leginfo.legislature.ca.gov/faces/codesTOCSelected.xhtml?tocCode=PRC"
  primary_statute_citation: "PRC §6001+"
  pip_layer: "cslc_sovereign_lands"  # may need to add this layer
  pip_filter: null

- name: "City of Santa Monica"
  short_name: "Santa Monica"
  type: "city"
  hierarchy: ["California", "Los Angeles County", "Santa Monica"]
  authority_domains: ["dog_policy", "operations", "parking"]
  web_url: "https://www.santamonica.gov"
  code_archive_url: "https://library.qcode.us/lib/santa_monica_ca/pub/municipal_code"
  primary_statute_citation: "SMMC 4.04.040"  # leash law
  pip_layer: "jurisdictions"
  pip_filter: "place_name = 'Santa Monica'"

- name: "Los Angeles County Department of Beaches and Harbors"
  short_name: "LA County Beaches"
  type: "county_department"
  hierarchy: ["California", "Los Angeles County"]
  authority_domains: ["dog_policy", "operations"]
  web_url: "https://beaches.lacounty.gov"
  code_archive_url: "https://library.municode.com/ca/los_angeles_county"
  primary_statute_citation: "LA County Code Title 17"
  pip_layer: "lacounty_beaches_harbors"  # may need to add this layer
  pip_filter: null

- name: "Point Reyes National Seashore"
  short_name: "Point Reyes NS"
  type: "federal_unit"
  hierarchy: ["United States", "National Park Service", "Point Reyes National Seashore"]
  authority_domains: ["dog_policy", "wildlife_protection"]
  web_url: "https://www.nps.gov/pore"
  code_archive_url: "https://www.ecfr.gov/current/title-36"
  primary_statute_citation: "36 CFR §2.15 + Superintendent's Compendium 2024"
  pip_layer: "pad_us"
  pip_filter: "unit_nm = 'Point Reyes National Seashore'"
```

---

## Cross-references — PIP layers expected vs new

| PIP layer | Status today (per [[pipeline-instantiation]]) | New layer needed? |
|-----------|-----------------------------------------------|-------------------|
| `pad_us` | ✅ live | no |
| `cpad` | ✅ live (CA-specific) | no (covers state parks + some) |
| `jurisdictions` | ✅ live (cities) | no |
| `counties` | ✅ live | no |
| `military_lands` | ✅ live | no |
| `tribal_lands` | ✅ live | no |
| `cdfw_mpa` | unknown — likely needed | **possibly add** |
| `cdfw_ecological_reserves` | unknown | **possibly add** |
| `usfws_refuges` | check | **possibly add** |
| `nps_units` | derivable from `pad_us` | no, derivable |
| `noaa_sanctuaries` | unknown | **add if NOAA waves matter for dog_policy (likely not)** |
| `cslc_sovereign_lands` | unknown | **add — important for tidelands** |
| `lacounty_beaches_harbors` | unknown | **possibly add** |
| `regional_park_districts` (EBRPD, MPRPD, etc.) | unknown | **possibly add** |

**Action item:** before Wave 1, audit which PIP layers exist in the
pipeline today vs what this taxonomy assumes. Gaps (likely 3-5
layers) need to be added to PIP first.

---

## Open questions — to resolve before schema

1. **State Lands tideland boundary**: do we model wet sand / dry
   sand as separate `zone_rules` sections, or just attribute the
   tideland authority at the beach level and let State Lands appear
   in the stack on every coastal beach? (Earlier session: chose
   beach-level. Confirming.)
2. **County health vs county parks**: do we have one `agency` row
   per *department*, or one per *county* with multiple
   authority_domains? Recommend per-department because their
   statutes live in different code titles and their web URLs
   differ.
3. **City + city parks dept**: same question. Recommend one
   `agency` row for the city as a whole (with multiple
   authority_domains), since the municipal code is single-source
   and the parks dept inherits authority from city ordinances.
4. **Operator vs agency overlap**: when City of Santa Monica is
   BOTH the agency (authority via SMMC) AND the operator
   (day-to-day management), we cross-reference via
   `operator.agency_id`. No duplication. Confirmed.
5. **Federal Superintendent's Compendium**: each NPS unit's dog
   policy is in its Compendium, not the federal CFR. Statute
   entries for NPS units cite the Compendium (with a CFR §2.15
   parent). Need a `parent_citation` field on `statute`.

---

## Reviewability

This is a v0 — review punch list before cutting schema migrations:

- [ ] PIP layer audit (existing vs needed)
- [ ] Confirm agency types A-P match real CA jurisdictional reality
      (any obvious omission?)
- [ ] Wave sequencing realistic given engineering capacity?
- [ ] Open questions 1-5 resolved
- [ ] Sample agency rows pass review with someone who knows CA
      jurisdiction better than me
- [ ] Re-verification cadence agreed (annual default proposed)

---

## Related

- [[law-as-primary-source-ca]] — strategy + decisions
- [[pipeline-instantiation]] — current PIP layer state
- [[governance-resolver-audit]] — 2026-05-12 CPAD/tiger_places audit
- [[scoring-scope]] — tier-1+2 definition
- [[no-human-visibility-principle]] — agencies surface as authority sources, not as social entities
- [[specialist-llm-architecture]] — law extractor as a future specialist
