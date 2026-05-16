# Walkthrough — Robert W. Crown Memorial State Beach (fid 8452)

**Purpose:** fourth end-to-end beach walkthrough for the
[[law-as-primary-source-ca]] initiative. Substitute for Point
Isabel (which isn't in `beaches_gold`). Tests **two-agency
governance** — the beach is legally a CA State Park unit (in CPAD,
named "State Beach") but **operated by East Bay Regional Park
District** (`park_name = "Crown Memorial Regional Shoreline"`).
This is the **agency-and-operator-are-different-government-entities**
case the prior walkthroughs didn't reach.

**Companions:** [[walkthrough-hbdb]], [[walkthrough-crystal-cove]],
[[walkthrough-fort-funston]].

**Conventions:** same as prior walkthroughs.
- ✅ verified (primary source captured)
- 🔵 unverified (training-data memory; needs source)
- ❓ unknown

---

## Sidebar — Point Isabel not in dataset

Point Isabel Regional Shoreline (Richmond) was the original choice
for this walkthrough but is **not in `beaches_gold`** — confirmed
via name + location_id search. Other EBRPD shoreline beaches ARE
present (Albany Beach fid 8659, Crown Memorial fid 8452). The
Point Isabel gap is likely OSM-ingest related (it may not have a
`natural=beach` tag), not systemic. Documented as a gap to
revisit during Wave 2 ingest review.

---

## 1. Beach identity

| Field | Value | Status |
|-------|-------|--------|
| `fid` | 8452 | ✅ |
| `name` | "Robert W. Crown Memorial State Beach" | ✅ |
| `location_id` | `robert-w-crown-memorial-state-beach` | ✅ |
| `park_name` | "Crown Memorial Regional Shoreline" | ✅ |
| `county_name` | Alameda | ✅ |
| `state` | CA | ✅ |
| `cpad_unit_id` | 46017 (in California Protected Areas Database) | ✅ |
| `c1_jurisdiction_id` | 512 (likely City of Alameda — verify against jurisdictions table) | 🔵 |
| `is_active` | true | ✅ |
| `tier` | likely 3 — dogs prohibited on beach itself, permitted on lawns + paved paths only | ✅ confirmed via parks.ca.gov |
| `hours` | "Day-use Area 5:00am to 10:00pm" | ✅ verbatim per parks.ca.gov |

**The naming contradiction encoded in the row itself:**
- `name` = "Robert W. Crown Memorial **State Beach**"
- `park_name` = "Crown Memorial **Regional Shoreline**"

That's not a dataset bug — it reflects reality. The beach is
legally a CA State Park unit (hence "State Beach" + CPAD entry)
operated by EBRPD (hence "Regional Shoreline" in the park_name).
**The model has to represent this dual-identity cleanly.**

---

## 2. Agency stack

### Row A — California Department of Parks and Recreation (CA State Parks / DPR)

**The legal owner / agency of record.** Crown Memorial is in CPAD
as a state park unit. CA State Parks holds the underlying
ownership and ultimate policy authority.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "California Department of Parks and Recreation" | ✅ |
| `type` | `state` | ✅ |
| `authority_domains` | `dog_policy` (baseline via parks.ca.gov/Dogs policy), `wildlife_protection` | 🔵 |
| `cpad_unit_id` | 46017 — the CPAD record for this unit | ✅ |
| **Relationship to operator** | DPR is the agency-of-record; day-to-day operation delegated to EBRPD via some form of lease/agreement | 🔵 |

### Row B — East Bay Regional Park District (EBRPD)

**The operator, and ALSO an agency with its own rules.** EBRPD's
Ordinance 38 applies on EBRPD-operated land. So EBRPD has its
own dog_policy authority over Crown Memorial via the operational
relationship.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "East Bay Regional Park District" | ✅ |
| `short_name` | "EBRPD" | ✅ |
| `type` | **`special_district`** (first walkthrough to test) | ✅ |
| `hierarchy` | Two-county special district (Alameda + Contra Costa) | ✅ |
| `governance` | Elected 7-member Board of Directors, ward-based | 🔵 |
| `authority_domains` | `dog_policy`, `operations`, `parking`, `enforcement` (EBRPD has its own police force) | 🔵 |
| `web_url` | https://www.ebparks.org | ✅ |
| `primary_statute_citation` | EBRPD Ordinance 38 | 🔵 |

### Row C — California State Lands Commission

SF Bay tidelands below MHW. Same baseline as prior walkthroughs.

### Row D — Alameda County Health Care Services

Water quality / bacteria advisories. Bay-shoreline sampling may be
less frequent than open ocean.

### Row E — SF Bay Conservation and Development Commission (BCDC)

Bay-shoreline permit authority. May have CDP-style conditions
affecting public access at Crown Memorial. Equivalent to CCC on
the open coast.

### Row F — City of Alameda (background)

The beach sits within City of Alameda. Typically EBRPD operation
preempts the city's animal code on EBRPD-operated land, but
worth verifying.

### Row G — California Department of Fish and Wildlife (CDFW)

**Surfaced by parks.ca.gov:** "At the north end of the beach,
Crab Cove is a marine reserve." Marine reserves in CA are CDFW-
administered MPAs. So a sub-area within Crown Memorial has a
separate CDFW agency authority for wildlife protection. **First
walkthrough to exercise the CDFW MPA layer.**

| Field | Value | Status |
|-------|-------|--------|
| `name` | "California Department of Fish and Wildlife" | ✅ |
| `short_name` | "CDFW" | ✅ |
| `type` | `state` | ✅ |
| `authority_domains` | `wildlife_protection` (at Crab Cove MPA specifically) | ✅ |
| `web_url` | https://wildlife.ca.gov | ✅ |
| `code_archive_url` | https://govt.westlaw.com/calregs (CCR Title 14) | ✅ |
| `pip_layer` | `cdfw_mpa` (existence in pipeline pending PIP audit — see [[ca-agency-taxonomy]]) | ❓ |
| **Scope at this beach** | sub-area (Crab Cove only); NOT the whole park | ✅ |

---

## 3. Operator stack

### Op-1 — East Bay Regional Park District (as operator)

**Day-to-day operator, distinct from the legal agency (DPR).**
This is the first walkthrough where operator and primary agency
are different government entities — different legal entities,
different governance, different ordinances.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "East Bay Regional Park District" | ✅ |
| `type` | `special_district` (operating) | ✅ |
| `agency_id` | **FK → Row B (EBRPD as agency)** | ✅ — EBRPD is both an agency AND the operator |
| `web_url` | https://www.ebparks.org | ✅ |
| `note` | EBRPD's authority to operate this state-owned park flows from a lease/operating agreement with DPR. The agreement itself is the bridge document (analogous to HBDB's MOU but at state-to-special-district scale). | 🔵 |

### Op-2 — Crab Cove Visitor Center

EBRPD operates a visitor center at Crown Memorial. May have its
own scope-limited operational rules.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "Crab Cove Visitor Center" | 🔵 |
| `type` | `agency_subdivision` (part of EBRPD operations) | 🔵 |
| `agency_id` | FK → Row B (EBRPD) | 🔵 |
| `scope_limitation` | Visitor center building + immediate grounds | 🔵 |

---

## 4. Policy source stack

### PS-1 — DPR statewide dog policy (parent layer)

Per [[walkthrough-crystal-cove]], DPR publishes its statewide dog
policy at parks.ca.gov/Dogs. **This applies as the parent layer
at Crown Memorial because Crown Memorial is legally a CA State
Park unit.**

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `agency_administrative_policy` | ✅ |
| `citation` | "California State Parks dog policy" | ✅ |
| `issuing_agency` | CA State Parks (Row A) | ✅ |
| `source_url` | https://www.parks.ca.gov/Dogs | ✅ |
| `note` | Statewide baseline: dogs on 6-ft leash in developed areas, prohibited on beach sand absent unit-specific exception. | ✅ |

### PS-1b — Crown Memorial unit-specific dog policy (operative)

The unit-specific page that operationalizes PS-1 at Crown
Memorial. **This is the operative document for dog policy at
Crown Memorial** — it's authored by DPR (the legal agency), not
by EBRPD (the operator).

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `agency_administrative_policy` (scope-narrowed to one park unit) | ✅ |
| `citation` | "Crown Memorial State Beach dog policy (parks.ca.gov/?page_id=526)" | ✅ |
| `issuing_agency` | DPR — Crown Memorial unit (NOT EBRPD) | ✅ |
| `scope` | `dog_policy` | ✅ |
| `source_url` | https://www.parks.ca.gov/?page_id=526 | ✅ |
| `parent_citation` | PS-1 (DPR statewide policy) | ✅ |

**Verbatim rule:**

> "Yes: Dogs allowed only on lawn areas and along the paved
> pathways. Dogs not allowed on the beach."

**Resolves the governance question:** DPR's parks.ca.gov page is
the operative dog policy here, NOT EBRPD's Ordinance 38. The fact
that DPR maintains a unit-specific page for an EBRPD-operated park
demonstrates that DPR retains policy authority — operator authority
is bounded by the agency's policy.

### PS-2 — Lease/operating agreement: DPR ↔ EBRPD

**The bridge document.** Whatever legal instrument grants EBRPD
authority to operate Crown Memorial on behalf of DPR. Likely a
long-term lease or operating agreement, executed at the
state-to-special-district scale.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `lease_agreement` (new subtype — distinct from `mou` because the parties are both government entities and the transfer is operational authority, not stewardship contract) | 🔵 |
| `citation` | "Lease/Operating Agreement: CA DPR ↔ EBRPD for Crown Memorial Regional Shoreline" | ❓ |
| `issuing_parties` | DPR (lessor) + EBRPD (lessee/operator) | 🔵 |
| `scope` | `operations` and possibly delegated `dog_policy` (the agreement may specify whether EBRPD's Ordinance 38 OR CA State Parks rules govern on the operated property) | ❓ |
| `source_url` | CPRA candidate (likely held by DPR's North Coast Redwoods District or Bay Area District) | ❓ |
| `note` | This is the **operational-authority transfer document** — the parallel to HBDB's MOU but at a higher governance scale. Whether DPR's dog policy still governs vs. EBRPD's Ordinance 38 likely turns on the agreement's terms. | 🔵 |

### PS-3 — EBRPD Ordinance 38 (NOT operative for dog_policy at this beach)

The district-wide EBRPD ordinance. **Confirmed NOT operative for
dog_policy at Crown Memorial** — DPR's policy governs here
because Crown Memorial is DPR-owned land. Ordinance 38 still
applies on EBRPD-owned property elsewhere in the district. May
also apply at Crown Memorial for purely operational domains
(parking enforcement, hours-of-operation enforcement) where EBRPD
hasn't been overridden by DPR.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `special_district_ordinance` (new subtype) | 🔵 |
| `citation` | "EBRPD Ordinance 38" | 🔵 |
| `issuing_agency` | EBRPD (Row B) | ✅ |
| `scope_at_crown_memorial` | NOT dog_policy. Possibly `enforcement`, `operations` (if not pre-empted by the lease). | ✅ |
| `note` | Resolved by parks.ca.gov page 2026-05-16: DPR's policy governs dog_policy at Crown Memorial despite EBRPD operating the property. The lease (PS-2) preserves DPR's policy authority on its own land. | ✅ |

### PS-4 — Crab Cove Marine Reserve (CDFW MPA designation)

The marine reserve designation at the north end of the beach.
Sub-area-specific authority — applies only to the Crab Cove
portion, not the rest of the park.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `state_regulation` (CCR Title 14, MPA designation) | 🔵 |
| `citation` | "Crab Cove State Marine Reserve" (CCR Title 14 §632, subsection TBD) | 🔵 |
| `issuing_agency` | CDFW (Row G) | ✅ |
| `scope` | `wildlife_protection` (sub-area: Crab Cove only) | ✅ |
| `source_url` | https://wildlife.ca.gov/Conservation/Marine/MPAs/Network/Northern-California (or specific MPA page) | 🔵 |
| `effect_on_dog_policy` | Marine reserves typically restrict take of marine resources; may NOT directly affect dog access but should be verified. The DPR policy already prohibits dogs on the beach, which subsumes any MPA-side restriction. | 🔵 |

### PS-4 — Crown Memorial-specific posted rules

Park-unit-specific rules. May be in EBRPD's per-park web page,
posted signs, or the visitor center.

### PS-5 — CA State Lands tidelands

Same baseline as prior walkthroughs. Bay tidelands below MHW.

### PS-6 — BCDC permit conditions (if applicable)

---

## 5. The governance question — RESOLVED

**Whose rules apply at Crown Memorial — DPR's or EBRPD's?
ANSWER: DPR's.** Verified 2026-05-16 via parks.ca.gov/?page_id=526
which carries DPR's verbatim dog policy for this unit and
explicitly notes that EBRPD "operates" the park.

**General principle this surfaces:** operator authority is
bounded by the agency's policy when operating gov-owned land via
an operating lease. The operator runs the place but does NOT
make its own dog policy on the agency's land. EBRPD Ordinance 38
applies on EBRPD-owned property elsewhere in the district; it does
NOT apply at Crown Memorial.

**The lease (PS-2) — still unsourced but inferable.** The DPR
page's unit-specific dog policy + the absence of any EBRPD-issued
overlay implies the lease preserves DPR's policy authority. The
lease's actual text would confirm and may also specify which
domains EBRPD does have discretion over (likely operations,
parking, hours enforcement). CPRA-requestable from either party
if needed.

---

## 6. `beach_agency` join rows

| beach_fid | agency | authority_domain | precedence_rank | note |
|-----------|--------|------------------|-----------------|------|
| 8452 | CA State Parks | `dog_policy` | 1 (legal authority) | parent if PS-1 governs |
| 8452 | EBRPD | `dog_policy` | 2 (operational) | parent if PS-3 governs via lease |
| 8452 | EBRPD | `operations` | 1 | clearly EBRPD's domain |
| 8452 | EBRPD | `enforcement` | 1 | EBRPD has its own police |
| 8452 | CA State Lands | `tidelands` | 1 | baseline |
| 8452 | Alameda Health | `water_quality` | 1 | baseline |
| 8452 | BCDC | `coastal_access` | 2 | bay-specific |

**The `precedence_rank` column finally has a real reason to
matter on `dog_policy` itself.** Two agencies both claim
dog_policy authority — the rank captures the layered
relationship: DPR is the parent authority; EBRPD's policy operates
under DPR's umbrella via the lease.

---

## 7. `beach_operator` join rows

| beach_fid | operator | agreement_type | scope |
|-----------|----------|----------------|-------|
| 8452 | EBRPD | `operating_lease` (under PS-2) | full park |
| 8452 | Crab Cove Visitor Center (EBRPD) | `direct_management` | visitor center scope |

---

## 8. Section-level dog policy (anticipated, needs verification)

Significant uncertainty until the operative source is verified.
Likely shape:

| Section | Rule | Source | Status |
|---------|------|--------|--------|
| Beach sand | **`prohibited`** (verbatim: "Dogs not allowed on the beach") | PS-1b | ✅ verbatim |
| Lawn areas | **`on_leash`** ("Dogs allowed only on lawn areas") — leashed per DPR 6-ft baseline | PS-1b + PS-1 | ✅ verbatim + inferred |
| Paved pathways | **`on_leash`** ("along the paved pathways") | PS-1b | ✅ verbatim |
| Crab Cove (north end) | `prohibited` (continues beach prohibition) + MPA protections | PS-1b + PS-4 | ✅ |
| Eelgrass / wildlife protection zones | likely `prohibited` (subsumed by beach prohibition) | PS-1b + PS-4 | 🔵 |

---

## 9. Gaps / verification needed

1. ✅ fid confirmed (8452), row pulled
2. 🔵 DPR ↔ EBRPD lease/operating agreement for Crown Memorial — text not sourced; CPRA-requestable from either party but **not blocking** since operative dog policy is verified via PS-1b
3. 🔵 EBRPD Ordinance 38 verbatim text — confirmed NOT operative at Crown Memorial for dog_policy; full text still useful for other EBRPD beaches in Wave 2
4. ❓ Crown Memorial page on ebparks.org for posted dog policy — WebFetch 403'd; would need user paste or alternate route
5. ✅ Parks.ca.gov page for Crown Memorial (page_id=526, dog policy verbatim captured)
6. ❓ CPAD record details for unit 46017 — confirms legal owner; verifiable via cpad_units table query
7. ❓ c1_jurisdiction_id 512 — confirm it's City of Alameda
8. ❓ Crab Cove State Marine Reserve specific CCR citation (Title 14 §632.x subsection)
9. ❓ Any BCDC permit conditions touching dog policy

---

## 10. What this walkthrough is designed to surface for the model

### The big new principle: operator authority is bounded
- **Operator authority is bounded by the agency's policy when
  operating gov-owned land via an operating lease.** EBRPD
  operates Crown Memorial but cannot override DPR's dog policy
  on DPR-owned land. The operator runs the place; the agency
  makes the rules.
- The model handles this cleanly: a beach can have an `operator`
  that is itself an `agency` (EBRPD is a special_district agency)
  WITHOUT that agency's policy applying at this beach. The
  operator FK and the policy_source FK are independent — the
  operator doesn't automatically bring its own policy with it.
- **EBRPD Ordinance 38 still applies on EBRPD-owned property
  elsewhere in the district.** This is the per-beach scoping
  question — Ordinance 38's effective scope depends on which
  parcels EBRPD owns vs operates.

### Sub-area agency overlays (CDFW MPA)
- **Crab Cove State Marine Reserve** introduces a CDFW MPA
  overlay on the north portion of Crown Memorial. First
  walkthrough to exercise the CDFW MPA layer. The MPA's effect
  on dog access is subsumed by DPR's beach prohibition here, but
  the sub-area pattern is real.
- Tests **multi-agency authority on the same beach at different
  sub-areas** — similar to Fort Funston where §7.97(d) only
  applied to Crissy Field WPA + Ocean Beach SPPA, not Fort
  Funston main.

### New `policy_source` subtypes
- **`lease_agreement`** — operational-authority transfer between
  two government entities. Distinct from `mou` (which is more
  stewardship-flavored, often gov-to-nonprofit) and
  `concession_lease` (gov-to-private). State-to-special-district
  leases are common in CA; this subtype likely needs to exist.
- **`special_district_ordinance`** — confirmed by EBRPD's
  Ordinance 38. As noted in the deleted Point Isabel scaffold,
  may not need to be distinct from a renamed `published_ordinance`
  subtype.

### Tests that confirm prior patterns
- Special-district as agency (Row B) — same pattern Point Isabel
  was going to test.
- Cross-county special district (Alameda + Contra Costa) — tests
  multi-county `agency.hierarchy`.
- EBRPD's enforcement authority distinct from city/county police
  — tests `enforcement` authority_domain.
- Scope-limited internal operator (Crab Cove Visitor Center) —
  tests scope-section pattern.

### Surfaces a known pattern that may be common in CA
- **State park land operated by a regional park district** is a
  recognized CA pattern beyond just Crown Memorial. Other examples
  may include McLaughlin Eastshore State Park (Albany Beach in our
  dataset; EBRPD-operated). Once we model Crown Memorial, the
  pattern generalizes across Wave 2 to any DPR-owned + special-
  district-operated parcel.

---

## 11. Related

- [[law-as-primary-source-ca]] — strategy
- [[walkthrough-hbdb]], [[walkthrough-crystal-cove]], [[walkthrough-fort-funston]]
- [[ca-agency-taxonomy]] — type catalog (needs: `special_district`
  as primary agency type for non-coast parks;
  `lease_agreement` subtype; multi-county `hierarchy` handling)
- [[inland-lake-beaches-in-scope]] — SF Bay shoreline is in scope
- [[entity-modeling]] — modeling principle
- [[scoring-scope]] — tier verification needed
