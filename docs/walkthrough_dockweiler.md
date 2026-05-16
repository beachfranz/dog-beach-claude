# Walkthrough — Dockweiler State Beach (fid 8477)

**Purpose:** fifth end-to-end beach walkthrough for the
[[law-as-primary-source-ca]] initiative. Tests **county-as-operator
of state-park land** — the same cross-government-entity pattern
Crown Memorial exercised, but with **LA County Beaches & Harbors**
as the operator instead of a special district. Validates whether
the bounded-operator principle from [[walkthrough-crown-memorial]]
generalizes to multiple operator-class variants.

**Companions:** [[walkthrough-hbdb]], [[walkthrough-crystal-cove]],
[[walkthrough-fort-funston]], [[walkthrough-crown-memorial]].

**Wave 3 anchor:** LA County operates several state-park beaches
under leases from DPR (Dockweiler, Will Rogers, Topanga, Leo
Carrillo, others). If we model Dockweiler correctly, the pattern
generalizes across the LA-region wave.

**Conventions:** same as prior walkthroughs.
- ✅ verified (primary source captured)
- 🔵 unverified (training-data memory; needs source)
- ❓ unknown

---

## 1. Beach identity

| Field | Value | Status |
|-------|-------|--------|
| `fid` | 8477 | ✅ |
| `name` | "Dockweiler State Beach" | ✅ |
| `location_id` | `dockweiler-state-beach-los-angeles` | ✅ |
| `county_name` | Los Angeles | ✅ |
| `state` | CA | ✅ |
| `cpad_unit_id` | 6136 (in CA Protected Areas Database — confirms state-park ownership) | ✅ |
| `c1_jurisdiction_id` | 207 (likely City of Los Angeles, given Vista del Mar / Playa del Rey location) | 🔵 |
| `lat`, `lon` | 33.9402, -118.4410 | ✅ |
| `park_name` | null (not all state parks have this set) | ✅ |
| `tier` | likely 3 or 4 — dogs prohibited per LA County beach policy + DPR baseline | 🔵 |
| Length | ~3.7 miles, Imperial Hwy south to Manhattan Beach line | 🔵 |

---

## 2. Agency stack

### Row A — California Department of Parks and Recreation (DPR)

**The legal agency.** Dockweiler is in CPAD as a state park unit.
DPR holds ownership and ultimate policy authority — same pattern
as Crown Memorial.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "California Department of Parks and Recreation" | ✅ |
| `type` | `state` | ✅ |
| `authority_domains` | `dog_policy` (baseline via parks.ca.gov/Dogs) | 🔵 |
| `cpad_unit_id` | 6136 | ✅ |

### Row B — Los Angeles County Department of Beaches and Harbors

**The operator** — first walkthrough to test
county-department-as-operator (Crown Memorial tested
special-district-as-operator).

| Field | Value | Status |
|-------|-------|--------|
| `name` | "Los Angeles County Department of Beaches and Harbors" | ✅ |
| `short_name` | "LA County Beaches & Harbors" | ✅ |
| `type` | `county_department` | ✅ |
| `hierarchy` | ["California", "Los Angeles County"] | ✅ |
| `authority_domains` | `dog_policy` (per LA County Code), `operations`, `parking`, `enforcement` (LA County Lifeguards under Fire Dept) | 🔵 |
| `web_url` | https://beaches.lacounty.gov | ✅ |
| `code_archive_url` | LA County Code on Municode | 🔵 |
| `primary_statute_citation` | LA County Code Title 17 (Beaches and Harbors) — needs section verification | 🔵 |

### Row C — LA County Fire Department / Lifeguard Division

LA County lifeguards are part of the Fire Department, not Beaches
& Harbors. Distinct authority for water safety.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "LA County Fire Department, Lifeguard Division" | 🔵 |
| `type` | `county_department` | ✅ |
| `authority_domains` | `water_safety`, `enforcement` (water-related) | 🔵 |

### Row D — LA County Department of Public Health

Water quality / bacteria advisories.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "Los Angeles County Department of Public Health" | ✅ |
| `authority_domains` | `water_quality`, `bacteria_advisories` | ✅ |

### Row E — CA State Lands Commission

Tidelands below MHW. Same baseline.

### Row F — California Coastal Commission

CDPs and coastal-access authority. Open coast = CCC jurisdiction.

### Row G — City of Los Angeles

The Dockweiler strip straddles unincorporated LA County and the
City of LA boundary (Vista del Mar is partly within LA city
limits per c1_jurisdiction_id 207). Generally state-park operation
preempts city animal codes, but worth verifying which side of the
city line the actual beach sits on.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "City of Los Angeles" | ✅ |
| `type` | `city` | ✅ |
| `relevance` | ❓ unclear — partial overlap with city limits but state-park operation likely preempts | |

### Row H — Federal Aviation Administration / LAX (background)

LAX is immediately east of Dockweiler — the beach is under flight
paths. Not policy-relevant for dog_policy but relevant for
operational atmosphere (jet noise, possible access restrictions
related to airport security after major events).

---

## 3. Operator stack

### Op-1 — LA County Beaches & Harbors (as operator)

Day-to-day operator under lease from DPR.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "Los Angeles County Department of Beaches and Harbors" | ✅ |
| `type` | `county_department` | ✅ |
| `agency_id` | FK → Row B (same legal entity, different role) | ✅ |
| `web_url` | https://beaches.lacounty.gov | ✅ |
| **Operating agreement** | DPR ↔ LA County lease (CPRA candidate; likely a long-standing operating lease for the LA-region state beaches) | 🔵 |

### Op-2 — Dockweiler RV Park concessionaire

LA County Parks (or a contracted operator) manages the Dockweiler
RV Park concession. Scope-limited operator pattern (rentals only,
not the beach).

| Field | Value | Status |
|-------|-------|--------|
| `name` | (concessionaire name TBD) | ❓ |
| `type` | `private_contractor` / `concessionaire` | 🔵 |
| `agency_id` | null | ✅ |
| `scope_limitation` | RV park only | 🔵 |

---

## 4. Policy source stack

### PS-1 — DPR statewide dog policy

Per [[walkthrough-crystal-cove]] and [[walkthrough-crown-memorial]],
DPR publishes the statewide baseline at parks.ca.gov/Dogs.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `agency_administrative_policy` | ✅ |
| `source_url` | https://www.parks.ca.gov/Dogs | ✅ |

### PS-1b — Dockweiler entry in DPR statewide /Dogs catalog (operative)

✅ **Confirmed via parks.ca.gov/Dogs catalog** (fetched
2026-05-16 via Playwright). DPR does NOT maintain a separate unit
page for Dockweiler — the dog policy is captured in the
statewide catalog row itself.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `agency_administrative_policy` | ✅ |
| `source_url` | https://www.parks.ca.gov/Dogs (catalog row for Dockweiler) | ✅ |
| `issuing_agency` | DPR | ✅ |
| `scope` | `dog_policy` | ✅ |
| `parent_citation` | PS-1 (DPR statewide policy) | ✅ |

**Verbatim row from DPR /Dogs catalog:**

> Dockweiler State Beach	**No**	Park unit operated by Los
> Angeles County. Check website at
> https://beaches.lacounty.gov/dockweiler-beach/ for more
> information.

Three things in one line:
- **"No"** = headline answer; dogs not allowed at Dockweiler.
- **"Park unit operated by Los Angeles County"** = DPR
  acknowledges the operator relationship in its catalog.
- **Reference to beaches.lacounty.gov** = DPR delegates
  operational detail publishing to the operator, but retains the
  headline rule.

**Refinement of the bounded-operator principle (from
[[walkthrough-crown-memorial]]):** DPR retains the headline rule
("No dogs") but delegates operational-detail publishing to the
operator. Crown Memorial = DPR authors + EBRPD echoes;
Dockweiler = DPR points at LA County. Different documentation
pattern, same legal-authority structure — the principle holds.

### PS-2 — Three-party lease chain: State → City of LA → LA County

✅ **Surfaced via the LA County Dockweiler page history section
2026-05-16.** Verbatim:

> "It was leased to the City of Los Angeles by the State of
> California in 1946. … The beach has been operated by the Los
> Angeles County Department of Beaches and Harbors for the City
> of Los Angeles since 1976."

This is a **three-party operating arrangement** that doesn't fit
a single `lease_agreement` row cleanly. Two real bridge documents:

**PS-2a — 1946 State ↔ City of LA lease**

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `lease_agreement` | ✅ |
| `parties` | State of California (lessor) + City of Los Angeles (lessee) | ✅ |
| `effective_date` | 1946 | ✅ |
| `scope` | full beach operating authority | ✅ |
| `source_url` | CPRA candidate | ❓ |

**PS-2b — 1976 City of LA ↔ LA County sub-operating agreement**

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `operating_agreement` (sub-operator arrangement, distinct from a direct lease from the original lessor) | ✅ |
| `parties` | City of Los Angeles (operator-of-record under PS-2a) + LA County Beaches & Harbors (day-to-day operator) | ✅ |
| `effective_date` | 1976 | ✅ |
| `scope` | day-to-day operations | ✅ |
| `source_url` | CPRA candidate | ❓ |

**Model implication:** The `beach_operator` table needs to
support multi-level operating chains. Approaches:
- Two `beach_operator` rows for Dockweiler: one for the City of
  LA (lessee, `agreement_type = 'lease_agreement'`) and one for
  LA County (sub-operator, `agreement_type = 'sub_operating'`),
  with a `parent_operator_id` FK linking them.
- OR a flat structure with multiple rows where the role / depth
  is implicit from the agreement_type.
- The chain matters for legal-authority queries ("under whose
  lease is this?") but the day-to-day rendering just needs the
  operator-of-record (LA County).

### PS-3 — LA County Code Title 17 (Beaches & Harbors)

The LA County ordinance governing all county-operated and
county-managed beaches. Includes dog-policy provisions (likely a
near-blanket prohibition; Rosie's Dog Beach in Long Beach is one
of the few exceptions). Verifies generalizability of LA County's
beach ordinance to Wave 3.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `municipal_code` (or `county_code` if we distinguish) | 🔵 |
| `citation` | "LA County Code Title 17 — Beaches and Harbors" (specific dog section TBD) | 🔵 |
| `issuing_agency` | LA County (Row B's parent county) | ✅ |
| `source_url` | https://library.municode.com/ca/los_angeles_county (Municode) | 🔵 |
| `note` | The general dog prohibition at LA County beaches is widely cited; the specific section number needs verification from Municode. | 🔵 |

### PS-4 — LA County Beaches & Harbors Dockweiler page (silent on dogs)

✅ **Fetched 2026-05-16 via Playwright.** The page lists
amenities, history, surf report — but is **silent on dog
policy.** No leash language, no "dogs prohibited" sign-text, no
mention of dogs whatsoever.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `agency_administrative_policy` (operator-published; silent on dogs) | ✅ |
| `source_url` | https://beaches.lacounty.gov/dockweiler-beach/ | ✅ |
| `dog_policy_content` | **None — operator page does not publish dog policy text.** | ✅ |

**Model insight:** the operator-posted-policy subtype that
HBDB exercised at dogbeach.org doesn't apply at every
operator-managed beach. At Dockweiler the operator publishes
operational info (amenities, history, surf) and is silent on
dogs. The dog policy is reachable only via PS-1b (DPR catalog
row) and PS-3 (LA County Code Title 17). **`operator_posted_policy`
subtype is OPTIONAL, not universal.** The model needs to allow
operators to have no published dog policy and gracefully walk to
the agency layer for the rule.

### PS-5 — CA State Lands tidelands

Same baseline.

### PS-6 — CCC CDP conditions

If any active CDPs at Dockweiler touch dog policy or seasonal
access.

---

## 5. The governance question — anticipated, same shape as Crown Memorial

**Who governs dog_policy at Dockweiler — DPR or LA County?**

Anticipated per the bounded-operator principle from
[[walkthrough-crown-memorial]]: **DPR governs** because the land
is DPR-owned. LA County's Title 17 applies on LA County-owned
beaches (most LA County beaches are county-owned; Dockweiler is
state-owned-county-operated, the inverse). Will verify by
checking parks.ca.gov for Dockweiler and beaches.lacounty.gov for
the same.

**The richer question:** if LA County's Title 17 and DPR's policy
DISAGREE, which governs? The bounded-operator principle says
DPR's policy wins on DPR-owned land. But LA County may simply
apply its own beach ordinance uniformly across all operated
beaches regardless of underlying ownership, in which case there's
either:
- A delegation from DPR to LA County in the lease, OR
- An on-the-ground enforcement reality where LA County's rule
  applies even where it shouldn't legally.

---

## 6. `beach_agency` join rows (anticipated)

| beach_fid | agency | authority_domain | precedence_rank |
|-----------|--------|------------------|-----------------|
| 8477 | DPR | `dog_policy` | 1 (legal authority) |
| 8477 | LA County B&H | `dog_policy` | 2 (operational; via Title 17 + lease) |
| 8477 | LA County B&H | `operations` | 1 |
| 8477 | LA County B&H | `parking` | 1 |
| 8477 | LA County Fire / Lifeguard | `water_safety` | 1 |
| 8477 | LA County Public Health | `water_quality` | 1 |
| 8477 | CA State Lands | `tidelands` | 1 |
| 8477 | CCC | `coastal_access` | 2 |

---

## 7. `beach_operator` join rows

| beach_fid | operator | agreement_type | scope |
|-----------|----------|----------------|-------|
| 8477 | LA County Beaches & Harbors | `lease_agreement` (under PS-2) | full beach |
| 8477 | Dockweiler RV Park concessionaire | `concession_lease` | RV park only |

---

## 8. Anticipated section-level dog policy

| Section | Anticipated rule | Source | Status |
|---------|-----------------|--------|--------|
| Beach sand | `prohibited` | PS-1 + likely PS-3 | 🔵 |
| Bike path / boardwalk | likely `on_leash` (per DPR + LA County rules in developed zones) | PS-1 + PS-3 | 🔵 |
| Parking lots | `on_leash` (per DPR baseline) | PS-1 | 🔵 |
| Dockweiler RV Park | `on_leash` at sites | PS-4 | 🔵 |
| Fire ring areas | likely `prohibited` (active fire = safety risk) | PS-4 | 🔵 |

---

## 9. Gaps / verification needed

1. ✅ fid confirmed (8477)
2. ✅ DPR parks.ca.gov page — does NOT exist as a separate unit page; DPR uses the /Dogs catalog row + delegates to LA County's site
3. ✅ LA County Beaches & Harbors page for Dockweiler — fetched; silent on dog policy
4. ❓ LA County Code Title 17 dog-policy section — Municode (SPA pattern still blocking)
5. ❓ 1946 State ↔ City of LA lease text + 1976 City of LA ↔ LA County operating agreement (CPRA candidates; not blocking)
6. ❓ Confirm c1_jurisdiction_id 207 = City of LA
7. ❓ Whether RV Park operator is a distinct entity / concessionaire
8. ❓ Any CDP conditions

---

## 10. What this walkthrough is designed to surface for the model

### Generalize the bounded-operator principle to multiple operator classes — ✅ CONFIRMED

Crown Memorial tested **special-district-as-operator**. Dockweiler
tests **county-department-as-operator** (in a multi-level chain).
Both hold the bounded-operator pattern: DPR retains policy
authority; operators implement. The principle generalizes to ANY
government-entity operator on DPR-owned land. **Unlock for the
entire Wave 3 LA-region pass** — Will Rogers, Topanga, Leo
Carrillo, etc. should all follow this structure.

**Refinement Dockweiler adds:** DPR doesn't always author a
unit-specific page. The DPR /Dogs catalog row alone is sufficient
as the operative source. This is more common than Crown
Memorial's pattern (where both a unit page AND the catalog
exist).

### Three-party lease chains exist

The State → City of LA → LA County arrangement at Dockweiler is
a real pattern in CA municipal land management. The
`beach_operator` table needs to support multi-level chains via
either nested `parent_operator_id` FKs or flat-row stacking.

### `operator_posted_policy` is OPTIONAL

Not every operator publishes the dog rule. HBDB's dogbeach.org
does; LA County's beaches.lacounty.gov for Dockweiler doesn't.
The model needs to allow operators to be silent and walk to the
agency layer for the rule.

### LA County Code Title 17 as Wave-3 reusable source

Once we have Title 17's dog-policy section sourced, it applies
to every LA-region beach LA County operates. Same way the
parks.ca.gov/Dogs page was a Wave-2 accelerator for state parks,
Title 17 is a Wave-3 accelerator for LA-region beaches.

### Multi-department county
- LA County contributes THREE separate agency rows (Beaches &
  Harbors, Fire/Lifeguard, Public Health). First walkthrough where
  a single county appears as multiple agency rows with different
  authority_domains. Tests `county` vs `county_department` typing
  cleanly.

### Test of `lease_agreement` subtype on a second case
- Crown Memorial anticipated the `lease_agreement` subtype;
  Dockweiler exercises it again with different parties. Two real
  examples increases confidence the subtype belongs in the
  taxonomy.

---

## 11. Related

- [[law-as-primary-source-ca]] — strategy
- [[walkthrough-hbdb]], [[walkthrough-crystal-cove]], [[walkthrough-fort-funston]], [[walkthrough-crown-memorial]]
- [[ca-agency-taxonomy]] — type catalog (will validate `county_department` as a first-class agency type with multiple instances per county)
- [[entity-modeling]] — modeling principle
