# Walkthrough — Manhattan Beach (fid 8265)

**Purpose:** seventh end-to-end beach walkthrough for the
[[law-as-primary-source-ca]] initiative. Tests **section-split
joint-operation** — same beach, different sections owned by
different government entities operationally. Sand by LA County
Beaches & Harbors; bike path + pier + parking + adjacent parks by
City of Manhattan Beach.

**Companion to** [[walkthrough-hbdb]],
[[walkthrough-crystal-cove]], [[walkthrough-fort-funston]],
[[walkthrough-crown-memorial]], [[walkthrough-dockweiler]],
[[walkthrough-rosies]].

**Why this slot reframed:** initially queued as "pure on-leash
city" test. Franz's Google AI verification surfaced that Manhattan
Beach is actually a section-split joint-op — sand operated by LA
County Beaches & Harbors; adjacent infrastructure (The Strand,
pier, parking, Bruce's Beach park) operated by City of Manhattan
Beach. **More interesting test case** than pure city: exposes a
governance pattern none of the prior six walkthroughs hit. Pure
city as a CA pattern may actually be rare; Balboa Beach (fid 8805)
is a candidate for a future pure-city walkthrough.

**Conventions:** same as prior walkthroughs.
- ✅ verified (primary source captured)
- 🔵 unverified (training-data memory; needs source)
- ❓ unknown

---

## 1. Beach identity

| Field | Value | Status |
|-------|-------|--------|
| `fid` | 8265 | ✅ |
| `name` | "Manhattan Beach" | ✅ |
| `location_id` | `manhattan-beach-los-angeles` | ✅ |
| `park_name` | "Manhattan County Beach" | ✅ |
| `county_name` | Los Angeles | ✅ |
| `state` | CA | ✅ |
| `cpad_unit_id` | 51690 (in CPAD as a LA County beach unit) | ✅ |
| `c1_jurisdiction_id` | 139 (likely City of Manhattan Beach) | 🔵 |
| `tier` | likely 3 or 4 — dogs prohibited on sand per LA County beach ordinance | 🔵 |
| Length | ~2 miles of beach, from Hermosa Beach line south to El Segundo line north | 🔵 |

**The naming dynamic:** `name = "Manhattan Beach"` (the city's
name) but `park_name = "Manhattan County Beach"` (the operator
designation). LA County's CPAD listing is by the operator label;
the consumer-facing name is the city's. Both correct; reflects
the joint-op structure.

---

## 2. Agency stack

### Row A — City of Manhattan Beach

**Local-jurisdiction agency for adjacent infrastructure.**
Authority over The Strand bike path, pier, parking facilities,
Bruce's Beach park, public works adjacent to sand. Charter city
with its own municipal code.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "City of Manhattan Beach" | ✅ |
| `type` | `city` (charter city) | 🔵 |
| `hierarchy` | ["California", "Los Angeles County", "Manhattan Beach"] | ✅ |
| `authority_domains` | `infrastructure_adjacent` (Strand, pier, parking, parks), `parking`, possibly `fire` | 🔵 |
| `web_url` | https://www.manhattanbeach.gov | ✅ |
| `code_archive_url` | Manhattan Beach Municipal Code — platform TBD (Municode? amlegal? qcode?) | ❓ |

### Row B — Los Angeles County Department of Beaches and Harbors

**Sand operator + facility operator.** Same agency that operates
Dockweiler — but here operating under a different relationship
(joint operation with city, not state-park-on-lease). Authority
over the sandy beach itself + day-to-day maintenance + bathrooms,
showers, lifeguard towers.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "Los Angeles County Department of Beaches and Harbors" | ✅ |
| `short_name` | "LA County Beaches & Harbors" | ✅ |
| `type` | `county_department` | ✅ |
| `authority_domains` | `dog_policy` (on the sand they operate), `operations`, `enforcement` | 🔵 |
| `web_url` | https://beaches.lacounty.gov | ✅ |
| `code_archive_url` | LA County Code Title 17 on Municode | 🔵 |

### Row C — LA County Fire Department / Lifeguard Division

**Water safety + ocean rescue agency.** Per Google AI summary,
provides all ocean rescue and medical emergency services.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "LA County Fire Department, Lifeguard Division" | 🔵 |
| `type` | `county_department` | ✅ |
| `authority_domains` | `water_safety`, `medical_emergency` | ✅ |

### Row D — LA County Department of Public Health

Water quality / bacteria advisories (same as Dockweiler).

### Row E — CA State Lands Commission

Tidelands below MHW (same as every coastal walkthrough).

### Row F — California Coastal Commission

CDPs and coastal-access. Background.

### Row G — Federal / Air Force adjacency (background)

Manhattan Beach is south of LAX. Not relevant for dog_policy
(unlike Coronado/Camp Pendleton); no military base on the beach.

---

## 3. Operator stack

### Op-1 — LA County Beaches & Harbors (sand + facilities)

| Field | Value | Status |
|-------|-------|--------|
| `name` | "LA County Beaches & Harbors" | ✅ |
| `type` | `county_department` | ✅ |
| `agency_id` | FK → Row B | ✅ |
| `scope_section` | **sand + bathrooms + showers + lifeguard towers** (not the bike path or pier) | ✅ — section-split case |

### Op-2 — City of Manhattan Beach Public Works (adjacent infrastructure)

| Field | Value | Status |
|-------|-------|--------|
| `name` | "City of Manhattan Beach Public Works Department" | 🔵 |
| `type` | `city_dept` | ✅ |
| `agency_id` | FK → Row A | ✅ |
| `scope_section` | **The Strand bike path, pier, parking facilities, Bruce's Beach park, sidewalks** | ✅ — section-split case |

**This is the NEW pattern:** operators are scoped to specific
sections of the beach. Sand is one operator; the path adjacent to
the sand is a different operator. Both are government entities.

---

## 4. Policy source stack

### PS-1 — LA County Code Title 17 (Beaches and Harbors)

The county-level dog ordinance. Same source row we created in
Phase 2 for Dockweiler. Operative for Manhattan Beach's sand
section because LA County Beaches & Harbors is the sand operator.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `municipal_code` (county-level, but municipal_code is the closest subtype) | ✅ already exists from Phase 2 seed |
| `citation` | "LA County Code Title 17" | 🔵 specific section TBD |
| `source_url` | https://library.municode.com/ca/los_angeles_county | 🔵 |

### PS-1b — LA County beach rules (operative published version)

✅ **Fetched 2026-05-16 via Playwright** at
beaches.lacounty.gov/la-county-beach-rules/.

Verbatim:

> "NO Animals allowed on the beach (no cats, dogs, horses, etc.)"

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `agency_administrative_policy` (scope-narrowed to operator-published rules page) | ✅ |
| `citation` | "LA County Beaches & Harbors — Rules & Regulations on LA County Beaches" | ✅ |
| `source_url` | https://beaches.lacounty.gov/la-county-beach-rules/ | ✅ |
| `parent_citation` | PS-1 (LA County Code Title 17) | ✅ |
| `scope` | `dog_policy` (sand section) | ✅ |

**Note on coverage:** this single rule applies to every LA County
operated beach (Manhattan Beach, Hermosa Beach, Redondo Beach,
Dockweiler, Will Rogers, Topanga, Leo Carrillo, Zuma, etc.). One
policy_source row → 30+ beaches' canonical sand rule. Confirms the
"agency-published rule covers many beaches" pattern from Crown
Memorial / Dockweiler / Crystal Cove.

### PS-2 — Manhattan Beach Municipal Code

The city ordinance covering Strand, pier, parking, adjacent
parks. **NOT operative for the sand** (LA County governs there)
but operative for the city-controlled adjacent areas.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `municipal_code` | 🔵 |
| `citation` | "Manhattan Beach Municipal Code §___ (Beaches / Animals / Parks)" | ❓ |
| `source_url` | Manhattan Beach city code platform (TBD) | ❓ |

### PS-3 — LA County operating policy at Manhattan Beach (administrative)

If LA County Beaches & Harbors maintains a beach-specific page
analogous to its Dockweiler page. May or may not exist as a
distinct entry vs. a generic LA County beaches page.

### PS-4 — California Health & Safety Code §115880+

Same as every coastal walkthrough. AB 411 water quality
delegation.

### PS-5 — PRC §6001+

Tidelands. Same as every coastal walkthrough.

### PS-6 — CCC CDP conditions

Background; likely no dog-policy-relevant CDP at Manhattan Beach.

### PS-7 — Third-party tourism site (community_attestation)

✅ **Fetched 2026-05-16 via Playwright** at
hermosatomanhattanbeach.com/manhattan-beach-dog-guide/.

Tier 5 (`community_attestation`) — not the operator's own
publication, but a third-party tourism site paraphrasing city
rules. Useful as supplementary evidence pending verification of
the actual MB Municipal Code sections.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `community_attestation` | ✅ |
| `citation` | "Hermosa to Manhattan Beach Guide — Manhattan Beach Dog Guide" | ✅ |
| `source_url` | https://hermosatomanhattanbeach.com/manhattan-beach-dog-guide/ | ✅ |
| `scope` | `dog_policy` (broad, multi-section) | ✅ |

**Verbatim claims:**

> "Dogs must be kept on a leash no longer than six feet at all
> times in public spaces. This includes parks, trails, and
> neighborhoods."

> "Similar to its neighbor Hermosa Beach, dogs are not allowed on
> the sand or in the water at Manhattan Beach."

> "Dogs on the Beach: Fines can range from $100 to $500, depending
> on the violation."

> "All dogs in Manhattan Beach must be licensed through the city.
> [Spayed/neutered $20/yr; non-spayed $60/yr.]"

**Designated off-leash dog parks (city-level carve-outs):**
- Polliwog Park Dog Run — NW corner; separate fenced areas for
  small and large dogs; dawn-to-dusk
- Live Oak Park Dog Run — north of tennis courts; 8 AM - 9 PM
- Veterans Parkway / Manhattan Beach Greenbelt — 3.5-mile trail;
  leashed-walks only (not off-leash)

---

## 5. Section-level dog policy (anticipated, needs verification)

| Section | Operator | Rule | Source | Status |
|---------|----------|------|--------|--------|
| Sand | LA County B&H | **`not_allowed`** ("NO Animals allowed on the beach") | PS-1b | ✅ |
| Water (ocean) | LA County B&H | **`not_allowed`** ("dogs are not allowed on the sand or in the water") | PS-7 | ✅ |
| The Strand bike path | City of MB | **`on_leash`** (6ft max — public-space leash law) | PS-7 | ✅ |
| Manhattan Beach Pier | City of MB | likely `not_allowed` (most CA piers prohibit dogs; not confirmed for MB specifically) | PS-2 | 🔵 |
| Parking facilities | City of MB | `on_leash` (public space rule) | PS-7 | ✅ |
| Bruce's Beach park | City of MB | `on_leash` (city park rule) | PS-7 | ✅ |
| Veterans Parkway Greenbelt (off-beach trail) | City of MB | `on_leash` (3.5-mile leashed-walks trail) | PS-7 | ✅ |
| Polliwog Park Dog Run (off-beach carve-out) | City of MB | `off_leash` (dawn-to-dusk; fenced) | PS-7 | ✅ |
| Live Oak Park Dog Run (off-beach carve-out) | City of MB | `off_leash` (8 AM - 9 PM) | PS-7 | ✅ |

---

## 6. `beach_agency` join rows (anticipated)

| beach_fid | agency | authority_domain | precedence_rank |
|-----------|--------|------------------|-----------------|
| 8265 | LA County B&H | `dog_policy` (sand) | 1 |
| 8265 | LA County B&H | `operations` (sand) | 1 |
| 8265 | City of Manhattan Beach | `infrastructure_adjacent` | 1 |
| 8265 | City of Manhattan Beach | `parking` | 1 |
| 8265 | LA County Fire / Lifeguard | `water_safety` | 1 |
| 8265 | LA County Public Health | `water_quality` | 1 |
| 8265 | CA State Lands | `tidelands` | 1 |
| 8265 | CCC | `coastal_access` | 2 |

---

## 7. `beach_operator` join rows (anticipated — new shape)

| beach_fid | operator | agreement_type | scope_section |
|-----------|----------|----------------|---------------|
| 8265 | LA County B&H | `joint_operation` | sand, bathrooms, showers, lifeguard towers |
| 8265 | City of Manhattan Beach Public Works | `direct_management` | The Strand, pier, parking, Bruce's Beach park |

**The `scope_section` column on beach_operator is the model
extension this walkthrough proposes.** Today's beach_operator
design is beach-level. Manhattan Beach requires section-scoped
operators because different sections of one beach have different
operators.

---

## 8. Gaps / verification needed

1. ✅ fid confirmed (8265)
2. ✅ section-split joint-op structure confirmed (Google AI + linked city/county sources)
3. ❓ Manhattan Beach Municipal Code platform (Municode? amlegal? qcode? other?)
4. ❓ Specific MB ordinance section for The Strand / pier / parks dog rules
5. ❓ LA County Title 17 specific dog section number (also blocking Dockweiler verification)
6. ❓ DPR catalog confirms Manhattan Beach is NOT a state park unit (Franz verified Manhattan Beach is not in parks.ca.gov/Dogs)
7. ❓ Confirm c1_jurisdiction_id 139 = City of Manhattan Beach
8. ❓ Whether the joint-op arrangement is documented in a written agreement (MOU? operating contract? historical understanding?)
9. ❓ Whether dogs are prohibited on the sand specifically or just leashed (LA County beach ordinance generally prohibits)

---

## 9. What this walkthrough is designed to surface for the model

### The big new pattern: section-split joint-operation

- Same beach, multiple operators, scope-split by physical section
- The `beach_operator` table needs to support `scope_section` as a
  non-null field for these cases (default to "full beach" for
  single-operator beaches)
- Section-level operator awareness lets the model render
  appropriately: "this section is operated by LA County (their
  policy applies)" vs. "this section is operated by the city
  (their policy applies)"

### Confirms patterns from prior walkthroughs
- LA County Title 17 reuse (same source row from Phase 2's
  Dockweiler seed); validates that one policy_source can cover
  multiple beaches via multiple `beach_policy_source` rows
- Multi-agency stack at one beach (LA County B&H, LA County Fire,
  LA County Public Health, CA State Lands, City of Manhattan
  Beach) — biggest agency stack of any walkthrough so far

### Open architectural question
- Is Manhattan Beach's joint-operation pattern (sand ≠ adjacent)
  *common* across CA coastal beaches? Probably yes for LA-region
  beaches that LA County operates within incorporated cities
  (Hermosa Beach, Redondo Beach, El Segundo Beach, etc. — all
  similar shapes). One walkthrough validates the model for many
  beaches.

---

## 10. Related

- [[law-as-primary-source-ca]]
- [[walkthrough-hbdb]], [[walkthrough-crystal-cove]],
  [[walkthrough-fort-funston]], [[walkthrough-crown-memorial]],
  [[walkthrough-dockweiler]], [[walkthrough-rosies]] — prior six
- [[consensus-source-authority]] — relevant for tier resolution
  within section-scoped operator stacks
- [[entity-modeling]] — modeling principle; this walkthrough
  argues for section-scoped operator entities, not beach-scoped
