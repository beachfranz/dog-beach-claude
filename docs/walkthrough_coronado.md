# Walkthrough — Coronado Beach (fid 8715)

**Purpose:** tenth and final end-to-end beach walkthrough for the
[[law-as-primary-source-ca]] initiative Phase-5b-prep batch.
Tests **city beach + military federal adjacency** — the Naval Base
Coronado / Naval Air Station North Island border presses against
the beach's geography and may inject federal access restrictions
at low-tide approaches and adjacent federal-property zones.

**Companions:** prior nine walkthroughs.

**Why this slot:** the prior nine walkthroughs covered city,
city-with-carve-out, state-park, federal NPS, county-on-lease,
county-direct, section-split joint-op, and tribal. **Military
adjacency is the major untested federal-authority class.** Many
CA coastal beaches have military neighbors (Coronado, Camp
Pendleton-adjacent SD beaches, Vandenberg-area SB beaches, Naval
Postgraduate School at Monterey, etc.). One walkthrough
validates the model for the pattern.

**Conventions:** same as prior walkthroughs.

---

## 1. Beach identity

| Field | Value | Status |
|-------|-------|--------|
| `fid` | 8715 | ✅ |
| `name` | "Coronado Beach" | ✅ |
| `location_id` | `coronado-beach-san-diego-8715` | ✅ |
| `county_name` | San Diego | ✅ |
| `state` | CA | ✅ |
| `cpad_unit_id` | 5171 (in CPAD; same id as Coronado Dog Beach fid 6202 — likely a single CPAD unit covers both) | ✅ |
| `c1_jurisdiction_id` | 469 (likely City of Coronado) | 🔵 |
| `park_name` | null | ✅ |
| `tier` | likely 3 — dogs prohibited on main beach (Coronado Dog Beach is the off-leash carve-out at NW tip) | 🔵 |
| Location | Hotel del Coronado area, Silver Strand, City of Coronado | ✅ |
| Length | ~1.5 miles of main beach (the iconic Hotel del strip) | 🔵 |

**The Coronado-beach geography:**
- **Coronado Beach (8715)** — the main city beach in front of the
  Hotel del Coronado, running south toward the Naval Amphibious
  Base.
- **Coronado Dog Beach (6202)** — the OFF-LEASH carve-out at the
  northwest tip of Coronado, near Naval Air Station North Island
  (which is the federal restricted zone immediately adjacent).
- **Silver Strand State Beach** is south of Coronado proper (NAB
  Coronado is between them).
- We're walking the MAIN beach (8715), not Dog Beach. Dog Beach
  would be a separate walkthrough (carve-out-style; parallel to
  HBDB / Rosie's).

---

## 2. Agency stack

### Row A — City of Coronado

The primary agency. Charter city, ~24k population. Operates its
own city services + the main beach.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "City of Coronado" | ✅ |
| `type` | `city` | ✅ |
| `hierarchy` | ["California", "San Diego County", "Coronado"] | ✅ |
| `authority_domains` | `dog_policy`, `operations`, `parking`, `enforcement` (Coronado Police) | 🔵 |
| `web_url` | https://www.coronado.ca.us | ✅ |
| `code_archive_url` | Coronado Municipal Code platform (TBD — likely Municode or amlegal) | ❓ |

### Row B — US Navy / NAS North Island + NAB Coronado (federal, ADJACENT)

**Adjacent federal authority.** Not operating Coronado Beach
itself but adjacent. NAS North Island is federal property
immediately north of Coronado Dog Beach; Naval Amphibious Base
Coronado is south of Coronado Beach proper. The boundary
between city beach and federal land can be ambiguous at low
tide (when more beach is exposed and people can walk close to
the base).

| Field | Value | Status |
|-------|-------|--------|
| `name` | "US Navy — Naval Base Coronado" | ✅ |
| `short_name` | "NBC" / "NAB Coronado" / "NAS North Island" | ✅ |
| `type` | `military` (new agency type) | ✅ |
| `hierarchy` | ["United States", "Department of Defense", "Department of the Navy", "Naval Base Coronado"] | ✅ |
| `authority_domains` | `access_restriction` (federal land), `security` | ✅ |
| **Relevance at Coronado Beach** | adjacent only; not operating the city beach. May have access restrictions at the federal-boundary (e.g., security signage, prohibited approach at low tide). | 🔵 |

### Row C — California State Lands Commission

Tidelands below MHW (baseline).

### Row D — San Diego County Department of Environmental Health

Water quality / bacteria advisories (baseline; SD County's
equivalent of OC Health / LA County Public Health).

### Row E — California Coastal Commission

CDPs / coastal access (baseline).

### Row F — California State Parks (DPR, background)

DPR doesn't operate Coronado Beach itself, but Silver Strand
State Beach (south of NAB Coronado, separate fid) is a DPR unit.
Background for adjacency context.

---

## 3. Operator stack

### Op-1 — City of Coronado (as operator)

Same legal entity as Row A; operates the main beach directly.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "City of Coronado" | ✅ |
| `type` | `city` (operating) | ✅ |
| `agency_id` | FK → Row A | ✅ |

### Op-2 — Coronado Lifeguard Services (likely under city PD or Fire)

Water safety. Coronado is small enough that lifeguarding may be
contracted or under city Fire / PD rather than a county service.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "City of Coronado Lifeguards" | 🔵 |
| `type` | `city_dept` | ✅ |
| `authority_domains` | `water_safety` | ✅ |

---

## 4. Policy source stack

### PS-1 — Coronado Municipal Code (city dog ordinance)

The city ordinance governing dog access on Coronado beaches.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `municipal_code` | 🔵 |
| `citation` | "Coronado Municipal Code §___" | ❓ |
| `source_url` | Coronado Municipal Code platform (TBD) | ❓ |

### PS-1b — Coronado Municipal Code Title 32 (Animal Regulations)

✅ **Citation captured 2026-05-16** via the city's published
dogs page. The Coronado MC Title 32 is the codified ordinance
covering animal access on city property.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `municipal_code` | ✅ |
| `citation` | "Coronado Municipal Code Title 32 — Animal Regulations" | ✅ |
| `source_url` | https://www.coronado.ca.us/757/Dogs (the city's dog-policy summary page, linking to the underlying Title 32) | ✅ for the summary page; full title text not fetched |
| `note` | The full Title 32 text would need direct Coronado MC lookup; the summary page is the operator-published reference. | 🔵 |

### PS-2 — City of Coronado published dogs page (operative summary)

✅ **Fetched 2026-05-16** at coronado.ca.us/757/Dogs.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `agency_administrative_policy` | ✅ |
| `source_url` | https://www.coronado.ca.us/757/Dogs | ✅ |
| `parent_citation` | PS-1b (Coronado MC Title 32) | ✅ |

**Verbatim — page intro:**

> "The City of Coronado welcomes your furry friends. On this
> page, you will find where dogs are permitted on and off leash on
> City property. Find more information by viewing the Coronado
> Municipal Code Title 32 - Animal Regulations."

**Key data extracted:**

- **Coronado Beach (main beach, this walkthrough's fid 8715)** is
  NOT listed on the "where dogs are permitted" page. The page is
  structured as an enumerated list of dog-permitted areas; omission
  = prohibited. **Implied rule: dogs NOT allowed on Coronado main
  beach.**
- **Coronado Dog Beach** (separate fid 6202): "dogs may be off
  leash on the beach, but need to be on leash at all points before
  the beach"
- **Six city parks with dog rules** (all on-leash unless otherwise
  noted, distinct from the beach itself):
  - Bay View Park (413 First Street) — on leash
  - Centennial Park (1099½ First Street) — on leash
  - Harbor View Park (First Street & E Avenue) — on leash
  - Tidelands Park (Glorietta Blvd @ Third Street) — on leash,
    pavement only
  - Vetter Park (1612 Cajon Place) — on leash
  - Coronado Cays Park (99 Grand Caribe Isle) — off-leash inside
    designated dog run; on-leash elsewhere

### PS-3 — Federal restricted-access signage at NAB / NAS boundary

Where the city beach meets federal property, the Navy posts
restricted-access signage (DoD authority under 18 USC §1382 and
related federal trespass statutes). This is real federal authority
but spatially limited to the boundary.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `federal_regulation` (DoD trespass) or `posted_sign` | 🔵 |
| `relevance_to_dogs` | minimal — affects access generally, not dogs specifically | 🔵 |

### PS-4 — H&S §115880+ (water quality, SD County implementation)

Baseline.

### PS-5 — PRC §6001+ (tidelands)

Baseline.

---

## 5. Section-level dog policy (anticipated, needs verification)

| Section | Rule | Source | Status |
|---------|------|--------|--------|
| Main beach (sand, in front of Hotel del) | **`not_allowed`** (Coronado Beach is NOT listed on the city's enumerated dog-permitted-areas page; omission = prohibited per the page structure) | PS-2 | ✅ implied |
| Water | `not_allowed` (extends sand prohibition) | PS-2 | ✅ implied |
| Boardwalk / Strand pathway | likely `on_leash` (city sidewalks; not explicitly enumerated but consistent with general city rule) | PS-1b | 🔵 |
| Parking | likely `on_leash` (city property; not explicitly enumerated) | PS-1b | 🔵 |
| Federal-boundary zone at NAB/NAS edge | federal access restrictions; not dog-specific | PS-3 | 🔵 |

---

## 6. Anticipated `beach_agency` rows

| beach_fid | agency | authority_domain | precedence_rank |
|-----------|--------|------------------|-----------------|
| 8715 | City of Coronado | `dog_policy` | 1 |
| 8715 | City of Coronado | `operations` | 1 |
| 8715 | City of Coronado | `parking` | 1 |
| 8715 | City of Coronado | `water_safety` | 1 |
| 8715 | US Navy (NBC) | `access_restriction` (federal-boundary zone only) | 1 (within its scope) |
| 8715 | SD County Env Health | `water_quality` | 1 |
| 8715 | CA State Lands | `tidelands` | 1 |
| 8715 | CCC | `coastal_access` | 2 |

---

## 7. Gaps / verification needed

1. ✅ fid confirmed (8715)
2. 🔵 Coronado.ca.us is JS-rendered with heavy Google Translate
   integration; basic Playwright fetch returns only language
   dropdown chrome. Same pattern as Yurok website. Multiple URL
   patterns tried (/beach, /services/parks_and_recreation,
   /government/departments/library/coronado_municipal_code) —
   none yielded inline content. Would need user-paste or deeper
   site navigation.
3. ❓ Coronado Municipal Code dog/beach ordinance — research path:
   user-paste from website OR fetch from Municode/amlegal if
   Coronado hosts its code there
4. ❓ Confirm Coronado main beach is dogs-prohibited (anticipated
   based on the fact that Coronado Dog Beach 6202 is the explicit
   off-leash carve-out elsewhere on the peninsula — main beach
   would not typically permit dogs)
5. ❓ Federal/civilian boundary location — likely visible on
   any local map of NAB Coronado / NAS North Island; specific
   coordinates not critical for the model design
6. ❓ DoD closure zones — unlikely on the main beach itself
   (federal authority is at adjacent property, not extending into
   civilian beach)

**Honest note:** specific dog rules at Coronado main beach are
deferred. The structural model wins (military agency type,
federal-boundary sub-area pattern) don't depend on the
verbatim rule.

---

## 8. What this walkthrough is designed to surface

### NEW agency type — `military`

- `agency.type = 'military'` for DoD / Navy / Army / etc.
- Distinct from generic `federal` because military federal
  authority has different scope and legal basis (18 USC §1382,
  DoD regulations) than civilian federal agencies (NPS, USFWS,
  USFS, etc.)
- Adjacency-only operative; rarely operates the beach itself but
  affects access at boundaries

### Federal boundary zones as sub-area concept

- Where a city beach meets federal military property, there's a
  spatial boundary where federal authority kicks in
- This is similar to the Crab Cove MPA sub-area at Crown Memorial
  (CDFW authority on a sub-region of the beach)
- The model already supports sub-area regions via `beach_policy_source`
  + section-scoped rules; military-boundary zones fit cleanly

### Confirms patterns from prior walkthroughs

- Pure city beach (City of Coronado as agency-and-operator)
- Adjacent-only federal authority (similar in shape to MPA
  overlays at Crown Memorial / Salt Creek)
- Multi-agency stack with most agencies non-operative for
  dog_policy specifically

---

## 9. Related

- [[law-as-primary-source-ca]]
- prior nine walkthroughs
- [[consensus-source-authority]]
- [[entity-modeling]]
