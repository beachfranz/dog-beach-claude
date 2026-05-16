# Walkthrough — Salt Creek Beach (fid 8316)

**Purpose:** eighth end-to-end beach walkthrough for the
[[law-as-primary-source-ca]] initiative. Tests **OC Parks as
primary county-park operator** (a county parks department, not a
state-park-on-lease arrangement) plus a **CDFW Marine Life Refuge
overlay** ("Niguel Marine Life Refuge" per the park_name).

**Companions:** [[walkthrough-hbdb]],
[[walkthrough-crystal-cove]], [[walkthrough-fort-funston]],
[[walkthrough-crown-memorial]], [[walkthrough-dockweiler]],
[[walkthrough-rosies]], [[walkthrough-manhattan-beach]].

**Why this slot:** the prior six walkthroughs covered city, state
park, federal NPS, and county-as-operator-of-state-park. Salt
Creek tests a different county pattern: **OC Parks** owns/operates
the beach directly, with a CDFW Marine Life Refuge designation
overlaying parts of it. This is a common pattern for OC coastal
beaches (Salt Creek, Strands, Aliso, Crystal Cove inland trails).

**Conventions:** same as prior walkthroughs.
- ✅ verified
- 🔵 unverified (training-data memory; needs source)
- ❓ unknown

---

## 1. Beach identity

| Field | Value | Status |
|-------|-------|--------|
| `fid` | 8316 | ✅ |
| `name` | "Salt Creek Beach" | ✅ |
| `location_id` | `salt-creek-beach-orange` | ✅ |
| `park_name` | "Niguel Marine Life Refuge" | ✅ |
| `county_name` | Orange | ✅ |
| `state` | CA | ✅ |
| `cpad_unit_id` | null (no CPAD entry — not a state park) | ✅ |
| `c1_jurisdiction_id` | null | ✅ |
| `tier` | likely 3 — typical OC Parks beach (dogs prohibited on sand) | 🔵 |
| Location | Dana Point, between The Strand and Monarch Beach | 🔵 |
| Length | ~1 mile | 🔵 |

**The park_name dynamic:** "Niguel Marine Life Refuge" is the
CDFW Marine Life Refuge / Areas of Special Biological Significance
designation. OC Parks is the operator. The beach is in Dana Point
city limits. So three layers without going to state or federal.

---

## 2. Agency stack

### Row A — Orange County Parks Department (OC Parks)

**Primary agency and operator.** OC Parks runs Salt Creek Beach
directly. Distinct from OC Public Health (water quality) and OC
Sheriff (enforcement).

| Field | Value | Status |
|-------|-------|--------|
| `name` | "Orange County Parks" | ✅ |
| `short_name` | "OC Parks" | ✅ |
| `type` | `county_department` | ✅ |
| `hierarchy` | ["California", "Orange County", "OC Parks"] | ✅ |
| `authority_domains` | `dog_policy`, `operations`, `parking`, `fire`, `lifeguards` | 🔵 |
| `web_url` | https://www.ocparks.com | ✅ |
| `code_archive_url` | OC County Code Title (TBD) on Municode | 🔵 |

### Row B — California Department of Fish and Wildlife (CDFW)

**Marine Life Refuge overlay** — Niguel Marine Life Refuge is a
CDFW-designated area. Limits take of marine resources. Probably
doesn't directly affect dog access but adds a regulatory layer.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "California Department of Fish and Wildlife" | ✅ |
| `short_name` | "CDFW" | ✅ |
| `type` | `state` | ✅ |
| `authority_domains` | `wildlife_protection`, `marine_resources` | ✅ |
| `web_url` | https://wildlife.ca.gov | ✅ |
| **Relevance** | Marine Life Refuge designation overlaps with the offshore tidewaters; rarely affects dog access directly. Second CDFW MPA test after Crab Cove at Crown Memorial. | 🔵 |

### Row C — City of Dana Point

The beach sits within Dana Point city limits. May or may not have
authority on the sand depending on the OC Parks operating
arrangement. Typically state-park-or-county-park operations
preempt city animal codes on the operated property.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "City of Dana Point" | ✅ |
| `type` | `city` | ✅ |
| **Relevance** | ❓ — verify whether MB city's animal code applies on OC Parks land | |

### Row D — Orange County Health Care Agency

Water quality / bacteria advisories (same baseline as HBDB).

### Row E — CA State Lands Commission

Tidelands below MHW (baseline).

### Row F — California Coastal Commission

CDPs and coastal-access (baseline; possibly relevant given the
ML Refuge overlay).

---

## 3. Operator stack

### Op-1 — OC Parks (as operator)

Same legal entity as Row A; operates the beach directly.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "Orange County Parks" | ✅ |
| `type` | `county_department` | ✅ |
| `agency_id` | FK → Row A | ✅ |
| `web_url` | https://www.ocparks.com/parks-trails/salt-creek-beach-park | 🔵 |

### Op-2 — OC Lifeguards (Lifeguard division, possibly part of OC Parks)

Water safety. May be its own division or under OC Parks operations.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "OC Lifeguards" | 🔵 |
| `type` | `county_department` (subdivision) | 🔵 |
| `authority_domains` | `water_safety` | ✅ |

---

## 4. Policy source stack

### PS-1 — Orange County Code (OC County dog ordinance)

The county-level dog ordinance covering OC Parks beaches.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `municipal_code` | 🔵 |
| `citation` | "OC County Code §___ (Beaches and Parks)" | ❓ |
| `source_url` | OC County Code on Municode (TBD) | ❓ |
| `note` | OC County generally prohibits dogs on county-operated beaches. Same general pattern as LA County Title 17 but a separate county code. | 🔵 |

### PS-1b — OC Parks general regional-park rules

✅ **Fetched 2026-05-16 via Playwright** at
ocparks.com/about-us/park-rules/regional-park-rules.

Verbatim baseline:

> "All pets must be on a leash that does not exceed 6 feet"

**Different from LA County baseline.** OC Parks' default is
leashed-pets-allowed; LA County's default is no-animals. Same
agency class (county-park) with opposite default rules. The new
resolver handles this via per-source rules, not assumptions.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `agency_administrative_policy` | ✅ |
| `source_url` | https://www.ocparks.com/about-us/park-rules/regional-park-rules | ✅ |
| `scope` | `dog_policy` (default for all OC Parks-operated parks) | ✅ |

### PS-2 — Salt Creek-specific park rules (operative)

✅ **Fetched 2026-05-16** at
ocparks.com/beaches/salt-creek-beach/park-rules.

Verbatim:

> "Dogs are not permitted on the beach. Dogs are permitted (on a
> 6-foot leash) on paved walkways and at Bluff Park, the grass
> area above Salt Creek Beach."

> "Beach hours: 5 am - 12 a.m. daily (includes Strands Beach)"

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `agency_administrative_policy` (scope-narrowed to Salt Creek + Strands) | ✅ |
| `source_url` | https://www.ocparks.com/beaches/salt-creek-beach/park-rules | ✅ |
| `parent_citation` | PS-1b (OC Parks general regional rule) | ✅ |
| **Pairing** | The rules page notes hours "includes Strands Beach" — Salt Creek and Strands (fid 8317) are paired operationally in OC Parks listings | ✅ |

### PS-3 — CCR Title 14 §632 (Niguel Marine Life Refuge designation)

The CDFW Marine Life Refuge designation regulation. Covers take of
marine resources; probably not dog policy directly.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `state_regulation` | 🔵 |
| `citation` | "CCR Title 14 §632 (Marine Protected Areas) — Niguel Marine Life Refuge entry" | 🔵 |

### PS-4 — Dana Point Municipal Code

If Dana Point's animal ordinance applies on OC Parks land. Default
assumption is no (operator preempts).

### PS-5 — H&S §115880+

Water quality baseline.

### PS-6 — PRC §6001+

Tidelands baseline.

---

## 5. Section-level dog policy (anticipated, needs verification)

| Section | Rule | Source | Status |
|---------|------|--------|--------|
| Sand | **`not_allowed`** ("Dogs are not permitted on the beach") | PS-2 | ✅ |
| Water | `not_allowed` (extends sand prohibition; consistent with the rule) | PS-2 | ✅ implied |
| Paved walkways | **`on_leash`** (6 ft) | PS-2 | ✅ |
| Bluff Park (grass area above the beach) | **`on_leash`** (6 ft) | PS-2 | ✅ |
| Parking lot | `on_leash` (extends paved-walkway rule) | PS-2 | ✅ implied |
| Marine Life Refuge offshore (Niguel) | wildlife / take restrictions; not dog-specific | PS-3 | 🔵 |

---

## 6. Anticipated `beach_agency` rows

| beach_fid | agency | authority_domain | precedence_rank |
|-----------|--------|------------------|-----------------|
| 8316 | OC Parks | `dog_policy` | 1 |
| 8316 | OC Parks | `operations` | 1 |
| 8316 | OC Parks | `parking` | 1 |
| 8316 | CDFW | `wildlife_protection` (Marine Life Refuge sub-area) | 1 |
| 8316 | OC Health | `water_quality` | 1 |
| 8316 | CA State Lands | `tidelands` | 1 |
| 8316 | CCC | `coastal_access` | 2 |
| 8316 | City of Dana Point | (background; non-operative on OC Parks land) | 3 |

---

## 7. Gaps / verification needed

1. ✅ fid confirmed (8316)
2. ❓ OC Parks Salt Creek Beach Park page — fetch via Playwright
3. ❓ OC Parks general beach rules page
4. ❓ OC County Code dog/beach ordinance section
5. ❓ Whether Salt Creek has any off-leash carve-out (unlikely; for completeness)
6. ❓ Niguel Marine Life Refuge specific CCR section number
7. ❓ Confirm Salt Creek is NOT on parks.ca.gov/Dogs (i.e., not a state park unit)
8. ❓ Any current CCC CDP conditions

---

## 8. What this walkthrough is designed to surface

### Tests
- **OC Parks as primary county-park operator** — different from
  LA County B&H pattern (which was joint with city at Manhattan).
  OC Parks is the operator AND the agency-of-record for its own
  parks. Single-entity at the county-park level.
- **Second CDFW Marine Life Refuge overlay** — first was Crab Cove
  at Crown Memorial. Confirms the pattern is reusable.
- **County-park-within-city-limits** — Dana Point as the
  containing city. Tests preemption of city ordinances by county
  operating authority.

### Open architectural questions
- Should `Niguel Marine Life Refuge` be a sub-area within the
  beach (like Crab Cove at Crown Memorial), or a separate beach
  entity? Different from a "section" — it's a regulatory overlay
  rather than a physical sub-area.

---

## 9. Related

- [[law-as-primary-source-ca]]
- prior seven walkthroughs
- [[consensus-source-authority]]
- [[entity-modeling]]
