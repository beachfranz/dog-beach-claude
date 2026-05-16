# Walkthrough — Fort Funston / GGNRA (fid TBD)

**Purpose:** third end-to-end beach walkthrough for the
[[law-as-primary-source-ca]] initiative. Tests **federal NPS**,
the **CFR + Superintendent's Compendium parent/child citation
pattern**, and (uniquely among the walkthroughs so far) a beach
with a **decades-long litigation history that has shaped the
current policy** — meaning **court rulings are a real
policy_source class** here, not a hypothetical.

**Companions:** [[walkthrough-hbdb]] (city + nonprofit operator),
[[walkthrough-crystal-cove]] (state park, restrictive direction).

**Conventions:** same as prior walkthroughs.
- ✅ verified (primary source captured)
- 🔵 unverified (training-data memory; needs source)
- ❓ unknown (no information available)

**Note:** I'm working from memory for nearly everything. Federal
NPS policy at GGNRA has been changing for 25+ years through
rulemaking attempts, lawsuits, and withdrawals — current state
genuinely needs primary verification.

---

## 1. Beach identity

| Field | Value | Status |
|-------|-------|--------|
| `fid` | 6097 | ✅ |
| `name` | "Fort Funston" | ✅ |
| `location_id` | `fort-funston-san-francisco` | ✅ |
| `county_name` | San Francisco | ✅ |
| `state` | CA | ✅ |
| `is_active` | true (flipped from false 2026-05-16; previous `inactive_reason = rollback_arena_auto_promote`) | ✅ |
| `tier` | likely 1 (off-leash designated in part) — pending litigation-state verification | 🔵 |
| Location | Southwestern San Francisco, coastal bluffs and beach below | 🔵 |
| `lat`, `lon` | (in `beaches_gold` row) | ✅ |
| Park unit | Golden Gate National Recreation Area (GGNRA), Fort Funston site | ✅ |
| Federal agency | National Park Service (NPS) | ✅ |

---

## 2. Agency stack

### Row A — National Park Service / GGNRA

The primary agency. Federal land manager.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "National Park Service" | ✅ |
| `unit_name` | "Golden Gate National Recreation Area" | ✅ |
| `type` | `federal` | ✅ |
| `hierarchy` | ["United States", "Department of the Interior", "National Park Service", "Golden Gate NRA"] | ✅ |
| `authority_domains` | `dog_policy`, `operations`, `wildlife_protection`, `historical_preservation`, `coastal_access` | 🔵 |
| `web_url` | https://www.nps.gov/goga | ✅ |
| `unit_specific_url` | https://www.nps.gov/goga/planyourvisit/fort-funston.htm (verify) | 🔵 |
| `code_archive_url` | https://www.ecfr.gov/current/title-36 (federal CFR) | ✅ |
| `primary_statute_citation` | 36 CFR §2.15 (pets) — plus the GGNRA-specific compendium | 🔵 |
| `pip_layer` | `pad_us` filtered to NPS units (or `nps_units` if separate) | ✅ |

### Row B — California State Lands Commission

Same baseline as HBDB and Crystal Cove. **But more legally
interesting here** — Fort Funston's coastline includes federal-land
above MHW and state tidelands below MHW. The interface is where
jurisdictional questions get sharp.

### Row C — City and County of San Francisco

**Possibly relevant.** Fort Funston is a federal site but it's
*in* San Francisco. SF may have animal-control authority that
applies absent federal preemption. SF's leash law (Park Code §5.07
or similar) may apply on the bluff trails if they're SF-owned
parcels adjacent to federal land.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "City and County of San Francisco" | ✅ |
| `type` | `city_county` (SF is unified) | ✅ |
| **Relevance at this beach** | ❓ — depends on actual parcel ownership and federal preemption boundaries | |

### Row D — SF Recreation and Parks Department

Adjacent or overlapping land manager.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "San Francisco Recreation and Parks Department" | ✅ |
| `type` | `city_dept` | ✅ |
| `agency_id` | FK → Row C (City and County of SF) | ✅ |
| **Relevance** | ❓ — Fort Funston proper is federal; SF Rec/Parks may own adjacent bluff parcels | |

### Row E — NOAA / Greater Farallones National Marine Sanctuary

Offshore federal regulation. Touches `water_quality` /
`wildlife_protection` rather than `dog_policy` at the beach
itself.

---

## 3. Operator stack

### Op-1 — National Park Service (as operator)

NPS operates GGNRA directly, just as DPR operates Crystal Cove
directly.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "National Park Service" | ✅ |
| `type` | `federal_park_agency` | ✅ |
| `agency_id` | **FK → Row A (NPS)** | ✅ — operator is the same legal entity as the agency |

### Op-2 — Golden Gate National Parks Conservancy

The 501(c)(3) park foundation for GGNRA. Analogous to Crystal
Cove Conservancy. Fundraising + programs + interpretive services,
generally not direct operations.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "Golden Gate National Parks Conservancy" | 🔵 |
| `type` | `nonprofit` / `park_foundation` | 🔵 |
| `web_url` | https://www.parksconservancy.org | 🔵 |
| `agency_id` | **null** | ✅ |
| **Role** | Likely fundraising / interpretation / volunteer coordination, NOT day-to-day enforcement | 🔵 |

### Op-3 — SF Dog Owners Group (SFDOG) — stakeholder, NOT operator

**Important distinction.** SFDOG is a community advocacy group
that has been the *plaintiff* in major lawsuits shaping Fort
Funston dog policy. They are NOT an operator under any agreement
with NPS. They're a **stakeholder** whose litigation actions
appear in the policy_source chain (via court rulings) but who
doesn't fit the operator role.

**Surfaces a new question for the model:** does the entity model
need a `stakeholder` class for orgs whose actions affected policy
without operating the beach? Or are they just cited inline in the
provenance of court ruling policy_source rows? Recommend the
latter — simpler.

---

## 4. Policy source stack

This is where Fort Funston gets uniquely interesting. The dog
policy at Fort Funston is the product of **multiple overlapping
sources** — federal regulation, a 1979 pet policy, multiple
attempted rulemakings, court rulings striking down rulemakings,
and the current operative Compendium.

**Citation chain (four layers deep):**

```
PS-1 (36 CFR §2.15)            ← general NPS pet rule
   ↑ parent_citation
PS-1a (36 CFR §7.97)           ← GGNRA-specific federal regulation
   ↑ parent_citation
PS-3 (1979 GGNRA Pet Policy)   ← agency administrative policy authorizing voice-control areas
   ↑ parent_citation
PS-2 (2025 GOGA Compendium)    ← annual Superintendent layer; operative current document
```

### PS-1 — 36 CFR §2.15 (NPS pets — federal baseline)

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `federal_regulation` | ✅ |
| `citation` | "36 CFR §2.15 (Pets)" | ✅ |
| `issuing_agency` | NPS | ✅ |
| `scope` | `dog_policy` | ✅ |
| `source_url` | https://www.ecfr.gov/current/title-36/chapter-I/part-2/section-2.15 | 🔵 |
| `note` | Federal baseline: pets restrained on a leash ≤6 ft, crated, or otherwise physically confined at all times unless Superintendent designates an exception. | 🔵 |

### PS-1a — 36 CFR §7.97(d) (Crissy Field + Ocean Beach Snowy Plover Areas)

The unit-specific federal regulation for **two specific sub-areas
of GGNRA only** — Crissy Field WPA and Ocean Beach SPPA. **Not
applicable at Fort Funston.** §7.97(d) imposes a federal seasonal
on-leash restriction (July 1 - May 15) that the 1979 Pet Policy by
itself wouldn't impose; it's a **"tightening" parent** that
narrows the Pet Policy's permissive voice-control allowance at
these two snowy plover protection sites.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `federal_regulation` | ✅ |
| `citation` | "36 CFR §7.97(d) (Dogs — Crissy Field and Ocean Beach Snowy Plover Areas)" | ✅ |
| `issuing_agency` | NPS | ✅ |
| `scope` | `dog_policy` — seasonal on-leash at two GGNRA sub-areas | ✅ |
| `parent_citation` | PS-1 (36 CFR §2.15) | ✅ |
| `geographic_scope` | Crissy Field WPA + Ocean Beach SPPA only — NOT Fort Funston | ✅ |
| `temporal_scope` | Seasonal: July 1 - May 15 each year | ✅ |
| `source_url` | https://www.govinfo.gov/content/pkg/CFR-2010-title36-vol1/pdf/CFR-2010-title36-vol1-sec7-97.pdf (2010 edition; current ecfr.gov blocked from automated fetch) | ✅ |

**Verbatim §7.97(d):**

> "(d) Dogs—Crissy Field and Ocean Beach Snowy Plover Areas.
>
> (1) Dogs must be restrained on a leash not more than six feet in length starting July 1 and ending May 15, in the following areas:
>
> (i) Crissy Field Wildlife Protection Area (WPA): Dog walking restricted to on-leash only in the area encompassing the shoreline and beach north of the Crissy Field Promenade (excluding the paved parking area, sidewalks and grass lawn of the former Coast Guard Station complex) that stretches east from the Torpedo Wharf to approximately 700 feet east of the former Coast Guard station, and all tidelands and submerged lands to 100 yards offshore.
>
> (ii) Ocean Beach Snowy Plover Protection Area (SPPA): Dog walking restricted to on-leash only in the area which encompasses the shoreline and beach area west of the GGNRA boundary, between Stairwell 21 to Sloat Boulevard, including all tidelands and submerged lands to 1,000 feet offshore.
>
> (2) Notice of these annual restrictions will be provided through the posting of signs at the sites, on maps identifying the restricted areas on the park's official website and through maps made available at other places convenient to the public."

**Important for the model:** §7.97(d) is **not in the citation
chain for Fort Funston.** Fort Funston's authority chain is just
§2.15 → 1979 Pet Policy → 2025 Compendium (three layers). The
four-layer chain I sketched earlier applies only to Crissy Field
WPA and Ocean Beach SPPA.

### Parent-citation chains vary by sub-area within one park unit

GGNRA has at least three distinct citation chains, depending on
which sub-area's rule is being rendered:

| Sub-area | Citation chain |
|----------|----------------|
| Fort Funston main beach (outside 12-acre closure) | §2.15 → 1979 Pet Policy → 2025 Compendium |
| Fort Funston 12-acre NW closure | §2.15 → 1979 Pet Policy → 2025 Compendium (carve-out from voice-control) |
| Crissy Field WPA | §2.15 → §7.97(d) → 1979 Pet Policy → 2025 Compendium |
| Ocean Beach SPPA | §2.15 → §7.97(d) → 1979 Pet Policy → 2025 Compendium |
| Other GGNRA voice-control areas (Marin sites, Baker Beach, Lands End, Fort Miley) | §2.15 → 1979 Pet Policy → 2025 Compendium |

**Model insight:** `parent_citation` is not a property of the
park unit — it's a property of each section-level rule. The same
beach can have rules from different citation chains depending on
which sub-area the rule covers. The section-rules table already
supports this (one row per section, each citing its own
`source_policy_id`), but the inference is real: don't bake
"this beach has citation chain X" as a beach-level attribute.

### PS-2 — 2025 GOGA Superintendent's Compendium

The current annual document. Explicitly references and operates
under the 1979 Pet Policy (PS-3) — not a freestanding rule but an
implementation of an older policy.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `superintendents_compendium` | ✅ |
| `citation` | "2025 GOGA Superintendent's Compendium" | ✅ |
| `issuing_agency` | NPS — GGNRA Superintendent | ✅ |
| `scope` | `dog_policy` (and many other operational rules) | ✅ |
| `parent_citation` | PS-3 (1979 Pet Policy) — explicitly referenced | ✅ |
| `effective_date` | 2025 | ✅ |
| `source_url` | https://www.nps.gov/goga/learn/management/upload/2025-GOGA-Compendium-Formatted.pdf | ✅ |

**Verbatim — voice-control authorization for Fort Funston:**

> "VOICE CONTROL DOG WALKING*: The 1979 Pet Policy allows for
> Managed Dogs, leashed or under Voice Control, in the following
> areas: …
> San Francisco (Exhibits #32-36):
> • Fort Funston, except in the 12-acre closure in northwest Fort
> Funston."

**Vocabulary surfaced:** "Managed Dogs" = the formal umbrella term
in the Compendium covering both leashed and voice-controlled
status, distinct from prohibited.

**Referenced exhibits:** the Compendium cites Exhibits #32-36 for
SF voice-control area maps. These are spatial boundary documents
attached to (not within) the Compendium text — relevant when we
need to render area boundaries.

### PS-3 — 1979 GGNRA Pet Policy

✅ **Confirmed operative.** The 2025 Compendium explicitly cites
"The 1979 Pet Policy allows for Managed Dogs, leashed or under
Voice Control, in the following areas…" — so this 46-year-old
policy is still the underlying authority, not just a historical
artifact.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `agency_administrative_policy` | ✅ |
| `citation` | "GGNRA Pet Policy of 1979" | ✅ confirmed operative via 2025 Compendium |
| `issuing_agency` | NPS — GGNRA | ✅ |
| `scope` | `dog_policy` | ✅ |
| `effective_date` | 1979 | ✅ |
| `parent_citation` | PS-1a (36 CFR §7.97) and/or PS-1 (36 CFR §2.15) — exact upward link needs verification from the policy text itself | 🔵 |
| `source_url` | ❓ — text itself not in the Compendium excerpt; may require FOIA or court records | |
| `note` | Establishes voice-control authorization at Fort Funston, Crissy Field, Ocean Beach, Baker Beach (north of Lobos Creek), Lands End, Fort Miley (east + west), plus Marin County trails and beaches. Confirmed by 2025 Compendium reference. | ✅ |

### PS-4 — Court rulings (new subtype — this beach demands it)

**Federal court rulings have shaped what NPS can and cannot do at
GGNRA.** Multiple cases over the decades have struck down NPS
attempts to issue Compendium changes restricting dog access on
procedural grounds (NEPA, APA, public notice requirements).

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | **`court_ruling`** (NEW subtype surfaced by this beach) | ✅ |
| `examples` | *United States v. Barley* (2005) — held that NPS Compendium changes restricting off-leash at GGNRA were invalid for lack of rulemaking. Various subsequent rulings on the GGNRA Dog Management Plan. | 🔵 |
| `issuing_authority` | US District Court / 9th Circuit (varies by case) | ✅ |
| `scope` | `dog_policy` | ✅ |
| `effect` | Constrains what PS-2 (Compendium) can do — Superintendent can't restrict off-leash without rulemaking that complies with NEPA + APA | ✅ |

### PS-5 — Withdrawn 2016 Dog Management Rule

✅ **Confirmed and dated** via Federal Register notice.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | **`withdrawn_rulemaking`** | ✅ |
| `citation` | "Withdrawal of Proposed Rule for Dog Management at the Golden Gate National Recreation Area, California" | ✅ |
| `fr_doc_number` | FR Doc 2017-27827 (NPS-PWR-GOGA-24579) | ✅ |
| `published` | December 27, 2017 (effective same day) | ✅ |
| `issuing_agency` | National Park Service (Interior Department) | ✅ |
| `withdrew` | 2016 NPRM at 81 FR 9139 (published Feb 24, 2016) | ✅ |
| `docket` | PPPWGOGAPO, PPMPSPD1Z.YM0000 | ✅ |
| `source_url` | https://www.govinfo.gov/content/pkg/FR-2017-12-27/html/2017-27827.htm | ✅ |
| `effective_status` | NOT operative — rulemaking terminated before final adoption | ✅ |
| `scope` | `dog_policy` | ✅ |

**Verbatim — why withdrawn:**

> "The National Park Service (NPS) no longer intends to prepare a
> final rule or issue a Golden Gate National Recreation Area dog
> management plan."
>
> "The NPS has now cancelled that planning process and terminated
> the associated NEPA and rulemaking processes. No final rule will
> be issued."

**What governs in the rule's absence — model-relevant note:**

The withdrawal notice itself is silent on what governs going
forward. It doesn't say "the 1979 Pet Policy remains in effect"
or reference any existing regulations. The legal effect is
**status quo ante** — whatever was operative before the 2016 NPRM
continues to govern. In practice, per the 2025 Compendium, that's
the 1979 Pet Policy + the unit-specific Compendium implementation.

**Model implication:** a `withdrawn_rulemaking` row doesn't itself
declare what governs in its absence. The model needs to handle
this gracefully — the rendering logic should walk to the
next-most-recent operative source rather than expecting the
withdrawal to point forward. At Fort Funston this means: withdraw
the 2016 rule, fall back to PS-3 (1979) + PS-2 (current
Compendium).

**On the ex parte rationale:** training-data memory and prior news
reporting suggest the withdrawal followed revelations of ex parte
contacts during the rulemaking. The Federal Register notice itself
does NOT cite ex parte concerns — it just says the planning
process is terminated. The ex parte finding is from separate
court / NPS-IG records and would need its own source if we wanted
to capture *why* in detail. The FR withdrawal is the operative
fact.

### PS-4 — 2016 Proposed Dog Management Rule (the withdrawn NPRM)

For provenance completeness, the 2016 NPRM itself can be captured
as a separate policy_source row marked as withdrawn.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `proposed_rule` (or extends `withdrawn_rulemaking` with a `withdrawn_by_id` FK pointing at PS-5) | 🔵 — modeling choice deferred |
| `citation` | "Proposed Rule for Dog Management at the Golden Gate National Recreation Area, 81 FR 9139" | ✅ |
| `published` | February 24, 2016 | ✅ |
| `effective_status` | Never effective; withdrawn 2017-12-27 by PS-5 | ✅ |
| `source_url` | https://www.federalregister.gov/d/2016-04032 (or similar — confirm) | 🔵 |

### PS-6 — Operator posted policy (NPS Fort Funston web page)

Same subtype as HBDB's dogbeach.org and Crystal Cove's
parks.ca.gov pages. NPS's own published policy at the unit level.

### PS-7 — Public Resources Code §6001+ (CA State Lands tidelands)

Same as prior walkthroughs. Below the federal-land MHW line.

---

## 5. `beach_agency` join rows

| beach_fid | agency | authority_domain | precedence_rank |
|-----------|--------|------------------|-----------------|
| TBD | NPS / GGNRA | `dog_policy` | 1 |
| TBD | NPS / GGNRA | `wildlife_protection` | 1 |
| TBD | NPS / GGNRA | `operations` | 1 |
| TBD | CA State Lands | `tidelands` | 2 (below MHW; subordinate to federal above) |
| TBD | SF Public Health (?) | `water_quality` | 1 |
| TBD | NOAA (Greater Farallones NMS) | `water_quality` (offshore) | 2 |

The `precedence_rank` actually has meaningful values at Fort
Funston for the first time — state and federal both have claims
on tideland water quality.

---

## 6. `beach_operator` join rows

| beach_fid | operator | agreement_type | scope |
|-----------|----------|----------------|-------|
| TBD | NPS (as operator) | `direct_management` | full beach |
| TBD | Golden Gate National Parks Conservancy | `cooperating_association` | programs / fundraising |

---

## 7. Section-level dog policy

Genuinely uncertain — current state needs primary verification.
The historical position is off-leash in designated areas; recent
years have seen attempts to restrict and litigation pushback.

| Section | Per 36 CFR §2.15 baseline | Per 2025 Compendium (operative) | Status |
|---------|---------------------------|---------------------------------|--------|
| Fort Funston main beach + bluffs (outside 12-acre closure) | leashed only | **voice-control off-leash** ("Managed Dogs, leashed or under Voice Control") | ✅ verbatim from PS-2 |
| 12-acre closure in northwest Fort Funston | leashed only | **prohibited** (permanent closure) | ✅ verbatim |
| Bank Swallow Protection Area | leashed/prohibited (wildlife) | likely the same 12-acre closure (verify via Exhibit #32-36) | 🔵 |

**Permanent vs seasonal closure pattern:** Fort Funston's 12-acre
closure is **permanent**, distinct from Crissy Field and Ocean
Beach which have **seasonal** on-leash restrictions (July 1 –
May 15) for snowy plover protection. The model likely needs a
`closure_type` field on the section rules: `permanent` /
`seasonal` / `weather_triggered` (sewage spill, surf advisory) /
`event_based`.

---

## 8. Gaps / verification needed

1. ✅ Confirm fid in `beaches_gold` for Fort Funston (6097).
2. ✅ Pull current GGNRA Superintendent's Compendium (2025 GOGA Compendium — voice-control list captured).
3. ✅ Verify current state of 1979 Pet Policy (operative; explicitly cited by 2025 Compendium).
4. ❓ Catalog the major court rulings for the `court_ruling` policy_source entries — still open; doesn't block the model now that the operative policy is verified.
5. ✅ Withdrawal status of the GGNRA Dog Management Plan confirmed (FR Doc 2017-27827, effective 2017-12-27).
6. ❓ Determine whether SF Park Code applies to any portion of Fort Funston (preemption question) — likely no, but unverified.
7. ❓ Verify Golden Gate National Parks Conservancy legal name and operator-vs-foundation role.
8. ✅ 36 CFR §7.97(d) captured verbatim — confirmed NOT in Fort Funston's chain; applies to Crissy Field + Ocean Beach only.
9. ❓ The voice-control footnote (asterisked in the Compendium) — what does "voice control" formally mean per the Compendium definition? Verbatim needed.
10. ❓ Pull the precise boundary of the "12-acre closure in northwest Fort Funston" from Exhibits #32-36.

---

## 9. What this walkthrough is designed to surface for the model

### New `policy_source` subtypes
- **`court_ruling`** — federal/state court decisions that shape policy. Anticipated for Fort Funston, likely needed for any beach with significant litigation history. Fields: court, case name, decision date, citation (volume/reporter/page), effect on policy. **Status:** still anticipated; verified Compendium operates under 1979 policy, so rulings may have established this status quo even though current operative document is the Compendium.
- **`withdrawn_rulemaking`** — proposed rules that didn't become final. Evidentiary for current state but not operative. Anticipated for GGNRA Dog Management Plan; still needs verification.
- **`superintendents_compendium`** — ✅ **confirmed by Fort Funston** as a needed subtype. Annual Park Superintendent document that operationalizes underlying federal regs + agency policies. Parallel to (but more formal than) Crystal Cove's parks.ca.gov park page.

### Citation chain depth — four layers, not three
- HBDB: 2 layers (city statute + city MOU + operator-posted)
- Crystal Cove: 2 layers (DPR-wide + park-unit)
- Fort Funston: **4 layers** (federal CFR §2.15 → federal CFR §7.97 → 1979 Pet Policy → 2025 Compendium)
- The `parent_citation` field needs to support arbitrarily deep chains. If implemented as a single FK, traversal can walk the chain to render full provenance.

### New vocabulary surfaced
- **"Managed Dogs"** — formal NPS umbrella term for leashed-or-voice-controlled status. Captures both "permitted" states distinct from "prohibited." Could be a `rule` value alongside `off_leash` / `on_leash` / `not_allowed`, OR could be derived from a combination of fields.
- **"Voice Control"** — specific subtype of off-leash that requires the dog be under voice and sight control. Not the same as unrestricted off-leash. Probably wants its own rule value: `off_leash_voice_control`.

### Closure-type field
- **Permanent** (Fort Funston 12-acre) vs **seasonal** (Crissy Field / Ocean Beach July 1 - May 15 plover protection). Different render and different verification cadence. The model probably needs a `closure_type` field on section rules: `permanent` / `seasonal` / `weather_triggered` / `event_based`.

### Spatial exhibits as attached documents
- The Compendium cites Exhibits #27-31B (Marin) and #32-36 (SF) for boundary maps of voice-control areas. These are spatial boundary documents *attached to* the Compendium, not within its text. The model may need an `attachments` field on policy_source, or a separate `policy_source_attachment` table for binary/image references.

### Confirms patterns from prior walkthroughs
- `parent_citation` on PS-2 (Compendium) pointing to PS-1 (CFR) — same pattern as Crystal Cove's PS-2 → PS-1.
- `agency_administrative_policy` for the 1979 Pet Policy — same subtype as the DPR-level Crystal Cove policy.
- Operator-as-agency for NPS — same pattern as DPR at Crystal Cove.
- Park foundation (Golden Gate NP Conservancy) as separate operator — same shape as Crystal Cove Conservancy.

### Net-new structural questions
- **Stakeholder entities** (e.g., SFDOG) that affected policy via litigation but don't operate the beach. **Recommended treatment: cite inline in court_ruling provenance; don't add a separate entity class.**
- **How the model represents "operative policy is older instrument because newer rulemaking failed."** PS-3 (1979 policy) + PS-5 (2017 withdrawal) together tell that story; needs both rows to be honest.

### Reckoning with uncertainty
- Federal NPS dog policy at GGNRA has been actively changing through litigation for 25+ years. Any walkthrough working from training-data memory will have stale or wrong claims about the *current* state. Treat this beach as one where verification is non-optional before any data populates downstream.

---

## 10. Related

- [[law-as-primary-source-ca]] — strategy
- [[walkthrough-hbdb]], [[walkthrough-crystal-cove]] — prior beaches
- [[ca-agency-taxonomy]] — type catalog (needs updates: `court_ruling` + `withdrawn_rulemaking` subtypes)
- [[entity-modeling]] — subjects/views/measurements
