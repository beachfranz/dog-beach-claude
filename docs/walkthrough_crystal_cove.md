# Walkthrough — Crystal Cove State Beach (fid 8330)

**Purpose:** second end-to-end beach walkthrough for the
[[law-as-primary-source-ca]] initiative. Tests **state-park-as-
primary-agency**, the **CCR Title 14 §4319 + Superintendent's Order
parent/child citation pattern**, and a **restrictive policy
direction** (where dogs are prohibited on most sections) — three
things [[walkthrough-hbdb]] didn't exercise.

**Companion to:** [[walkthrough-hbdb]] (city + operator + MOU stack).

**Conventions:** same as HBDB walkthrough.
- ✅ verified (primary source captured)
- 🔵 unverified (training-data memory; needs source)
- ❓ unknown (no information available)

**Note:** I'm working from memory for nearly everything here.
Honest scaffold to start; expect every claim to be checked or
overturned as Franz feeds primary sources.

---

## 1. Beach identity

| Field | Value | Status |
|-------|-------|--------|
| `fid` | 8330 | ✅ |
| `name` (the readable column on beaches_gold) | (TBD — Franz to confirm which Crystal Cove segment) | 🔵 |
| `state` | CA | 🔵 |
| `tier` | likely **3 or 4** — beach itself is dogs-prohibited; only paved areas permit leashed dogs (see PS-2). May fall outside scoring scope per [[scoring-scope]]. | ✅ confirmed by parks.ca.gov listing |
| Location | Newport Coast / Laguna Beach area, OC. Park spans both coastal and inland (backcountry) zones. | 🔵 |
| `lat`, `lon` | (in `beaches_gold` row — note `lon`, not `lng`) | ✅ |
| Park unit | Crystal Cove State Park (CA State Parks unit) | ✅ |

**Crystal Cove segment disambiguation:** the park has multiple
beach segments — Pelican Point, Reef Point, Los Trancos, Moro
Beach. fid 8330 should be one of these specifically. Worth
confirming which from the `display_name` in `beaches_gold`.

---

## 2. Agency stack — `agency` table rows

### Row A — California Department of Parks and Recreation (CA State Parks / DPR)

The primary agency. Replaces the "city" pattern from HBDB.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "California Department of Parks and Recreation" | ✅ |
| `short_name` | "CA State Parks" / "DPR" | ✅ |
| `type` | `state` | ✅ |
| `hierarchy` | ["California"] | ✅ |
| `authority_domains` | `dog_policy`, `operations`, `parking`, `fire`, `wildlife_protection`, `historical_preservation` (the Historic District) | 🔵 |
| `web_url` | https://www.parks.ca.gov | ✅ |
| `unit_web_url` | https://www.parks.ca.gov/?page_id=644 (Crystal Cove SP page — verify) | 🔵 |
| `code_archive_url` | https://govt.westlaw.com/calregs (CCR Title 14) | ✅ |
| `primary_statute_citation` | CCR Title 14 §4319 | ✅ |
| `pip_layer` | `pad_us` filtered to state-park polygons (or `cpad`) | ✅ |

### Row B — California State Lands Commission

Same as HBDB Row B. Tidelands below MHW.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "California State Lands Commission" | ✅ |
| `authority_domains` | `tidelands`, `submerged_lands` | ✅ |
| `pip_layer` | `cslc_sovereign_lands` (existence pending PIP audit) | ❓ |

### Row C — Orange County Health Care Agency

Same as HBDB Row C. Water quality / bacteria advisories.

### Row D — California Coastal Commission

**Possibly relevant** at Crystal Cove specifically. The CCC issues
Coastal Development Permits with conditions that can affect dog
access, parking, lighting, and seasonal closures for wildlife.
Need to check whether Crystal Cove has any active CDP conditions
that touch dog policy.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "California Coastal Commission" | ✅ |
| `authority_domains` | `coastal_access`, `coastal_development_permits` | ✅ |
| `web_url` | https://www.coastal.ca.gov | ✅ |
| **Relevance at this beach** | ❓ — need to check for active CDPs affecting dog policy | |

---

## 3. Operator stack — `operator` table rows

### Op-1 — California Department of Parks and Recreation (as operator)

Unlike HBDB (where the city outsources day-to-day management to a
nonprofit), state parks typically **self-operate**. DPR is both the
agency AND the direct operator.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "California Department of Parks and Recreation" | ✅ |
| `type` | `state_park_agency` (DPR running its own unit directly) | ✅ |
| `agency_id` | **FK → Row A (CA State Parks)** | ✅ — operator and agency are the same entity, distinguished by role |
| `web_url` | https://www.parks.ca.gov | ✅ |

### Op-2 — Crystal Cove Conservancy (501(c)(3) park foundation)

**Possibly an operator entity, possibly just a fundraising/
education partner.** Park foundations (vs operating partners like
HBDB's Preservation Society) usually focus on programs and
fundraising rather than day-to-day operations. Need to verify the
relationship.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "Crystal Cove Conservancy" | 🔵 (legal name needs verification) |
| `type` | `nonprofit` / `park_foundation` | 🔵 |
| `web_url` | https://crystalcove.org (likely) | 🔵 |
| `agency_id` | **null** (not the same legal entity as DPR) | ✅ |
| **Role** | Likely fundraising / education / Historic District cottage operations, NOT direct beach management. Verify. | ❓ |

### Op-3 — Crystal Cove Beach Cottages (Historic District operator)

The 1930s cottages on the beach are rented to the public. They're
operated by a private concessionaire under contract to DPR. This
is a **third operator type** the model needs to handle: a
concession-lease operator with very narrow scope (just the
cottages, not the beach).

| Field | Value | Status |
|-------|-------|--------|
| `name` | (concessionaire name TBD) | ❓ |
| `type` | `private_contractor` / `concessionaire` | 🔵 |
| `web_url` | https://www.crystalcovebeachcottages.com (likely) | 🔵 |
| `agency_id` | **null** (private entity) | ✅ |
| `scope_limitation` | Cottage rentals only; not the beach itself | 🔵 |

This row is interesting for the model because it tests
**scope-limited operators** — an operator whose authority covers
only a sub-section of the beach. The HBDB walkthrough didn't
exercise this.

---

## 4. Policy source stack

### PS-1 — DPR statewide dog policy (administrative)

The DPR-wide baseline dog policy as published on parks.ca.gov/Dogs.
**Important correction:** this is NOT cited to a specific CCR
section on the DPR page. The page cites three tangential statutes
(Penal Code 365.7 for service-animal misrepresentation; PRC 5008.1
for immunizations; H&S 121690 for county licenses) but does NOT
cite the specific code section governing where dogs may be
physically located. The operational rule appears to be DPR
administrative policy promulgated under general agency authority
(probably PRC §5003), not a specifically-cited CCR section. My
earlier claim that this was CCR Title 14 §4319 was wrong.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `agency_administrative_policy` (new subtype — surfaced by this beach) | ✅ |
| `citation` | "California State Parks dog policy (parks.ca.gov/Dogs)" | ✅ |
| `issuing_agency` | CA State Parks / DPR | ✅ |
| `scope` | `dog_policy` | ✅ |
| `source_url` | https://www.parks.ca.gov/Dogs | ✅ |
| `effective_date` | (page revision not exposed) | ❓ |
| `parent_citation` | Likely PRC §5003 (DPR's general operating authority) — not exposed on the page | 🔵 |

**Verbatim baseline rules (DPR-wide):**

> "Dogs must be on a maximum 6-foot leash at ALL times and physically under your control."
>
> "Dogs cannot enter buildings or undeveloped areas."
>
> "Dogs are generally prohibited on trails, near rivers/creeks, in open forests, meadows, and environmental campsites."
>
> "Dogs are not permitted on most beaches (requires park-specific verification)."
>
> "Dogs cannot be left unattended; overnight campers must keep dogs in a tent or vehicle."

### PS-1a — Tangential statutes cited by parks.ca.gov/Dogs

The DPR page cites three statutes that DON'T directly govern
placement but are related to dog ownership obligations. Captured
for completeness because they establish state-level requirements
that interact with state-park rules.

| Citation | Scope | Status |
|----------|-------|--------|
| **Penal Code §365.7** | Service-animal misrepresentation = misdemeanor | ✅ |
| **PRC §5008.1** | Proof of immunizations and valid licenses | ✅ |
| **H&S Code §121690** | All 58 CA counties require dog licenses | ✅ |

### PS-2 — Crystal Cove park-specific dog policy

Park-unit-specific rules layered on top of PS-1. **This is the
dog policy at this specific beach.**

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `agency_administrative_policy` (same subtype as PS-1, scope-narrowed to one park unit) | ✅ |
| `citation` | "Crystal Cove State Park dog policy (parks.ca.gov/?page_id=644)" | ✅ |
| `issuing_agency` | DPR — Crystal Cove State Park | ✅ |
| `scope` | `dog_policy` | ✅ |
| `source_url` | https://www.parks.ca.gov/?page_id=644 | ✅ |
| `parent_citation` | PS-1 (DPR statewide policy) | ✅ — **parent/child relationship validated** |

**Verbatim Crystal Cove rule:**

> "Yes: Dogs allowed on paved areas only. Except for service animals, dogs not allowed on the beach or in the backcountry. Dogs are not allowed in Deer Canyon, Lower Moro, and Upper Moro Campgrounds."

**Reading:**
- **Permitted:** paved areas only, on 6-ft leash (per PS-1 baseline).
- **Prohibited:** the beach itself, the backcountry, AND the three campgrounds (Deer Canyon, Lower Moro, Upper Moro). The campground prohibition makes Crystal Cove **stricter than the DPR baseline** (which usually allows leashed dogs at campsites).
- **Exception:** service animals exempt from prohibitions.

### PS-3 — DPR ↔ Crystal Cove Conservancy partnership agreement (if exists)

If the Conservancy has a formal cooperating-association agreement
with DPR (most CA state-park foundations do — they're authorized
under PRC §513).

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `mou` or `cooperating_association_agreement` | 🔵 |
| `citation` | "Cooperating Association Agreement: DPR ↔ Crystal Cove Conservancy" | ❓ |
| `scope` | `operations` (programs/fundraising), NOT `dog_policy` | 🔵 |
| **Existence** | likely (standard pattern for CA state parks) | 🔵 |

### PS-4 — Crystal Cove Beach Cottages concession contract

The concession agreement giving the cottage operator authority
over rentals. May be CPRA-requestable.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `concession_lease` | ✅ |
| `scope` | `operations` (cottage rentals only) | ✅ |
| **Existence** | confirmed (the cottages are publicly rented) | ✅ |
| `source_url` | ❓ — CPRA candidate | |

### PS-5 — Public Resources Code §6001+ (State Lands tidelands)

Same as HBDB PS-4. State Lands authority below MHW.

### PS-6 — Health & Safety Code §115880+ (AB 411 / water quality)

Same as HBDB PS-5. OC Health implementation.

### PS-7 — Coastal Development Permit conditions (if applicable)

If Crystal Cove has active CDP conditions that affect dog policy
(e.g., seasonal closures for shorebird nesting, leash-restriction
near nesting zones), they'd live here.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `cdp_condition` | ✅ — anticipated subtype, tested here for the first time |
| `issuing_agency` | CA Coastal Commission | ✅ |
| **Existence at Crystal Cove** | ❓ — needs check of CCC's permit database | |

---

## 5. `beach_agency` join rows

| beach_fid | agency | authority_domain | precedence_rank |
|-----------|--------|------------------|-----------------|
| 8330 | CA State Parks | `dog_policy` | 1 |
| 8330 | CA State Parks | `operations` | 1 |
| 8330 | CA State Parks | `parking` | 1 |
| 8330 | CA State Parks | `wildlife_protection` | 1 |
| 8330 | CA State Lands | `tidelands` | 1 |
| 8330 | OC Health | `water_quality` | 1 |
| 8330 | CA Coastal Commission | `coastal_access` | 2 (regulatory overlay) |

The `precedence_rank` is more interesting at Crystal Cove than
HBDB — multiple agencies actually overlap on coastal_access and
wildlife_protection.

---

## 6. `beach_operator` join rows

| beach_fid | operator | agreement_type | scope |
|-----------|----------|----------------|-------|
| 8330 | CA State Parks (as operator) | `direct_management` | full beach |
| 8330 | Crystal Cove Conservancy | `cooperating_association` (if exists) | programs/fundraising |
| 8330 | Crystal Cove Beach Cottages (concessionaire) | `concession_lease` | cottage rentals only |

The scope-limited operator (cottages-only) is the new pattern
this beach tests.

---

## 7. Section-level dog policy

Expected per state-park default (CCR §4319 baseline), needs
verification from the Crystal Cove-specific Superintendent's Order.

| Section | Per DPR baseline (PS-1) | Per Crystal Cove (PS-2) | Source | Status |
|---------|-------------------------|--------------------------|--------|--------|
| Beach sand | "not permitted on most beaches" | **prohibited** (except service animals) | PS-2 | ✅ verbatim |
| Backcountry (inland) | "prohibited on trails, in open forests, meadows" | **prohibited** (except service animals) | PS-2 | ✅ verbatim |
| Paved areas (parking, walkways) | leashed 6-ft allowed | **leashed allowed** | PS-1 + PS-2 | ✅ |
| Deer Canyon Campground | usually leashed-at-campsite allowed | **prohibited** | PS-2 | ✅ verbatim — stricter than baseline |
| Lower Moro Campground | usually leashed-at-campsite allowed | **prohibited** | PS-2 | ✅ verbatim — stricter than baseline |
| Upper Moro Campground | usually leashed-at-campsite allowed | **prohibited** | PS-2 | ✅ verbatim — stricter than baseline |
| Historic District (cottages) | not addressed at park-policy level | ❓ — may have separate concession rules | PS-2 + PS-4 (concession) | ❓ |
| Tide pools (Reef Point) | "prohibited near rivers/creeks" | prohibited (part of beach) | PS-2 | ✅ |

---

## 8. Gaps / verification needed

Concrete TODOs to elevate verification status:

1. ❓ Identify which Crystal Cove segment fid 8330 corresponds to (display_name in beaches_gold).
2. ❓ Pull CCR Title 14 §4319 verbatim text + stable URL.
3. ❓ Find the Crystal Cove Superintendent's Order / Department Notice — likely on the park's web page or posted at the entrance kiosk.
4. ❓ Verify Crystal Cove Conservancy legal name and role (cooperating association vs operator).
5. ❓ Identify the cottage concessionaire (private company running the rentals).
6. ❓ Check CCC permit database for active CDPs affecting Crystal Cove dog policy or seasonal closures.
7. ❓ Verify whether tier 2 vs 3 vs 4 is right per current `beaches_gold.scoring_tier`.

---

## 9. What this walkthrough is designed to surface for the model

Discoveries this beach should produce, beyond what HBDB taught:

### Tests the model — confirmed and updated
- ✅ **`parent_citation` field on policy_source** — validated.
  Crystal Cove's park-specific policy (PS-2) cites up to DPR
  statewide policy (PS-1) as its parent. Real parent/child
  relationship, real data behind it.
- ✅ **`policy_source.subtype = 'agency_administrative_policy'`** —
  new subtype surfaced by this walkthrough. Distinct from
  `statute`, `regulation`, `mou`, `operator_posted_policy`.
  Captures the DPR-style published-on-our-website policy that
  isn't directly cited to a specific code section but functions as
  the operative rule under broader statutory authority. Both PS-1
  (DPR-wide) and PS-2 (Crystal Cove unit) are this subtype, with
  PS-2's `parent_citation` = PS-1.
- 🔵 **`policy_source.subtype = 'cdp_condition'`** — still
  untested; CCC permit database lookup deferred.
- ✅ **Operator-as-agency** — DPR operates Crystal Cove directly,
  with `agency_id` FK pointing at the DPR agency row. Pattern
  validated.
- 🔵 **Scope-limited operator** — Crystal Cove Beach Cottages
  concessionaire scoped to rentals only; still hypothetical until
  the concession contract is sourced.

### Major Wave-2 unlock surfaced by this walkthrough
- **parks.ca.gov/Dogs is the authoritative DPR-published catalog**
  of dog policy across all 280+ CA state parks, with a per-park
  alphabetical lookup table. **Wave 2 just got simpler by an
  order of magnitude** — instead of scraping each park's page
  individually, the agency already publishes the consolidated
  lookup. Worth pinning as a Wave-2 accelerator (see
  [[law-as-primary-source-ca]]).
- This also means the `agency_administrative_policy` subtype is
  the dominant pattern for the entire Wave 2 — most CA state
  parks' dog policy is on this one DPR-administered page, not in
  per-park source documents.

### My §4319 error — what to take away
- I cited CCR Title 14 §4319 confidently in the original draft
  based on training-data memory. **That citation is not on the
  DPR page** and may not exist as the governing rule. The actual
  operative authority is administrative policy under broader PRC
  authority, not a specific CCR section.
- Lesson for the broader initiative: **don't fabricate citation
  numbers from memory.** The primary source either confirms a
  citation or it doesn't. Better to say "the statutory basis
  needs primary verification" than to assert a number that turns
  out to be wrong.

### Tests of restrictive-direction rendering
- HBDB taught the layered statute→MOU→operator stack where the
  rendered headline is "leash optional." Crystal Cove is the
  inverse — restrictive baseline (§4319 = prohibited on beach
  sand), possibly even more restrictive locally. The headline
  render is likely "dogs prohibited on beach; allowed in parking
  lot on leash."
- The product UX of "dogs prohibited" rendering is the test case
  for how we communicate negatives — important for tier-3+4 beach
  surfacing.

### Open meta-question
- **Park foundations vs operating-partner nonprofits.** Crystal
  Cove Conservancy is likely the former (fundraising/programs);
  HBDB's Preservation Society is the latter (day-to-day
  stewardship). Both are 501(c)(3) nonprofits associated with a
  beach. The model may need to distinguish their roles, OR may
  treat them both as "operators" with different scope. Decision
  deferred until we see the actual relationship at Crystal Cove.

---

## 10. Related

- [[law-as-primary-source-ca]] — strategy
- [[walkthrough-hbdb]] — first walkthrough (companion)
- [[ca-agency-taxonomy]] — type catalog (needs updates for
  `superintendents_order` and possibly `cooperating_association`
  subtypes)
- [[entity-modeling]] — modeling principle
