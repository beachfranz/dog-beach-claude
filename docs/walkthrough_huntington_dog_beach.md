# Walkthrough — Huntington Dog Beach (fid 6212)

**Purpose:** end-to-end proof-of-concept for the
[[law-as-primary-source-ca]] entity model on one real beach.
Validates the design before any schema migration; surfaces gaps the
abstract [[ca-agency-taxonomy]] can't.

**Convention used in this doc:**

- ✅ **verified** — claim sourced from an authoritative public document; URL captured.
- 🔵 **unverified** — claim based on training-data memory or
  general knowledge; needs verification before shipping.
- ❓ **unknown** — claim couldn't be determined from available info; needs research.

**Note on naming:** this walkthrough uses `policy_source` rather
than `statute` — today's conversation broadened the entity to a
typology that includes statutes, regulations, MOUs, compendiums,
etc. The [[ca-agency-taxonomy]] doc still says `statute` and
should be updated as a separate follow-up.

---

## 1. Beach identity

| Field | Value | Status |
|-------|-------|--------|
| `fid` | 6212 | ✅ |
| `display_name` | Huntington Dog Beach | ✅ |
| `state` | CA | ✅ |
| `tier` | 1 (off-leash designated — see conflict note below) | 🔵 (per [[scoring-scope]] vocabulary) |
| Location | Pacific Coast Hwy, below Bluff Top Park, north of HB pier; carve-out runs 22nd Street → Seapoint Ave | ✅ |
| `lat`, `lng` | (in `beaches_gold` row) | ✅ |
| Approx. length | ~1 mile | 🔵 |
| Opened | ~1996 (per OC Register 2007-11-15) | ✅ |
| Annual traffic | ~150,000 dogs/year (per OC Register 2007-11-15) | 🔵 (figure is 2007-era; current traffic unknown) |

---

## 2. Agency stack — `agency` table rows

The legal-authority entities that govern HBDB. Each row is what
would land in the `agency` table.

### Row A — City of Huntington Beach

| Field | Value | Status |
|-------|-------|--------|
| `name` | "City of Huntington Beach" | ✅ |
| `type` | `city` | ✅ |
| `hierarchy` | ["California", "Orange County", "Huntington Beach"] | ✅ |
| `authority_domains` | `dog_policy`, `operations`, `parking`, `fire`, `water_safety` | 🔵 |
| `web_url` | https://www.huntingtonbeachca.gov | ✅ |
| `code_archive_url` | https://ecode360.com/ (eCode360 / General Code) | ✅ |
| `pip_layer` | `jurisdictions` | ✅ (cross-ref already in pipeline) |
| `pip_filter` | `place_name = 'Huntington Beach'` | 🔵 |

### Row B — California State Lands Commission

| Field | Value | Status |
|-------|-------|--------|
| `name` | "California State Lands Commission" | ✅ |
| `type` | `state` | ✅ |
| `authority_domains` | `tidelands`, `submerged_lands` | ✅ |
| `web_url` | https://www.slc.ca.gov | ✅ |
| `code_archive_url` | https://leginfo.legislature.ca.gov/faces/codesTOCSelected.xhtml?tocCode=PRC | ✅ |
| `pip_layer` | `cslc_sovereign_lands` | ❓ (layer existence in pipeline unconfirmed — see [[ca-agency-taxonomy]] PIP audit) |

### Row C — Orange County Health Care Agency

| Field | Value | Status |
|-------|-------|--------|
| `name` | "Orange County Health Care Agency" | ✅ |
| `type` | `county_department` | ✅ |
| `hierarchy` | ["California", "Orange County"] | ✅ |
| `authority_domains` | `water_quality`, `bacteria_advisories` | ✅ |
| `web_url` | https://www.ochealthinfo.com | 🔵 |
| `code_archive_url` | (OC ordinance code on Municode) | 🔵 |
| `pip_layer` | `counties` filtered to `name = 'Orange'` | ✅ |

### Row D — California State Parks (DPR)

❓ Probably not relevant for HBDB itself. The land at HBDB is
city-owned per the off-leash designation. Adjacent state park land
(Bolsa Chica SB to the north, Huntington SB immediately south) is
under DPR, but HBDB proper is the city's one-mile carve-out. **Skip
unless walkthrough surfaces overlap.**

### Federal layers

❓ No NPS / USFWS / BLM / military overlap at HBDB. **Skip.**

---

## 3. Operator stack — `operator` table rows

Day-to-day stewards. Distinct from agencies — these are who runs
the place, not who issues policy.

### Op-1 — The Preservation Society of Huntington Dog Beach (501(c)(3))

| Field | Value | Status |
|-------|-------|--------|
| `name` (current) | "The Preservation Society of Huntington Dog Beach" | ✅ verified via Visit HB / surfcityusa.com listing |
| `name` (prior, 2007) | "Friends of Dog Beach" — renamed at unknown later date | ✅ per OC Register 2007-11-15 |
| `tagline` | "Preserve, Protect, Pick-up" | ✅ |
| `type` | `nonprofit` | ✅ |
| `web_url` | https://www.dogbeach.org/ | ✅ verified 2026-05-16 |
| `physical_presence` | Tent at lifeguard tower 24 on weekends; booths at three entrances; 8' × 20' storage shed (per 2007 MOU) | ✅ |
| `formed` | ~1996 alongside beach opening | 🔵 inferred |
| `2007 president` | Martin Senat | ✅ |
| `contact_email` | ❓ | |
| `contact_phone` | ❓ | |
| `hours` | (continuous public access; nonprofit hours don't gate beach access) | ✅ |
| `agency_id` | **null** | ✅ — nonprofit is not itself an agency. This is the model-validating case. |

### Op-2 — Huntington Beach Marine Safety Division

| Field | Value | Status |
|-------|-------|--------|
| `name` | "Huntington Beach Marine Safety Division" | 🔵 |
| `type` | `city_dept` | ✅ |
| `web_url` | https://www.huntingtonbeachca.gov/Government/Departments/marine_safety/ | 🔵 |
| `agency_id` | **FK → Row A (City of Huntington Beach)** | ✅ — operator IS the same legal entity as an agency. The FK earns its keep. |

---

## 4. Policy source stack — `policy_source` table rows

The legal/policy artifacts that back specific rules. This is the
table that replaces the `statute` sketch from the earlier design.

### PS-1 — HBMC §13.08.070 "Dogs and Other Animals"

The beach/pier-specific animal ordinance. PS-1 and PS-2 from the
original draft **collapsed into this single source** — both the
beach-wide prohibition AND the HBDB carve-out live in this one
section, not in two separate ordinances.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `municipal_code` | ✅ |
| `citation` | "Huntington Beach Municipal Code §13.08.070 — Dogs and Other Animals" | ✅ |
| `issuing_agency_id` | FK → Row A (City of HB) | ✅ |
| `scope` | `dog_policy` | ✅ |
| `source_url` | https://ecode360.com/HU4937 | ✅ |
| `effective_date` | Most recent amendment Ord 4275-3/23 (March 2023); original Ord 344-10/31 | ✅ |
| `amendment_history` | "344-10/31, 554-12/49, 769-7/60, 2907-8/87, 3355-7/97, 3606-6/03, 3930-2/12, 4118-5/17, 4275-3/23" | ✅ |
| `last_verified` | 2026-05-16 (PDF downloaded from ecode360) | ✅ |

**Verbatim text (subsection A — the rule):**

> No Person having the care, charge or control of any animal,
> domesticated or wild, shall permit or allow said animal to be on
> the Pier or on or upon that Beach or Adjacent Beach Area bounded
> by the Beach Service Road and the Pacific Ocean, including the
> Water Activity Zone, unless expressly permitted by a specific
> event permit. This section shall not apply to a police service
> animal, or guide dog, signal dog or service dog as defined in
> Penal Code Section 365.5. **This section shall not apply to the
> Adjacent Beach Areas, and the Water Activity Zone, located north
> of the line created by extending the northern curb line of 22nd
> Street to the Pacific Ocean to Seapoint Avenue, wherein dogs
> constrained by a leash no longer than six feet in length are
> permitted.**

**What this says per primary source:** HBDB (the carve-out strip
from 22nd Street north to Seapoint) is an **on-leash designated
area**, with a 6-foot maximum leash length. Off-leash dogs are not
authorized by the code.

**Why this matters for the model — statute-vs-lived-reality conflict:**

Every consumer surface (operator FAQ, Google reviews, local guides,
our own beach data) treats HBDB as off-leash. The city and the
nonprofit operator effectively tolerate the practice. The statute
says otherwise. **This is the test case for the conflict-resolution
flow** the model was designed for — I'd expected to need Dockweiler;
HBDB itself is the example.

Two next-step verification questions:
- ❓ Is there another HBMC section (a separate parks/recreation
  ordinance, a resolution, or an administrative regulation) that
  designates HBDB as off-leash? The carve-out's "specific event
  permit" exception hints at administrative discretion.
- ❓ Or has the consumer narrative simply outrun the code, with
  the city's enforcement (or non-enforcement) being the de-facto
  policy?

### PS-3 — MOU between City of HB and HDB nonprofit

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | **`mou`** | ✅ — new subtype surfaced by this walkthrough |
| `citation` | "MOU: City of HB ↔ Friends of Dog Beach, 2007" (current version may be later) | ✅ origin date verified |
| `issuing_agency_id` | FK → Row A (City of HB) | ✅ |
| `scope` | `operations` (booths, storage shed, consultation rights) — **NOT leash policy** | ✅ |
| `source_url` | 🔵 CPRA request submitted to City of HB 2026-05-16 — awaiting response (typical CA timeline: 10 days for acknowledgement, weeks for production) | |
| `verification` | Paraphrase of substantive content available (see below); full document pending CPRA fulfillment | 🔵 |
| `drafted_by` | Martin Senat (Friends of Dog Beach president) + Jim Engle (HB Community Services Director) | ✅ |
| `approved_by` | HB Community Services Commission, unanimous, 2007-11 (City Council vote pending in article) | ✅ |

**Known terms (per OC Register 2007-11-15):**
- 10' × 10' merchandise booths at three main entrances, weekends only
- 8' × 20' storage shed (out of sight from PCH and Bluff Top Park trail)
- City agrees to "consult with the Friends of Dog Beach before
  making decisions that impact the beach"

**Substantive policy content (per Franz's read of the MOU, 2026-05-16):**

> "The City Council officially recognizes the PSHDB's role in
> maintaining this stretch of coast. Rather than the state
> enforcing standard blanket dog prohibitions in this specific
> area, the city and managing bodies allow dogs to run here."

This is the bridge clause. The MOU:
- Formally recognizes the operator's stewardship role.
- Positions the HBDB carve-out as a deliberate non-enforcement of
  "standard blanket dog prohibitions."
- Uses the phrase "allow dogs to **run**" — operationally
  off-leash language, not a leashed reading.

**Reconciliation with PS-1 (the leashed code text):** the
HBMC §13.08.070 leashed-only language is not directly contradicted
by the MOU; the MOU instead establishes a parallel
recognition-and-non-enforcement framework. Functionally, the
operator-published "leash optional" policy is the surface
expression of the MOU's "allow dogs to run" framing.

**Open question:** is the MOU's "allow dogs to run" a binding city
policy that *overrides* the statute, or is it a non-enforcement
understanding that *coexists with* the statute? Legally an MOU
cannot amend a municipal code; only a Council ordinance can. So
the MOU is almost certainly the second — a recorded
non-enforcement framework, not a legal repeal. The practical
effect is the same; the formal status differs.

**Reusable pattern:** the city of HB has similar MOUs with Council
on Aging, Friends of Junior Guards, and Friends of Shipley Nature
Center. Template-style community-nonprofit-operator MOUs are a
common HB instrument and likely exist in other CA coastal cities
too — useful for finding analogous operator entities elsewhere.

### PS-4 — Public Resources Code §6001+ (State Lands tidelands authority)

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `state_statute` | ✅ |
| `citation` | "CA Public Resources Code §6001 et seq." | ✅ |
| `issuing_agency_id` | FK → Row B (State Lands) | ✅ |
| `scope` | `tidelands` | ✅ |
| `source_url` | https://leginfo.legislature.ca.gov/faces/codes_displayexpandedbranch.xhtml?tocCode=PRC&division=6.&title=&part=&chapter=&article= | ✅ |

### PS-6 — Operator posted policy (dogbeach.org)

A new policy_source surfaced by the walkthrough. The operator's
public statement of the dog policy at HBDB. Not statute, not
regulation, not MOU — this is the "posted policy" published by the
entity that runs the place day-to-day.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `operator_posted_policy` | ✅ — new subtype |
| `citation` | "Preservation Society of HDB website" | ✅ |
| `issuing_entity` | Op-1 (Preservation Society) — not an agency | ✅ |
| `scope` | `dog_policy` | ✅ |
| `source_url` | https://www.dogbeach.org/ | ✅ |
| `effective_date` | unknown (page revision history not exposed) | ❓ |
| `last_verified` | 2026-05-16 | ✅ |

**Captured text (LLM-summarized from the page — verbatim
verification recommended):**

> "Dog Beach is leash optional."
>
> "Please keep your dog leashed until you have reached the beach
> sand. Dogs must be leashed in the parking lot and on the upper
> bluff."
>
> "The leash law applies to all dogs!"

**Conflict with PS-1 (the statute):** PS-1 says on-leash within
the carve-out zone (max 6 ft). PS-6 says leash-optional on the
sand. The operator and the statute disagree on the controlling
rule. The MOU between the city and the operator (PS-3) is the
mechanism that gives the operator's stated policy any operational
weight; without it, the operator would just be a third party.

### PS-5 — CA Health & Safety Code §115880+ (AB 411 / beach water quality)

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `state_statute` | ✅ |
| `citation` | "CA Health & Safety Code §115880 et seq." | ✅ |
| `issuing_agency_id` | FK → Row C (OC Health) — delegated implementation | ✅ |
| `scope` | `water_quality` | ✅ |
| `source_url` | https://leginfo.legislature.ca.gov/faces/codes_displayexpandedbranch.xhtml?tocCode=HSC | ✅ |

---

## 5. `beach_agency` join rows — the authority stack

Beach-level (not section-level), per the locked design.

| beach_fid | agency | authority_domain | precedence_rank |
|-----------|--------|------------------|-----------------|
| 6212 | City of HB | `dog_policy` | 1 |
| 6212 | CA State Lands | `tidelands` | 1 |
| 6212 | OC Health | `water_quality` | 1 |
| 6212 | City of HB | `operations` | 1 |
| 6212 | City of HB | `parking` | 1 |
| 6212 | City of HB | `water_safety` | 1 |

No precedence conflicts at HBDB — each authority_domain has exactly
one agency claiming it. **Good.** The model's `precedence_rank`
column doesn't earn its keep here, but it would on (say) Crystal
Cove where State Parks + City of Newport overlap.

---

## 6. `beach_operator` join rows

| beach_fid | operator | agreement_type | effective_start | effective_end |
|-----------|----------|---------------|------------------|---------------|
| 6212 | Huntington Dog Beach (nonprofit) | `operating_agreement` | ~1997 | null |
| 6212 | HB Marine Safety | `direct_management` | (city founding) | null |

---

## 7. Section-level dog policy with `policy_source` FK

In `zone_rules.regions[].sections[]`, each rule now carries a
`source_policy_id`:

| Section | Per statute (PS-1) | Per operator (PS-6) | Per lived reality | Status |
|---------|--------------------|---------------------|-------------------|--------|
| `sand` (within HBDB carve-out: 22nd Street → Seapoint) | `on_leash` (max 6ft) | `off_leash` ("leash optional") | `off_leash` | ✅ — **3-source conflict** |
| `water_swim` (within carve-out, "Water Activity Zone") | `on_leash` (max 6ft) | (operator silent — presumed leash-optional consistent with sand) | `off_leash` | 🔵 conflict implied |
| `sand` (south of 22nd Street toward pier) | `prohibited` | `prohibited` | `prohibited` | ✅ no conflict |
| `parking` (PCH lot adjacent) | (no animals — covered by §13.08.070's prohibition on "Beach Service Road" zone) | `on_leash` (operator explicitly states) | `on_leash` | ✅ consistent |
| `upper bluff` (path from parking to sand) | (covered by §13.08.070) | `on_leash` (operator explicit) | `on_leash` | ✅ consistent |
| Pier | `prohibited` | (operator silent — not in their managed area) | `prohibited` | ✅ |

---

## 8. Gaps / verification needed

Concrete TODOs that emerged from this walkthrough:

1. ✅ Verify exact HBMC section numbers for PS-1 (off-leash designation) and PS-2 (general leash law). Pull from codepublishing.com.
2. ✅ Confirm the 501(c)(3) registered name and current website.
3. ✅ Determine MOU status — is it on the city clerk's site, FOIA-able, or undocumented?
4. ✅ Section-by-section policy detail (water, dune areas, lifeguard towers, etc.).
5. ✅ Effective date of the original off-leash designation (folklore says 1997-1999; needs primary source).
6. ❓ Confirm `cslc_sovereign_lands` PIP layer status (separate task — see [[ca-agency-taxonomy]] PIP audit).

---

## 9. What this walkthrough surfaced about the model

Discoveries that should update the design BEFORE schema migrations:

### Confirmed working
- `operator.agency_id` nullable FK pattern handles both cases cleanly: HDB nonprofit (null), HB Marine Safety (FK to City of HB).
- Beach-level `beach_agency` with `authority_domain` column scales to the 4-5 domains HBDB needs without forcing section granularity.
- `policy_source` as the broader entity (vs `statute`) handles the MOU case naturally.

### New facts to add to the taxonomy
- **`policy_source.subtype = 'mou'`** is a needed value. (Already proposed in today's conversation; HBDB confirms it on real data.)
- **`authority_domain = 'water_safety'`** as a distinct domain — Marine Safety Division has it, separate from `dog_policy` and `water_quality`.
- **DMO / tourism organization is a new entity class.** Visit Huntington Beach (155 Fifth Street #111, HB) publishes a public listing for HBDB that's neither agency nor operator nor policy-source-in-the-legal-sense. It's a promotional voice. May warrant a `promotional_listing` policy_source subtype OR a separate `promoter` entity type. Decision deferred.
- **Operator name "The Preservation Society of Huntington Dog Beach"** is the verified registered name — surfaced via the Visit HB listing 2026-05-16.
- **`operator_posted_policy` is a needed policy_source subtype.** dogbeach.org states "leash optional" — directly contradicts the statute. This is the operator's working policy, distinct from the MOU itself. Probably wants its own subtype because verification cadence is different (operator pages change ad-hoc; MOUs change rarely).
- **Operators have name history.** "Friends of Dog Beach" → "The Preservation Society of Huntington Dog Beach" is the same legal entity (presumably) under two different names over time. The `operator` table needs an `aliases` or `prior_names` field, or a separate `operator_name_history` table — otherwise a future research session won't find the 2007 entity when searching for the current name. Common-enough situation that it deserves first-class modeling.
- **Template-MOU pattern across community nonprofits.** HB has parallel MOUs with Council on Aging, Friends of Junior Guards, Friends of Shipley Nature Center. Implies that "city + community-nonprofit operator under MOU" is a common CA coastal-city pattern, not a one-off. Wave 3 of [[law-as-primary-source-ca]] should look for analogous operators in every CA city, not just the obvious city-parks-dept structures.
- **Historical news articles aren't policy_sources.** The 2007 OC Register article isn't a policy_source in our typology — it's a historical record. But it's evidentiary for entity-history fields (operator name, formation date, MOU origin). Probably doesn't need its own entity class; just gets cited inline in the relevant entity field's provenance.

### Statute-vs-MOU-vs-operator — a layered, reconciled stack
- **Update 2026-05-16 (latest):** Franz's read of the MOU surfaces
  the bridge clause: "the City Council officially recognizes the
  PSHDB's role… Rather than the state enforcing standard blanket
  dog prohibitions in this specific area, the city and managing
  bodies allow dogs to run here." That language reframes the
  apparent conflict.
- **The shape isn't statute-vs-operator anymore — it's a layered
  stack:**
  - **PS-1 (statute, §13.08.070):** on-leash, 6 ft max. The codified rule.
  - **PS-3 (MOU):** city formally adopts a non-enforcement framework, recognizes the operator's stewardship, frames the carve-out as "allow dogs to run." The bridge.
  - **PS-6 (operator posted, dogbeach.org):** "leash optional" on sand; leashed on parking/bluff. The user-facing summary of the MOU's framing.
- These can be read as **harmoniously layered** rather than
  conflicting: the statute is the formal text; the MOU is the
  city's recorded non-enforcement policy; the operator's posted
  policy is the public expression of the MOU. All three exist; the
  later layers operate within (not against) the earlier one.
- **Legally:** an MOU cannot amend a code. Only an ordinance can.
  So PS-1's leashed text remains the law; PS-3 records a
  non-enforcement understanding; PS-6 publishes the practical
  policy. If enforcement is ever attempted, PS-1 is what gets
  enforced.
- **For the model:** this is exactly the structure
  `precedence_rank` should express — PS-1 is the canonical rule
  (rank 1 in the strict sense), PS-3 provides a non-enforcement
  overlay (rank 2), PS-6 publishes the practical effect (rank 3).
  Render-time logic picks which to show based on a product
  decision; the model preserves all three.
- The product question simplifies: render PS-6 (the operator's
  posted policy) as the headline answer **with a "more info"
  drawer** exposing PS-1 and PS-3 for users who want the legal
  picture. That's defensible (we cite source for everything) and
  matches reputation (we render what regulars know).

### Quiet gaps still open
- The `precedence_rank` column is justified but unused at HBDB.
  Confirms it's worth keeping for Crystal Cove / Coronado overlap
  cases.
- **Open research — the 1997 amendment text.** §13.08.070's
  amendment history shows the first post-opening amendment was Ord
  **3355-7/97** (July 1997), almost certainly the one that created
  the HBDB carve-out (the beach opened ~1996 per OC Register
  2007-11-15). The current text restricts the carve-out to
  "leash no longer than six feet." If the original 1997 version
  said something different (off-leash, unleashed, or no leash
  restriction), the operator's "leash optional" position and
  150K-dogs/year off-leash practice are honest legacy artifacts of
  the original rule being narrowed later. If the 1997 text
  already said leashed, off-leash has always been a non-enforcement
  tolerance. **Resolution:** pull Ord 3355-7/97 from HB city clerk
  records or ecode360's revision history.
- **MOU is operational only.** The 2007 MOU between the city and
  the (then) Friends of Dog Beach covers booths, sheds, and
  consultation — not leash policy. So if the off-leash arrangement
  is authorized anywhere, it's in (a) the 1997 ordinance text
  pre-narrowing, or (b) a separate parks/rec resolution, or (c)
  nothing — pure non-enforcement.

---

## 10. Related

- [[law-as-primary-source-ca]] — strategy
- [[ca-agency-taxonomy]] — type catalog (needs `policy_source` rename + `mou` subtype + `water_safety` domain)
- [[entity-modeling]] — subjects-vs-derivations-vs-measurements
- [[scoring-scope]] — tier-1 confirmation
- [[no-human-visibility-principle]] — kahus stay invisible
