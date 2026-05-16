# Walkthrough — Klamath Beach / Yurok Reservation (fid 561)

**Purpose:** ninth end-to-end beach walkthrough for the
[[law-as-primary-source-ca]] initiative. Tests **tribal sovereignty**
as an entirely new governance class. Tribal land is held in
federal trust; sovereign tribal nations have their own laws,
courts, and police forces. Neither city, county, state, nor
federal regimes apply the way they do on non-tribal land.

**Companions:** prior eight walkthroughs.

**Why this slot:** the prior eight walkthroughs covered city,
state park, federal NPS, county-as-operator (state-park-on-lease),
county-park-direct, and section-split joint-op. **Tribal land is
the major untested operator/agency class.** Tribal regulations
have the force of law on tribal land but operate under a
fundamentally different legal framework.

**Conventions:** same as prior walkthroughs.
- ✅ verified
- 🔵 unverified (training-data memory; needs source)
- ❓ unknown

---

## 1. Beach identity

| Field | Value | Status |
|-------|-------|--------|
| `fid` | 561 | ✅ |
| `name` | "Klamath Beach" | ✅ |
| `location_id` | `klamath-beach-del-norte` | ✅ |
| `park_name` | "Yurok Reservation" | ✅ |
| `county_name` | Del Norte | ✅ |
| `state` | CA | ✅ |
| `tier` | likely 2 or 3 — tribal regulations may permit dog access with conditions; needs verification | 🔵 |
| Location | Klamath River mouth, Del Norte County, far northern CA coast | 🔵 |
| Adjacent | Redwood National Park + Redwood State Park (the unified Redwood NSP), Tolowa Dunes State Park | 🔵 |

**Key fact:** the beach is on the Yurok Reservation. The Yurok
Reservation extends along the Klamath River from the coast (at
Requa) up the river. The mouth of the Klamath is on tribal land.

---

## 2. Agency stack

### Row A — Yurok Tribe (sovereign tribal government)

**The primary agency.** A sovereign nation under federal Indian
law. Has its own constitution, tribal council, courts, and police
force. **First non-city/county/state/federal agency in our model.**

| Field | Value | Status |
|-------|-------|--------|
| `name` | "Yurok Tribe" | ✅ |
| `type` | `tribal` (new agency type — first instance) | ✅ |
| `hierarchy` | ["United States (sovereign-equal)", "Yurok Tribe"] | ✅ — flat hierarchy; tribal nations aren't under state authority in the same way cities are |
| `authority_domains` | `dog_policy`, `operations`, `wildlife_protection`, `enforcement` (tribal police), `cultural_protection` | 🔵 |
| `web_url` | https://www.yuroktribe.org | 🔵 |
| `code_archive_url` | Yurok Tribal Code — likely published on yuroktribe.org or via NARF (Native American Rights Fund) | 🔵 |
| `tribal_council` | Elected; sovereign decision-making | 🔵 |
| `tribal_court` | Has its own court system | 🔵 |
| `pip_layer` | `tribal_lands` (BIA-maintained tribal-land polygons) | ✅ — exists per [[pipeline-instantiation]] |
| `bia_recognition` | Federally recognized tribe | ✅ |

**Authority subtleties:**
- On tribal land, **tribal law governs first** for civil matters
- **Federal law (e.g., Major Crimes Act)** governs some criminal matters
- **State law** generally does NOT apply on tribal land for tribal members (with exceptions)
- **PL-280** (which CA is part of) gives state law SOME criminal jurisdiction on most CA tribal lands — including Yurok — but generally NOT civil/regulatory jurisdiction
- So for dog policy: **Yurok tribal regulations are the operative source**, not state or federal

### Row B — Bureau of Indian Affairs (BIA, US Department of Interior)

**Federal trustee.** Holds tribal land in trust on behalf of the
US government for the tribe. Has approval authority over major
land-use changes but does NOT govern day-to-day operations.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "Bureau of Indian Affairs" | ✅ |
| `type` | `federal` | ✅ |
| `authority_domains` | `trust_oversight`, `major_land_use` | ✅ |
| `web_url` | https://www.bia.gov | ✅ |
| **Relevance** | Trustee role; not the operative source of dog policy | 🔵 |

### Row C — California State Lands Commission (limited applicability)

**Limited and contested.** State Lands generally has authority
over tidelands, but tribal nations dispute state authority on
tidelands adjacent to tribal land. The Yurok specifically have
claims to traditional fishing and use rights at the Klamath
mouth that conflict with state assertions.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "California State Lands Commission" | ✅ |
| **Relevance at this beach** | ❓ — possibly contested; State Lands may not be operative on tidelands directly adjacent to Yurok land | |

### Row D — Del Norte County (background only)

Surrounding county. Generally has no operative authority on
tribal land. Background.

### Row E — NPS / Redwood National Park (background; adjacent)

Redwood NP is adjacent but does NOT extend onto Yurok Reservation.
The Klamath River mouth area has been the subject of ongoing
discussions about cooperative management between NPS, CA State
Parks, and the Yurok Tribe.

### Row F — California Department of Fish and Wildlife (limited)

CDFW has marine resource authority offshore but tribal fishing
rights at the Klamath mouth significantly limit CDFW authority
for Yurok members. Public dog policy on the beach is more in
Yurok's domain than CDFW's.

---

## 3. Operator stack

### Op-1 — Yurok Tribe (as operator)

The Yurok Tribe operates the beach area as part of its
reservation. Various departments (Cultural Resources, Public
Safety, Natural Resources) handle different operational aspects.

| Field | Value | Status |
|-------|-------|--------|
| `name` | "Yurok Tribe" | ✅ |
| `type` | `tribal` (operating its own land) | ✅ |
| `agency_id` | FK → Row A (same legal entity) | ✅ |

### Op-2 — Possible cooperative-management partner

If the Klamath River mouth area has any cooperative management
agreement with NPS or CA State Parks (parts of Redwood NSP),
that'd appear here. Verify.

---

## 4. Policy source stack

### PS-1 — Yurok Tribal Code (sovereign law)

The tribe's codified law. **NEW SUBTYPE NEEDED.** Our existing
`tribal_resolution` subtype is tier 3 (agency_administrative_policy
level), but a codified tribal law has the force of statute on
tribal land. **Recommend adding `tribal_code` as a tier-1 subtype.**

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | **`tribal_code`** (new subtype — proposed addition to enum) | 🔵 |
| `citation` | "Yurok Tribal Code — Title TBD (Animal Control / Reservation Use)" | ❓ |
| `issuing_agency` | Yurok Tribe (Row A) | ✅ |
| `tier` | 1 (codified law on tribal land) | ✅ proposed |
| `source_url` | Yurok website or NARF database (TBD) | ❓ |

### PS-2 — Tribal council resolutions (tier 3 admin)

Specific resolutions issued by the Yurok Council on operational
matters. These would be tier 3 — admin policy under the umbrella
of the Tribal Code.

| Field | Value | Status |
|-------|-------|--------|
| `subtype` | `tribal_resolution` (existing subtype, tier 3) | ✅ |

### PS-3 — Yurok Tribe published rules / visitor info

Public-facing rules for visitors to Yurok land. May or may not
exist as a distinct webpage. Equivalent to operator-posted-policy.

### PS-4 — Federal CFR §25 (Indians)

Federal regulations covering Indian trust land. Background; not
directly governing dog policy.

### PS-5 — Possible CDFW Marine Life Refuge / MLPA boundary

If any CDFW marine protection extends to the Klamath mouth area;
likely contested.

---

## 5. Section-level dog policy (genuinely uncertain)

I have low confidence in the actual rules without sourcing. Some
tribes welcome visitors with dogs on leash; some restrict access;
some require permits. The Yurok specifically may have permit
requirements for certain areas (e.g., Requa lookout, fishing
sites).

| Section | Anticipated rule | Source | Status |
|---------|-----------------|--------|--------|
| Beach (sand) | likely `on_leash` with possible cultural-protection restrictions | PS-1 + PS-3 | ❓ low confidence |
| River mouth (sacred site / fishing area) | possibly restricted access | PS-1 + PS-3 | ❓ |
| Adjacent trails / inland | likely `on_leash` | PS-3 | ❓ |

---

## 6. `beach_agency` join rows (anticipated)

| beach_fid | agency | authority_domain | precedence_rank |
|-----------|--------|------------------|-----------------|
| 561 | Yurok Tribe | `dog_policy` | 1 (sovereign on tribal land) |
| 561 | Yurok Tribe | `operations` | 1 |
| 561 | Yurok Tribe | `enforcement` | 1 (tribal police) |
| 561 | Yurok Tribe | `cultural_protection` | 1 |
| 561 | BIA | `trust_oversight` | 2 (background) |
| 561 | (state/federal/county background) | various | 3+ |

---

## 7. Gaps / verification needed

1. ✅ fid confirmed (561)
2. 🔵 Yurok Tribe website verified at yuroktribe.org. JS-rendered
   (Wix-based) — automated fetch returns site chrome + mission
   statement but doesn't surface a visitor-rules / dog-policy
   page. Klamath Office contact: 190 Klamath Blvd, Klamath CA
   95548; phone (855) 559-8765.
3. ❓ Yurok Tribal Code section governing animal access — research
   path requires NARF database lookup OR direct tribal contact.
   Deferred. The structural model wins (tribal_code subtype,
   tribal agency type) don't depend on this specific rule.
4. ❓ Whether Yurok permits/restricts dog access at Klamath beach
   specifically — same deferral.
5. ❓ Cultural-protection areas and dog access — same deferral.
6. ❓ Any cooperative-management agreement with NPS / CA State Parks
   — Redwood NSP is adjacent; pursuit deferred.
7. ❓ Public-facing rules / visitor info — Yurok site uses Wix
   SPA; can't extract via basic Playwright. Either deeper site
   navigation or phone call needed.

---

## 8. What this walkthrough is designed to surface

### NEW agency / operator classes — `tribal`

- **`agency.type = 'tribal'`** as a first-class agency type.
  Distinct from city, county, state, federal because of sovereign
  status under federal Indian law.
- **`operator.type = 'tribal'`** parallel.
- **Tribal agencies have a flat hierarchy** with the federal
  government (sovereign-equal), not a nested one under state law.
  The `hierarchy` field on `agency` may need to allow this shape.
- **PL-280 jurisdictional split** — CA is one of the PL-280 states
  where state law has SOME criminal jurisdiction on tribal land
  but generally not civil/regulatory. The model may need to
  represent this nuance for fields where jurisdictional split
  matters (less so for dog policy; more for things like
  fishing/hunting).

### NEW `policy_source` subtype: `tribal_code`

- Our existing `tribal_resolution` is tier 3 (agency_admin level).
- **A codified tribal code has the force of statute on tribal
  land.** It should be tier 1.
- Recommend adding `tribal_code` as a new subtype at tier 1.
- `tribal_resolution` stays as tier 3 for administrative
  decisions under the code's umbrella.

### Contested-jurisdiction question

State Lands' authority on tidelands adjacent to tribal land is
genuinely contested. For our purposes (dog policy on a beach),
this probably doesn't matter — tribal authority is operative on
the beach. But it's a real legal complexity to be aware of.

### Open architectural question

- Should "tribal land" be modeled as a sub-region of a beach
  (when a beach straddles tribal and non-tribal land — common at
  reservation boundaries)? Probably needs section-level rules
  with a per-section tribal-vs-state operating-status indicator.
  This walkthrough doesn't fully test that because Klamath Beach
  appears to be entirely on Yurok land per the park_name.

---

## 9. Related

- [[law-as-primary-source-ca]]
- prior eight walkthroughs
- [[consensus-source-authority]]
- [[entity-modeling]]
- [[no-human-visibility-principle]] — tribal members are part of
  the entity hierarchy but stay invisible at the consumer
  surface; the tribe is the entity, not its officers
