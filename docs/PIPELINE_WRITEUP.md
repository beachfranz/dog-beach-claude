# How we classified 668 California beaches

A technical walk-through of the jurisdiction + operational-data pipeline behind Dog Beach Scout.

## TL;DR

Starting from a raw list of 668 California beach POIs (name + lat/lon, nothing else), we produced:

- **623 classified records** (93%) with governing authority at the correct tier — federal, state, county, or city
- **552 with strong deterministic evidence** (89% of classified), from six authoritative polygon / GIS sources
- **621 with actionable dog policy** (99.7% of classified) — researched from web sources, with `yes / no / mixed / seasonal` values plus per-beach allowed/prohibited area text
- **609 with parking data** (98%), 421 with restrooms (68%), 405 with hours (65%), 288 with lifeguard presence (46%)
- **37 records correctly flagged invalid** (businesses, private land, non-public) and 8 duplicates detected via spatial+name similarity

Total pipeline runtime: ~6 minutes for jurisdiction classification + ~10 minutes for operational enrichment across 186 unique governing bodies.

---

## The problem

Jurisdiction is the key to everything. Who sets the dog rules at a beach? That answer depends on who actually operates it, which is one of:

1. **Federal agency** — NPS, USFS, BLM, DOD, FWS, BIA (tribal), USBR, USACE, USAF, Navy, Space Force
2. **State** — California State Parks, California Coastal Commission, UC natural reserves, CDFW, California Tahoe Conservancy
3. **County** — LA County Beaches & Harbors, Marin County Parks, Sonoma County Regional Parks, etc.
4. **City** — whoever runs Rosie's Dog Beach in Long Beach, who decides the 9am curfew at Del Mar, etc.

Tides further complicate it: the beach itself is usually state tidelands even when an adjacent city operationally manages it under a lease. So "where are the coordinates?" is a different question from "who decides the rules?"

## Architectural evolution: AI → polygon

First pass used proximity + name-matching against NPS's places API and California State Parks entry points. This got us about 73% coverage (491 of 668) but a lot of the classifications were heuristic and wrong in annoying ways. Classic example: **Corona del Mar State Beach** — our keyword matcher saw "State Beach" in the name and classified it as governing state, but it's actually operated by the City of Newport Beach under a concession agreement. Name-based signals are fundamentally unreliable for this class of problem.

The breakthrough was shifting to **polygon point-in-polygon matching** against authoritative GIS boundaries, with the AI demoted from primary classifier to audit-only. Each agency draws its own boundary; a beach is either inside a polygon or it isn't. No similarity thresholds, no false positives from name collisions.

## Data sources

| Source | Scope | Used for |
|---|---|---|
| [Census TIGER/Line Places](https://tigerweb.geo.census.gov/) | US incorporated cities (polygons) | City jurisdiction |
| [USA Federal Lands](https://services.arcgis.com/P3ePLMYs2RVChkJx/) (Esri Living Atlas) | NPS, USFS, BLM, DOD, FWS, USBR nationally (polygons) | Primary federal classifier |
| [CA State Parks ParkBoundaries](https://services2.arcgis.com/AhxrK3F6WM8ECvDi/) | All CA state park units (462 polygons) | State jurisdiction |
| [CA Protected Areas Database (CPAD)](https://gis.cnra.ca.gov/) | All protected lands in CA by agency level (polygons) | County parks, non-profit land, special districts |
| [BLM CA Land Status SMA](https://gis.blm.gov/caarcgis/) | CA federal surface management w/ agency codes | Gap-fill for federal (catches BIA tribal, USFS, BLM NCAs our primary missed) |
| [US Census Incorporated Places API](https://geocoding.geo.census.gov/) | Point-lookup for incorporated place | Context for the city-polygon buffer |
| [Google Maps Geocoding](https://maps.googleapis.com/) | Reverse-geocode to street/city/county | Address context |
| [CA Coastal Commission Public Access Points](https://services9.arcgis.com/wwVnNW92ZHUIr0V0/) | 1,631 curated CA coastal access points | Amenity cross-reference + dog-friendly hints |
| [Anthropic Claude Haiku 4.5](https://www.anthropic.com/) | LLM | Audit + operational data extraction |
| [Tavily Search](https://tavily.com/) | Web search API | Surface authoritative pages per governing body |

## Pipeline stages

Eleven classification stages plus operational enrichment, all runnable independently and orchestrated by `v2-run-pipeline`:

```
 1. v2-dedup                     spatial + name similarity within 50m
 2. v2-non-beach-filter          regex: 'deli', 'psychic', 'garage', '& Bar',
                                 'surf school/camp', 'HOA', street suffixes
 3. v2-geocode-context           Google reverse geocode + Census point lookup
 4. v2-federal-classify          USA Federal Lands point-in-polygon
                                 (skips Santa Monica Mountains NRA — mixed mgmt)
 5. v2-state-classify            CA State Parks ParkBoundaries point-in-polygon
 6. v2-state-operator-override   curated table of state-owned, city/county-operated
                                 leases (Santa Monica SB, Corona del Mar SB,
                                 Leucadia SB, Moonlight SB, Will Rogers SB →
                                 LA County, etc.)
 7. v2-ccc-crossref              CCC public-access-point match within 200m
 8. v2-city-classify             Census TIGER Places point-in-polygon,
                                 plus a ~100m buffer pass for tideland-adjacent
 9. v2-county-classify           CPAD point-in-polygon; county→county evidence,
                                 federal/state→tier fix, non-profit/special-district/
                                 HOA→invalid
10. v2-default-county            residual county default + neighbor-inherit
                                 from trusted-source neighbors within 200m
11. v2-state-name-rescue         name contains 'State Beach/Park' → state
12. v2-county-name-rescue        name contains 'County Park/Regional Park' → county
13. v2-non-beach-late            rivers, broad regions ('Lost coast'), 'The Wall'
14. v2-private-land-filter       Del Monte Forest bbox + '17-Mile Drive' CCC name
15. v2-blm-sma-rescue            BLM CA SMA for federal gaps + private-land flag
16. v2-ai-audit                  Claude independently assesses every classification

-- Operational enrichment --
17. v2-ccc-enrich                pull CCC amenity fields into structured columns
18. v2-enrich-operational        per-tier Tavily + Claude: dog policy, parking,
                                 hours, amenities, confidence
```

## Classification results

**623 of 668 beaches classified (93%), 45 excluded (37 invalid, 8 duplicate).**

By source (stronger at top, weaker at bottom):

| Source | Count | Evidence type |
|---|---|---|
| `city_polygon` | 293 | Census TIGER point-in-polygon |
| `state_polygon` | 123 | CA State Parks point-in-polygon |
| `federal_polygon` | 62 | USA Federal Lands point-in-polygon |
| `state_operator_override` | 20 | Curated lease table |
| `city_polygon_buffer` | 17 | TIGER polygon + 100m buffer |
| `county_polygon` | 16 | CPAD county polygon |
| `cpad_state` | 7 | CPAD state polygon (gap-fill) |
| `cpad_federal` | 6 | CPAD federal polygon (gap-fill) |
| `county_name_rescue` | 3 | Name contains "County Park" |
| `state_name_rescue` | 3 | Name contains "State Beach/Park" |
| `blm_sma_federal` | 2 | BLM CA SMA (BIA tribal lands) |
| **Strong polygon evidence** | **552** | **89%** |
| `county_default` | 71 | Residual → county of geocode |

## AI audit — an independent sanity check

After classification, Claude Haiku is fed each beach's name, city, county, and Census place, and asked independently whether the classification makes sense. Agreement rates by source:

| Source | Agree % | Disagree % | Interpretation |
|---|---|---|---|
| `city_polygon` | **90%** | 8% | Census-driven city polygons near perfect |
| `federal_polygon` | **79%** | 19% | NPS / military mostly right; disagrees are often AI not recognizing obscure BLM land |
| `state_polygon` | 78% | 21% | Big Sur state park boundaries; disagrees tend to be city-operated state beaches (see operator override below) |
| `city_polygon_buffer` | 76% | 24% | 100m buffer expected to be less clean |
| `county_default` | 13% | 83% | **Expected** — "county" was the residual for beaches we had no positive evidence for; Claude often disagrees and suggests a more specific answer |

The county_default disagreement rate is a feature, not a bug: it means the AI audit is correctly flagging that our weakest classifications are, indeed, the weakest. Follow-up stages (name-rescues, CPAD, BLM-SMA) then target that disagree population specifically.

## Operational data — dog policy, parking, hours, amenities

For every unique governing body (186 of them: 22 federal, 70 state, 64 city, 30 county), a single Tavily + Claude Haiku research pass extracts:

- `dogs_allowed` — yes / no / **mixed** / seasonal / unknown
- `dogs_allowed_areas` and `dogs_prohibited_areas` — specific beach names for mixed-policy bodies
- `dogs_off_leash_area`, `dogs_leash_required`, time/season restrictions
- `has_parking`, `parking_type`, `parking_notes`
- `hours_text`, `hours_notes`
- `has_restrooms`, `has_showers`, `has_lifeguards`, `has_picnic_area`, `has_food`, `has_drinking_water`, `has_fire_pits`, `has_disabled_access`
- `enrichment_confidence` — high / low

The `mixed` value turned out to be the most common and most honest answer: most California coastal cities and state parks have a designated dog beach plus prohibition elsewhere. A naive yes/no schema fought the data.

### Final operational coverage

| Field | Known | % of 623 ready |
|---|---|---|
| `dogs_allowed` informative (yes/no/mixed/seasonal) | 621 | **99.7%** |
| `has_parking` | 609 | 98% |
| `has_restrooms` | 421 | 68% |
| `hours_text` | 405 | 65% |
| `has_lifeguards` | 288 | 46% |
| `enrichment_confidence = high` | 490 | 79% |

### Dog-policy distribution

| Value | Beaches |
|---|---|
| mixed | 453 |
| no | 65 |
| yes | 64 |
| seasonal | 39 |
| unknown | 2 |

---

## War stories

- **Corona del Mar State Beach** is state-owned but operated by the City of Newport Beach. The keyword matcher got it wrong; the operator-override table fixed it. The fix generalises: Santa Monica SB → city, Will Rogers / Dockweiler SB → LA County, Leucadia and Moonlight SB → City of Encinitas. Captured as a curated `state_park_operators` table.
- **Del Monte Forest / 17-Mile Drive** has ~10 famous beaches (Fanshell, Point Joe, Bird Rock, Moss Beach, Stillwater Cove). They are private Pebble Beach Company land, not state, not county, not public. CCC names flag some as "17-Mile Drive"; a bounding box (36.555–36.615 N, −121.985 to −121.935 W) catches the rest. All marked `review_status = invalid`.
- **Santa Monica Mountains National Recreation Area** has a huge federal polygon that overlaps beaches operationally run by LA County and the City of Malibu. SAMO NRA is explicitly excluded from the auto-lock rule; the 4 beaches it would have captured are handled by city/county stages instead.
- **BIA tribal lands.** The primary USA Federal Lands dataset doesn't include BIA tribal beaches. Thompson Beach and Smith River Beach both showed up as `governing county` residuals, but BLM CA Land Status has them with SMA_ID = 3 (BIA). Added a gap-fill stage.
- **Lake Tahoe.** Beaches around Tahoe split across CA State Parks (D.L. Bliss SP), USFS (Lake Tahoe Basin Management Unit), and private Tahoe Pines HOA. All three are represented correctly in the data; the HOA one is marked invalid via the non-govt CPAD filter.
- **Sonoma Coast State Park** is a single state park with dog-allowed beaches (Blind Beach, Marshall Gulch, Carmet Beach, Wright's Beach) and dog-prohibited beaches (Bodega Dunes, Campbell Cove) *within the same park*. The `mixed` value plus the `dogs_allowed_areas` / `dogs_prohibited_areas` free-text fields let users see exactly which sections allow dogs.

## What's honest about the limits

- **71 county_default residuals** have no positive polygon evidence. They're mostly real beaches in unincorporated coastal CDPs (Bolinas, Dillon, Davenport, Mussel Shoals, etc.) where "county" really is the operating authority, but we can't prove it. They get the AI-research dog policy and show up in the app with a "generic county" source tag.
- **Mixed is a lossy compression.** For parks like Sonoma Coast, per-beach resolution would require a second research pass per individual beach, not per park. The free-text allowed/prohibited area fields carry that nuance for now.
- **The operational research is LLM-driven.** It's quite good — 79% is high confidence — but it's not ground truth. Monthly refresh is planned to pick up rule changes.
- **Military bases stay unknown.** Camp Pendleton, NBVC Point Mugu, Vandenberg Space Force Base, Pillar Point Space Force Station all have restricted public access and no published beach-level dog policy. `dogs_allowed = unknown` is the right answer.

## Pipeline characteristics

- **Deterministic when it matters.** Jurisdiction classification is polygon-based; the LLM only audits, never decides.
- **Cheap.** Total AI cost for the entire 668-beach pipeline including audit and operational research is ~$3 on Haiku 4.5 pricing.
- **Reproducible.** Every non-polygon decision (state operator overrides, private land bbox, non-beach regex, county-name signals) is captured as code or a checked-in table. No "lost classifications" from informal judgements.
- **Incremental.** Each stage is an independent edge function. New beaches can be processed through any subset. Monthly refresh is a single POST to the orchestrator.

## What's next

- Monthly refresh cron hitting `v2-enrich-operational` for policy drift
- Per-beach (not per-body) dog-policy refinement for the `mixed` bodies — another Tavily/Claude pass that takes the beach's specific name
- Extending beyond California using the same pattern (TIGER is national; most states publish park boundaries as ArcGIS services)
