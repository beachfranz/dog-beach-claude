# Operator dog-policy extraction — v2 evaluation
Run completed 2026-04-28 overnight. Top 50 operators by 805 footprint.
Pipeline: Tavily search → Haiku URL-picker → httpx with Tavily-extract fallback → 3-pass extraction (Haiku Pass A, Sonnet Pass B/C).

## Top-line
| Metric | Count |
|---|---|
| Operators in run | 47 of 50 |
| Evidence rows | 72 |
| Source A (direct_url) rows | 31 |
| Source B (site_search) rows | 41 |
| Fetched ok via httpx | 36 |
| Fetched ok via Tavily fallback | 19 |
| Fetch failed (403/timeout) | 13 |
| Pass A ok | 53 |
| Pass B ok | 55 |
| Pass C ok | 55 |
| Total input tokens | 335,184 |
| Total output tokens | 22,271 |
| Estimated cost | ~$2.00 |

## Source A vs Source B agreement
| Pattern | Count |
|---|---|
| Both agree on default_rule | 5 |
| Both disagree | 4 |
| A only | 12 |
| B only | 6 |
| Neither has rule but at least one has summary | 12 |
| Nothing extracted | 8 |

## Disagreements (Source A vs B)

These are the operators where the two sources extracted different default_rules. Worth manual review.

### City of Coronado
- A: **restricted** — Dogs are welcome in Coronado; Dog Beach allows off-leash on sand, most parks require leashes, and Tidelands Park restricts dogs to pavement only.
- B: **no** — Dogs are banned from all of Coronado Beach except Dog Beach at the north end, where they are always permitted off-leash.

### California Department of Fish and Wildlife
- A: **no** — Dogs are prohibited at Moss Landing Wildlife Area.
- B: **restricted** — Dogs allowed on leash at Pismo State Beach but prohibited entirely at Oso Flaco Lake and Pismo Dunes Natural Preserve.

### Orange County
- A: **restricted** — Dogs must be on leash at all times; banned from beach June 15â€“Sept. 10 between 9amâ€“6pm.
- B: **no** — Dogs are prohibited from all public beaches in Orange County, except for guide dogs or service dogs.

### City of Fort Bragg
- A: **yes** — Dogs are allowed at Glass Beach in Fort Bragg; dog parks are also available at CV Starr and Noyo Headlands Park.
- B: **restricted** — Dogs must be on leash at all times in City parks; City Council may designate off-leash areas by resolution.

## Per-operator detail (47 ops)

### California Department of Parks and Recreation (265 beaches)
- **A**: rule=`restricted` · fetch=`ok`
  - url: https://www.parks.ca.gov/?page_id=30471
  - summary: Dogs are allowed on leash at Pismo State Beach but prohibited at Oso Flaco Lake and Pismo Dunes Natural Preserve.
- **B**: rule=`—` · fetch=`—`

### United States National Park Service (104 beaches)
- **A**: rule=`restricted` · fetch=`ok`
  - url: https://www.nps.gov/goga/stbe.htm
  - summary: Dogs on leash are allowed in the parking lot, picnic areas, and adjacent county beach, but prohibited on the NPS beach section.
- **B**: rule=`restricted` · fetch=`ok`
  - url: https://www.nps.gov/pore/planyourvisit/pets.htm
  - summary: Dogs on leash (â‰¤6 ft) allowed on select Point Reyes beaches year-round; Tomales Bay west-side beaches prohibited; seasonal closures apply.

### City of San Diego (79 beaches)
- **A**: rule=`—` · fetch=`ok`
  - url: https://webmaps.sandiego.gov/portal/apps/storymaps/stories/9bda81a5f7c6489fb87c218a9c7ce605
  - summary: No dog policy information could be extracted from this page.
- **B**: rule=`restricted` · fetch=`ok_tavily`
  - url: https://www.sandiego.gov/park-and-recreation/parks/dogs/bchdog
  - summary: Dogs are welcome off-leash at Dog Beach and Fiesta Island (4amâ€“10pm); all other San Diego beach/bay areas restrict dogs.

### City of Malibu (45 beaches)
- **A**: rule=`no` · fetch=`ok`
  - url: https://www.malibucity.org/FAQ.aspx?QID=64
  - summary: Dogs are prohibited on all Malibu beaches; Leo Carrillo State Beach allows dogs on leash in some areas.
- **B**: rule=`—` · fetch=`ok_tavily`
  - url: https://ecode360.com/44333476
  - summary: Malibu adopts LA County's animal control ordinance; no beach-specific dog rules are stated on this page.

### United States Forest Service (42 beaches)
- **A**: rule=`restricted` · fetch=`ok`
  - url: https://www.fs.usda.gov/r05/inyo/recreation/june-lake-beach
  - summary: Dogs must be leashed at all times and owners must pick up waste at June Lake Beach.
- **B**: rule=`restricted` · fetch=`ok`
  - url: https://www.fs.usda.gov/r05/laketahoebasin/recreation/where-can-i-take-my-dog-lake-tahoe
  - summary: Dogs on leash are welcome almost everywhere in the Lake Tahoe Basin except 6 designated swimming beaches where dogs are prohibited.

### Monterey County (41 beaches)
- **A**: rule=`—` · fetch=`—`
- **B**: rule=`restricted` · fetch=`ok_tavily`
  - url: https://www.countyofmonterey.gov/government/departments-i-z/public-works-facilities-parks/county-parks/policies
  - summary: Dogs welcome at all Monterey County Parks on leash â‰¤7ft; proof of rabies vaccination required and extra fees apply for camping.

### Santa Barbara County (38 beaches)
- **A**: rule=`—` · fetch=`—`
- **B**: rule=`—` · fetch=`ok`
  - url: https://www.countyofsb.org/887/Off-Leash-Dog-Areas
  - summary: Page content is unavailable; no beach dog policy details could be extracted.

### Santa Cruz County (37 beaches)
- **A**: rule=`no` · fetch=`ok`
  - url: https://wildlife.ca.gov/Lands/Places-to-Visit/Moss-Landing-WA
  - summary: Dogs are prohibited at Moss Landing Wildlife Area.
- **B**: rule=`—` · fetch=`http_403+tavily_failed: Failed to fetch url`
  - url: https://www.santacruzca.gov/Government/City-Departments/Parks-Recreation/Parks-Beaches-Open-Spaces/Dog-Off-Leash-Areas

### Humboldt County (36 beaches)
- **A**: rule=`—` · fetch=`—`
- **B**: rule=`restricted` · fetch=`ok`
  - url: https://humboldt.county.codes/Code/271-8
  - summary: Dogs must be on leashes â‰¤10 ft in all parks; banned at Swimmers Delight beach and Tooby Park, allowed at Freshwater Park only if confined to vehicle.

### Los Angeles County (36 beaches)
- **A**: rule=`—` · fetch=`—`
- **B**: rule=`no` · fetch=`ok`
  - url: https://beaches.lacounty.gov/la-county-beach-rules/
  - summary: Dogs and all animals are prohibited on LA County beaches with no exceptions listed.

### Santa Catalina Island Conservancy (31 beaches)
- **A**: rule=`restricted` · fetch=`ok`
  - url: https://catalinaconservancy.org/resources/policies-and-information/pet-policies/
  - summary: Dogs on Conservancy property must be leashed, vaccinated, and licensed; they may only be loose inside homes or private fenced yards.
- **B**: rule=`—` · fetch=`—`

### Mendocino County (28 beaches)
- **A**: rule=`—` · fetch=`—`
- **B**: rule=`—` · fetch=`ok_tavily`
  - url: https://www.mendocinocounty.gov/departments/animal-care-services/animal-protection/faq
  - summary: This FAQ page covers general animal care topics and does not provide any beach dog policy information.

### City of Laguna Beach (23 beaches)
- **A**: rule=`restricted` · fetch=`ok_tavily`
  - url: https://www.lagunabeachcity.net/our-beaches/dogs-on-the-beach
  - summary: Dogs allowed on Laguna Beach year-round on leash, but only before 9am/after 6pm June 15â€“Sept 10; banned always at Thousand Steps Beach.
- **B**: rule=`—` · fetch=`http_400`
  - url: https://www.facebook.com/lagunabeachgov/posts/dogs-are-part-of-the-crew-but-there-are-rules-dogs-are-not-allowed-on-the-beach-/1382460620581535/

### City of Half Moon Bay (23 beaches)
- **A**: rule=`—` · fetch=`—`
- **B**: rule=`restricted` · fetch=`ok`
  - url: https://www.visithalfmoonbay.org/places/poplar-beach/
  - summary: Dogs are allowed on-leash at Poplar Beach, unlike most nearby state-run beaches where dogs are prohibited.

### Marin County (23 beaches)
- **A**: rule=`—` · fetch=`ok_tavily`
  - url: https://www.parks.marincounty.org/parkspreserves/parks/corte-madera-pathway
  - summary: No dog policy information is available on this page.
- **B**: rule=`—` · fetch=`http_403+tavily_failed: Failed to fetch url`
  - url: https://parks.marincounty.gov/parkspreserves/rules-and-regulations

### Los Angeles County Department of Beaches and Harbors, County of (23 beaches)
- **A**: rule=`no` · fetch=`ok`
  - url: https://beaches.lacounty.gov/la-county-beach-rules/
  - summary: Dogs (and all animals) are prohibited on LA County beaches with no named exceptions listed.
- **B**: rule=`—` · fetch=`ok`
  - url: https://beaches.lacounty.gov/la-county-beach-rules-faq/
  - summary: This FAQ page contains no information about dog policies at LA County beaches.

### Placer County (19 beaches)
- **A**: rule=`no` · fetch=`ok`
  - url: https://www.placer.ca.gov/10337/PCSO-reminder-to-follow-beach-rules
  - summary: Dogs are prohibited on all Placer County Tahoe beaches and in the water; rules may vary at Skylandia Park and Beach.
- **B**: rule=`—` · fetch=`pdf_route_to_tavily+tavily_failed: Failed to fetch url`
  - url: https://www.placer.ca.gov/DocumentCenter/View/76317/2023-121223-Signed-Ordinance-6245-B

### City of Newport Beach (19 beaches)
- **A**: rule=`restricted` · fetch=`ok`
  - url: https://www.newportbeachca.gov/government/departments/recreation-senior-services/marine-protection-tidepools
  - summary: Dogs are banned from all city beaches 10amâ€“4:30pm; must be leashed at all times.
- **B**: rule=`—` · fetch=`ok`
  - url: https://ecms.newportbeachca.gov/WEB/DocView.aspx?id=730699&dbid=0&repo=CNB
  - summary: No dog policy information could be extracted; the page failed to load any content.

### Del Norte County (18 beaches)
- **A**: rule=`—` · fetch=`—`
- **B**: rule=`—` · fetch=`pdf_route_to_tavily+tavily_failed: Failed to fetch url`
  - url: https://agendas.dnco.org/450966/450979/450980/452243/RevisedAnimalControlOrdinanceCountyCounselAnimalControl452753.pdf

### City of Santa Cruz (17 beaches)
- **A**: rule=`—` · fetch=`http_403+tavily_failed: Failed to fetch url`
  - url: https://www.cityofsantacruz.com/government/city-departments/parks-recreation/parks-beaches-open-spaces/parks/delaveaga-park
- **B**: rule=`—` · fetch=`http_403+tavily_failed: Failed to fetch url`
  - url: https://www.santacruzca.gov/files/assets/city/v/1/pr/documents/where-can-i-take-my-dog.pdf

### East Bay Regional Park District (16 beaches)
- **A**: rule=`no` · fetch=`ok`
  - url: https://www.ebparks.org/safety/dogs
  - summary: Dogs are prohibited at all beaches; elsewhere leash required in developed areas, off-leash allowed in undeveloped open space under voice control.
- **B**: rule=`—` · fetch=`ok_tavily`
  - url: https://www.ebparks.org/sites/default/files/Attachment_C_Ordinance38_Dogs_On_Leash.pdf
  - summary: Dogs must be on leash in all developed areas, paved trails, picnic areas, campgrounds, and other listed zones within East Bay Regional Parks.

### San Mateo County (16 beaches)
- **A**: rule=`—` · fetch=`—`
- **B**: rule=`—` · fetch=`ok_tavily`
  - url: https://www.smcgov.org/parks/tunitas-creek-beach-regulations
  - summary: Tunitas Creek Beach is governed by San Mateo County Ordinance No. 4778; specific dog rules are not detailed on this page.

### United States Bureau of Land Management (16 beaches)
- **A**: rule=`no` · fetch=`ok`
  - url: https://wildlife.ca.gov/Lands/Places-to-Visit/Moss-Landing-WA
  - summary: Dogs are prohibited at Moss Landing Wildlife Area.
- **B**: rule=`—` · fetch=`—`

### San Luis Obispo County (16 beaches)
- **A**: rule=`—` · fetch=`ok`
  - url: https://slocountyparks.com/dog-parks/
  - summary: SLO County has designated dog parks (not beaches) where dogs must be licensed, vaccinated, and neutered (males 6+ months).
- **B**: rule=`—` · fetch=`—`

### City of San Clemente (14 beaches)
- **A**: rule=`—` · fetch=`ok`
  - url: https://www.san-clemente.org/194/Beaches
  - summary: This page contains no information about dog policies at San Clemente beaches.
- **B**: rule=`—` · fetch=`—`

### San Bernardino County (14 beaches)
- **A**: rule=`—` · fetch=`—`
- **B**: rule=`—` · fetch=`ok`
  - url: https://animalcare.sbcounty.gov/animallaws/
  - summary: Dogs must be leashed or enclosed when off their property; no beach-specific dog policies are mentioned on this page.

### City of San Buenaventura (Ventura) (13 beaches)
- **A**: rule=`—` · fetch=`—`
- **B**: rule=`restricted` · fetch=`ok_tavily`
  - url: https://www.cityofventura.ca.gov/DocumentCenter/View/32168/9B
  - summary: Dogs must be leashed on public beaches at all times; off-leash areas exist only at Arroyo Verde and Camino Real parks.

### City of Eureka (12 beaches)
- **A**: rule=`—` · fetch=`—`
- **B**: rule=`—` · fetch=`http_403+tavily_failed: Failed to fetch url`
  - url: https://codelibrary.amlegal.com/codes/eureka/latest/eureka_ca/0-0-0-34715

### City of Long Beach (11 beaches)
- **A**: rule=`restricted` · fetch=`ok`
  - url: https://www.longbeach.gov/park/park-and-facilities/directory/rosies-dog-beach
  - summary: Dogs are off-leash allowed at Rosie's Dog Beach (6amâ€“8pm daily) but must be leashed outside the designated Dog Zone area.
- **B**: rule=`restricted` · fetch=`ok`
  - url: https://www.longbeach.gov/park/park-and-facilities/directory/dog-parks/
  - summary: Long Beach has multiple off-leash dog parks and a dog beach (Rosie's Dog Beach); dogs must be vaccinated, licensed, and over 4 months old.

### Stanislaus County (10 beaches)
- **A**: rule=`—` · fetch=`ok`
  - url: https://www.stancounty.com/parks/facilities.shtm
  - summary: Pets are prohibited at Modesto Reservoir; no general dog policy stated for other Stanislaus County parks on this page.
- **B**: rule=`—` · fetch=`ok_tavily`
  - url: https://www.stancounty.com/bos/agenda/2020/20200714/PH04.pdf
  - summary: This page covers Stanislaus Animal Services Agency fee changes for adoptions and vaccinations; no beach dog policies are mentioned.

### City of Santa Barbara (10 beaches)
- **A**: rule=`—` · fetch=`http_403+tavily_failed: Failed to fetch url`
  - url: https://sbparksandrec.santabarbaraca.gov/sites/default/files/2024-08/Where%20Can%20I%20Take%20My%20Dog%20Full%20Page.pdf
- **B**: rule=`—` · fetch=`http_403+tavily_failed: Failed to fetch url`
  - url: https://santabarbaraca.com/plan-your-trip/know-before-you-go/pet-friendly-santa-barbara/

### California Department of Fish and Wildlife (9 beaches)
- **A**: rule=`no` · fetch=`ok`
  - url: https://wildlife.ca.gov/Lands/Places-to-Visit/Moss-Landing-WA
  - summary: Dogs are prohibited at Moss Landing Wildlife Area.
- **B**: rule=`restricted` · fetch=`ok`
  - url: https://www.parks.ca.gov/?page_id=30471
  - summary: Dogs allowed on leash at Pismo State Beach but prohibited entirely at Oso Flaco Lake and Pismo Dunes Natural Preserve.

### City of Santa Monica (9 beaches)
- **A**: rule=`restricted` · fetch=`ok`
  - url: https://www.santamonica.gov/places/parks/beach-park-1
  - summary: Dogs allowed in Beach Park 1 on leash with clean-up materials, but dogs are not allowed on the beach.
- **B**: rule=`—` · fetch=`ok_tavily`
  - url: https://ecode360.com/42726883
  - summary: This page lists Santa Monica's animal ordinance chapter sections but does not display beach-specific dog rules in the visible content.

### Orange County (9 beaches)
- **A**: rule=`restricted` · fetch=`ok`
  - url: https://www.ocparks.com/beaches/capistrano-beach
  - summary: Dogs must be on leash at all times; banned from beach June 15â€“Sept. 10 between 9amâ€“6pm.
- **B**: rule=`no` · fetch=`ok`
  - url: https://www.ocpetinfo.com/field-operations/oc-pet-laws
  - summary: Dogs are prohibited from all public beaches in Orange County, except for guide dogs or service dogs.

### City of Pacific Grove (9 beaches)
- **A**: rule=`no` · fetch=`ok_tavily`
  - url: https://www.cityofpacificgrove.org/Document_Center/Resolutions%20&%20Ordinances/Ordinances/2022/22-007%20Caledonia%20Park%20Dogs%20Exception%20Ord.pdf
  - summary: Dogs are prohibited in Pacific Grove public parks; on-leash exceptions exist for certain areas, but Lovers Point Park beach and grass areas ban dogs.
- **B**: rule=`no` · fetch=`ok_tavily`
  - url: https://ecode360.com/48186279
  - summary: Dogs are generally prohibited in all Pacific Grove parks; on-leash exceptions exist for coastal trails and some parks, but Lovers Point Park beach is excluded.

### City of Oxnard (8 beaches)
- **A**: rule=`—` · fetch=`http_403+tavily_failed: Failed to fetch url`
  - url: https://www.oxnard.org/city-department/public-works/parks/parks-faq/
- **B**: rule=`—` · fetch=`http_403+tavily_failed: Failed to fetch url`
  - url: https://www.oxnard.gov/public-works/parks-division

### City of Oceanside (7 beaches)
- **A**: rule=`no` · fetch=`ok_tavily`
  - url: https://www.ci.oceanside.ca.us/government/public-works/beaches-pier/beaches/beach-pier-rules-regulations
  - summary: Dogs are prohibited on all Oceanside city beaches year-round; leashed dogs are allowed on The Strand walkway only.
- **B**: rule=`—` · fetch=`—`

### City of Carlsbad (7 beaches)
- **A**: rule=`no` · fetch=`ok_tavily`
  - url: https://www.carlsbadca.gov/residents/about-carlsbad/beaches
  - summary: Dogs are not allowed on any Carlsbad beach, leashed or unleashed; violators face up to $300 citation, or $600 for off-leash.
- **B**: rule=`no` · fetch=`ok_tavily`
  - url: https://www.carlsbadca.gov/Home/Components/News/News/2939/5
  - summary: Dogs (leashed and unleashed) are currently not allowed on Carlsbad city beaches or parks; rules may change after future City Council review.

### City of Fort Bragg (7 beaches)
- **A**: rule=`yes` · fetch=`ok`
  - url: https://www.city.fortbragg.com/services/local_parks_aquatic_center.php
  - summary: Dogs are allowed at Glass Beach in Fort Bragg; dog parks are also available at CV Starr and Noyo Headlands Park.
- **B**: rule=`restricted` · fetch=`ok`
  - url: https://www.codepublishing.com/CA/FortBragg/html/FortBragg09/FortBragg0968.html
  - summary: Dogs must be on leash at all times in City parks; City Council may designate off-leash areas by resolution.

### City of Oakland (7 beaches)
- **A**: rule=`—` · fetch=`—`
- **B**: rule=`—` · fetch=`http_403+tavily_failed: Failed to fetch url`
  - url: https://www.oaklandca.gov/Community/Parks-Facilities/Park-Use-Guidelines

### City of Coronado (7 beaches)
- **A**: rule=`restricted` · fetch=`ok`
  - url: https://www.coronado.ca.us/757/Dogs
  - summary: Dogs are welcome in Coronado; Dog Beach allows off-leash on sand, most parks require leashes, and Tidelands Park restricts dogs to pavement only.
- **B**: rule=`no` · fetch=`ok`
  - url: https://www.coronado.ca.us/244/Coronado-Beach
  - summary: Dogs are banned from all of Coronado Beach except Dog Beach at the north end, where they are always permitted off-leash.

### City of Crescent City (7 beaches)
- **A**: rule=`—` · fetch=`pdf_route_to_tavily+tavily_failed: Failed to fetch url`
  - url: https://www.crescentcity.org/media/Agendas/2019/April%201st%202019.pdf
- **B**: rule=`—` · fetch=`ok`
  - url: https://www.crescentcity.org/BeachfrontPark-1
  - summary: Crescent City's Beachfront Park has a dedicated off-leash dog park (Dog Town) with separate areas for large and small dogs.

### City of South Lake Tahoe (7 beaches)
- **A**: rule=`—` · fetch=`—`
- **B**: rule=`—` · fetch=`ok_tavily`
  - url: https://www.tahoepublicbeaches.org/features/pet-friendly/
  - summary: This page lists City of South Lake Tahoe beaches under a 'Dogs Allowed with Restrictions' category, but does not detail the specific restrictions.

### City of Monterey (7 beaches)
- **A**: rule=`—` · fetch=`—`
- **B**: rule=`—` · fetch=`http_403+tavily_failed: Failed to fetch url`
  - url: https://monterey.municipal.codes/Code/23-8

### City of Dana Point (6 beaches)
- **A**: rule=`—` · fetch=`http_403+tavily_failed: Failed to fetch url`
  - url: https://www.danapoint.org/Community/Parks-Trails/Parks-FAQs
- **B**: rule=`—` · fetch=`ok_tavily`
  - url: https://ecode360.com/42958775
  - summary: This page covers park enforcement and violation procedures but does not specify any dog or leash policies for Dana Point beaches.

### City of Richmond (6 beaches)
- **A**: rule=`—` · fetch=`—`
- **B**: rule=`—` · fetch=`ok_tavily`
  - url: https://www.richmond.ca/__shared/assets/dog_owners_info11507.pdf
  - summary: Dogs must be on-leash in all public places except designated off-leash areas; no beach-specific rules are mentioned.

### El Dorado County (6 beaches)
- **A**: rule=`—` · fetch=`—`
- **B**: rule=`—` · fetch=`http_403+tavily_failed: Failed to fetch url`
  - url: https://www.eldoradocounty.ca.gov/files/assets/county/v/1/documents/land-use/parks-amp-trails/dog-park-rules-and-regulations.pdf

