-- Hand-curated authoritative websites for high-leverage CA beach
-- operators. Overrides CPAD's agncy_web where that field points to
-- a parent/sister/random agency (e.g., CDPR → wildlife.ca.gov is
-- wrong; NPS → defense.gov is wrong; City of San Diego → sdsu.edu
-- is wrong).
--
-- These are the operators with the most beaches in 805 + the ones
-- whose dog policy is most-queried. Curated 2026-04-28.

update public.operators set website = v.website
from (values
  -- State agencies
  ('california-department-of-parks-and-recreation', 'https://www.parks.ca.gov/'),
  ('california-department-of-fish-and-wildlife',    'https://wildlife.ca.gov/'),
  ('california-state-lands-commission',             'https://www.slc.ca.gov/'),
  ('california-coastal-commission',                 'https://www.coastal.ca.gov/'),
  ('california-coastal-conservancy',                'https://scc.ca.gov/'),

  -- Federal
  ('united-states-national-park-service',           'https://www.nps.gov/'),
  ('united-states-forest-service',                  'https://www.fs.usda.gov/'),
  ('bureau-of-land-management',                     'https://www.blm.gov/'),
  ('united-states-fish-and-wildlife-service',       'https://www.fws.gov/'),
  ('us-army-corps-of-engineers',                    'https://www.usace.army.mil/'),

  -- Counties
  ('los-angeles-county-department-of-beaches-and-harbors', 'https://beaches.lacounty.gov/'),
  ('orange-county',                                 'https://www.ocparks.com/'),
  ('san-diego-county',                              'https://www.sandiegocounty.gov/parks/'),

  -- Cities (coastal, top-leverage)
  ('city-of-san-diego',                             'https://www.sandiego.gov/'),
  ('city-of-long-beach',                            'https://www.longbeach.gov/'),
  ('city-of-newport-beach',                         'https://www.newportbeachca.gov/'),
  ('city-of-malibu',                                'https://www.malibucity.org/'),
  ('city-of-santa-monica',                          'https://www.santamonica.gov/'),
  ('city-of-coronado',                              'https://www.coronado.ca.us/'),
  ('city-of-huntington-beach',                      'https://www.huntingtonbeachca.gov/'),
  ('city-of-laguna-beach',                          'https://www.lagunabeachcity.net/'),
  ('city-of-imperial-beach',                        'https://www.imperialbeachca.gov/'),
  ('city-of-encinitas',                             'https://www.encinitasca.gov/'),
  ('city-of-carlsbad',                              'https://www.carlsbadca.gov/'),
  ('city-of-half-moon-bay',                         'https://www.hmbcity.com/'),
  ('city-of-pacifica',                              'https://www.cityofpacifica.org/'),
  ('city-of-santa-cruz',                            'https://www.cityofsantacruz.com/'),
  ('city-of-santa-barbara',                         'https://www.santabarbaraca.gov/'),
  ('city-of-monterey',                              'https://www.monterey.org/'),
  ('city-of-ventura',                               'https://www.cityofventura.ca.gov/'),
  ('city-of-oxnard',                                'https://www.oxnard.org/'),
  ('city-of-redondo-beach',                         'https://www.redondo.org/'),
  ('city-of-hermosa-beach',                         'https://www.hermosabeach.gov/'),
  ('city-of-manhattan-beach',                       'https://www.manhattanbeach.gov/'),
  ('city-of-pismo-beach',                           'https://www.pismobeach.org/'),
  ('city-of-morro-bay',                             'https://www.morrobayca.gov/'),
  ('city-of-san-clemente',                          'https://www.san-clemente.org/'),
  ('city-of-dana-point',                            'https://www.danapoint.org/'),
  ('city-of-carmel-by-the-sea',                     'https://ci.carmel.ca.us/'),
  ('city-of-pacific-grove',                         'https://www.cityofpacificgrove.org/'),
  ('city-of-marina',                                'https://www.cityofmarina.org/'),
  ('city-of-fort-bragg',                            'https://city.fortbragg.com/'),
  ('city-of-mendocino',                             null),  -- Mendocino is unincorporated; CDP only

  -- Special districts
  ('east-bay-regional-park-district',               'https://www.ebparks.org/'),
  ('mountains-recreation-and-conservation-authority','https://www.mrca.ca.gov/'),

  -- Private conservancies (frequent in 805)
  ('the-nature-conservancy',                        'https://www.nature.org/'),
  ('the-wildlands-conservancy',                     'https://www.wildlandsconservancy.org/'),
  ('santa-catalina-island-conservancy',             'https://www.catalinaconservancy.org/')
) as v(slug, website)
where public.operators.slug = v.slug;
