-- Replaces 20260428_operators_website_backfill.sql which had a JOIN
-- bug: it grouped by mng_agncy (manager) and pulled agncy_web (which
-- is paired with agncy_name = OWNER). This stamped owner URLs onto
-- manager operators (Santa Barbara County → usbr.gov, Ventura County
-- → rsrpd.org, LA Rec & Parks → usace.army.mil, etc.) — wrong on
-- ~255 manager agencies covering ~1,450 of 17,239 CPAD units (8.4%).
--
-- Correct logic: an operator's homepage from CPAD lives only when
-- the operator IS an owner (agncy_name) of some unit. CPAD doesn't
-- expose manager-level websites; operators that are pure managers
-- (never owners) get NULL here and need Tavily/curated fill.
--
-- Procedure:
--   1. Clear current operators.website (nuke buggy AND curated values)
--   2. Re-apply corrected CPAD backfill (group by agncy_name)
--   3. Re-apply curated overrides last so they win precedence
--
-- See memory project_cpad_agncy_vs_mng for why agncy_web pairs with
-- agncy_name and not mng_agncy. That's the load-bearing rule.

begin;

-- 1. Clear all websites. We'll rebuild them in steps 2+3.
update public.operators set website = null;

-- 2. Corrected CPAD backfill — group by OWNER (agncy_name)
update public.operators op
set website = sub.agncy_web
from (
  select cu.agncy_name as owner_name,
         (array_agg(cu.agncy_web order by cu.agncy_web))[1] as agncy_web
    from public.cpad_units cu
   where cu.agncy_web is not null and cu.agncy_web <> ''
   group by cu.agncy_name
) sub
where op.cpad_agncy_name = sub.owner_name
  and (op.website is null or op.website = '');

-- 3. Re-apply curated overrides (mirrors operators_website_curated.sql,
--    inlined here so this migration is self-contained and the
--    re-baseline is atomic).
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
  ('city-of-mendocino',                             null),

  -- Special districts
  ('east-bay-regional-park-district',               'https://www.ebparks.org/'),
  ('mountains-recreation-and-conservation-authority','https://www.mrca.ca.gov/'),

  -- Private conservancies (frequent in 805)
  ('the-nature-conservancy',                        'https://www.nature.org/'),
  ('the-wildlands-conservancy',                     'https://www.wildlandsconservancy.org/'),
  ('santa-catalina-island-conservancy',             'https://www.catalinaconservancy.org/')
) as v(slug, website)
where public.operators.slug = v.slug;

commit;
