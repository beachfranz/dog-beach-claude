-- Canonicalize CA DPR policy_source URLs to per-park detail pages.
--
-- Before: 36 of 44 CA DPR policy_source rows had source_url =
-- 'https://www.parks.ca.gov/Dogs' (the generic catalog page) instead
-- of the per-park detail page (parks.ca.gov/?page_id=N).
--
-- The generic /Dogs catalog is a fallback page; the per-park detail
-- pages are the authoritative source for each park's specific rule.
-- Per Franz directive 2026-05-17, prefer detail pages everywhere.
--
-- Mapping was resolved by fetching parks.ca.gov/?page_id=21805
-- (Complete State Parks Listing).

UPDATE public.policy_source ps
   SET source_url = m.url
  FROM (VALUES
    ('Año Nuevo State Park', 'https://www.parks.ca.gov/?page_id=523'),
    ('Asilomar State Beach', 'https://www.parks.ca.gov/?page_id=566'),
    ('Bean Hollow State Beach', 'https://www.parks.ca.gov/?page_id=527'),
    ('Candlestick Point State Recreation Area', 'https://www.parks.ca.gov/?page_id=519'),
    ('Carmel River State Beach', 'https://www.parks.ca.gov/?page_id=567'),
    ('Caspar Headlands State Beach', 'https://www.parks.ca.gov/?page_id=445'),
    ('Cayucos State Beach', 'https://www.parks.ca.gov/?page_id=596'),
    ('Del Norte Coast Redwoods State Park', 'https://www.parks.ca.gov/?page_id=414'),
    ('Doheny State Beach', 'https://www.parks.ca.gov/?page_id=645'),
    ('El Capitán State Beach', 'https://www.parks.ca.gov/?page_id=601'),
    ('Fort Ord Dunes State Park', 'https://www.parks.ca.gov/?page_id=580'),
    ('Fort Ross State Historic Park', 'https://www.parks.ca.gov/?page_id=449'),
    ('Greenwood State Beach', 'https://www.parks.ca.gov/?page_id=447'),
    ('Half Moon Bay State Beach', 'https://www.parks.ca.gov/?page_id=531'),
    ('Humboldt Redwoods State Park', 'https://www.parks.ca.gov/?page_id=425'),
    ('Lake Del Valle State Recreation Area', 'https://www.parks.ca.gov/?page_id=537'),
    ('Little River State Beach', 'https://www.parks.ca.gov/?page_id=419'),
    ('McLaughlin Eastshore State Park State Seashore', 'https://www.parks.ca.gov/?page_id=520'),
    ('Mendocino Headlands State Park (incl. Ford House)', 'https://www.parks.ca.gov/?page_id=442'),
    ('Montaña de Oro State Park', 'https://www.parks.ca.gov/?page_id=592'),
    ('Montara State Beach', 'https://www.parks.ca.gov/?page_id=532'),
    ('Morro Strand State Beach', 'https://www.parks.ca.gov/?page_id=593'),
    ('Pacifica State Beach', 'https://www.parks.ca.gov/?page_id=524'),
    ('Pescadero State Beach', 'https://www.parks.ca.gov/?page_id=522'),
    ('Point Mugu State Park', 'https://www.parks.ca.gov/?page_id=630'),
    ('Point Sal State Beach', 'https://www.parks.ca.gov/?page_id=605'),
    ('San Elijo State Beach', 'https://www.parks.ca.gov/?page_id=662'),
    ('Schooner Gulch State Beach', 'https://www.parks.ca.gov/?page_id=446'),
    ('Sinkyone Wilderness State Park', 'https://www.parks.ca.gov/?page_id=429'),
    ('Sonoma Coast State Park', 'https://www.parks.ca.gov/?page_id=451'),
    ('Tolowa Dunes State Park', 'https://www.parks.ca.gov/?page_id=430'),
    ('Trinidad State Beach', 'https://www.parks.ca.gov/?page_id=418'),
    ('Westport-Union Landing State Beach', 'https://www.parks.ca.gov/?page_id=440'),
    ('Wilder Ranch State Park', 'https://www.parks.ca.gov/?page_id=549')
  ) AS m(park_suffix, url)
 WHERE ps.issuing_agency_id = 234
   AND ps.citation = 'California State Parks dog policy — ' || m.park_suffix;

-- Dockweiler is a special case — citation phrased differently
UPDATE public.policy_source
   SET source_url = 'https://www.parks.ca.gov/?page_id=617'
 WHERE id = 17
   AND citation LIKE 'Dockweiler State Beach dog policy%';

-- Row 14 ("California State Parks dog policy (parks.ca.gov/Dogs)")
-- is the LEGITIMATE catchall baseline — leave its source_url pointing
-- at the generic /Dogs catalog. Row 13 (Crown Memorial lease) has
-- source_url=NULL by design (lease document not online).
