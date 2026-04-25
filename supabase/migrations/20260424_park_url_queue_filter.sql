-- park_url_scrape_queue: filter out known-thin / blocked domains
-- (2026-04-24)
--
-- Two domains observed in first-run yield analysis to be unscrapable:
--   parks.ca.gov     — JS-rendered SPA; BS4 yields ~21 chars per page.
--                      Every CA State Park URL returns useless text.
--                      For state parks we already have CDPR's agency-
--                      wide default in governing_body_dog_policies, so
--                      losing per-park scrape costs nothing.
--   encinitasca.gov  — observed 404s (stale URLs in CPAD).
--
-- Add more domains to the regex as we discover them.
-- Queue size: 351 → 139 after filter.

create or replace view public.park_url_scrape_queue as
select distinct on (s.fid)
  s.fid, s.display_name, s.state_code,
  c.unit_name as cpad_unit_name,
  c.park_url, c.agncy_web,
  coalesce(p.scraped_at, '1970-01-01'::timestamptz) as last_scraped_at,
  p.extraction_status as last_status
from public.locations_stage s
join public.cpad_units c on st_contains(c.geom, s.geom::geometry)
left join public.park_url_extractions p
  on p.fid = s.fid and p.source_url = c.park_url
where s.is_active = true
  and c.park_url is not null
  and c.park_url !~* '(parks\.ca\.gov|encinitasca\.gov)'
  and (p.scraped_at is null or p.scraped_at < now() - interval '90 days')
order by s.fid, st_area(c.geom::geography) asc;
