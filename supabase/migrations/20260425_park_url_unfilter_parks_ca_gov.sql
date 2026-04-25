-- Unfilter parks.ca.gov from park_url_scrape_queue (2026-04-25)
--
-- The original 2026-04-24 filter excluded parks.ca.gov because BS4 yielded
-- ~21 chars per page (JS-rendered SPA). The Tier-2 Playwright fallback in
-- extract_from_park_url.py was added to handle exactly this case but was
-- gated only on HTTP errors (403/429/connect). Pages returning 200-with-
-- empty-DOM slipped through.
--
-- The companion script change (this same date) makes Playwright also
-- trigger when BS4 strips a 200 response below MIN_PAGE_CHARS, which
-- unlocks parks.ca.gov for real extraction.
--
-- Encinitasca.gov stays filtered — its issue is stale 404 URLs in CPAD,
-- not JS-rendering, so Playwright won't help.
--
-- Queue size delta: ~139 → ~338 (+199 parks.ca.gov beaches).
--
-- Two CTEs to update: cpad_match's WHERE filter AND discovered_match's
-- NOT EXISTS check (must mirror the cpad eligibility test).

drop view if exists public.park_url_scrape_queue;

create view public.park_url_scrape_queue as
with cpad_match as (
  select distinct on (s.fid)
    s.fid,
    s.display_name,
    s.state_code,
    c.unit_name as cpad_unit_name,
    c.park_url,
    c.agncy_web,
    'cpad'::text as discovery_source
  from public.locations_stage s
  join public.cpad_units c on st_contains(c.geom, s.geom::geometry)
  where s.is_active = true
    and c.park_url is not null
    and c.park_url !~* 'encinitasca\.gov'
  order by s.fid, st_area(c.geom::geography) asc
),
discovered_match as (
  select distinct on (d.fid)
    s.fid,
    s.display_name,
    s.state_code,
    null::text as cpad_unit_name,
    d.source_url as park_url,
    d.agency_url as agncy_web,
    'discovered'::text as discovery_source
  from public.discovered_park_pages d
  join public.locations_stage s on s.fid = d.fid
  where s.is_active = true
    and not exists (
      select 1
      from public.cpad_units c
      where st_contains(c.geom, s.geom::geometry)
        and c.park_url is not null
        and c.park_url !~* 'encinitasca\.gov'
    )
  order by d.fid, d.match_score desc nulls last
)
select
  q.fid,
  q.display_name,
  q.state_code,
  q.cpad_unit_name,
  q.park_url,
  q.agncy_web,
  coalesce(p.scraped_at, '1970-01-01'::timestamptz) as last_scraped_at,
  p.extraction_status as last_status,
  q.discovery_source
from (
  select * from cpad_match
  union all
  select * from discovered_match
) q
left join public.park_url_extractions p
  on p.fid = q.fid and p.source_url = q.park_url
where p.scraped_at is null
   or p.scraped_at < now() - interval '90 days';
