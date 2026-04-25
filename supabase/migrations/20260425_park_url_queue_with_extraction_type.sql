-- park_url_scrape_queue v3: surface extraction_type and cpad_unit_name (2026-04-25)
--
-- v2 (20260425_park_url_queue_candidate_fanout) added candidate fan-out.
-- v3 adds extraction_type so consumers can record HOW the URL was sourced.
--
-- Mapping today:
--   discovery_source='cpad'       → extraction_type='cpad_source'
--   discovery_source='discovered' → extraction_type='cpad_source_crawl'
--                                   (discovered_park_pages is sitemap-grep
--                                    against CPAD agncy_web)
-- Future 'derived_url_crawl' will come from non-CPAD discovery (e.g.,
-- place_name → site:search) when we wire it in.

drop view if exists public.park_url_scrape_queue;

create view public.park_url_scrape_queue as
with cpad_match as (
  select distinct on (s.fid, c.park_url)
    s.fid,
    s.display_name,
    s.state_code,
    c.unit_name      as cpad_unit_name,
    c.park_url,
    c.agncy_web,
    c.distance_m::numeric  as cpad_distance_m,
    c.candidate_rank,
    'cpad'::text     as discovery_source,
    'cpad_source'::text as extraction_type
  from public.locations_stage s
  join public.beach_cpad_candidates c on c.fid = s.fid
  where s.is_active = true
    and c.park_url is not null
    and c.park_url !~* 'encinitasca\.gov'
  order by s.fid, c.park_url, c.distance_m, c.candidate_rank
),
discovered_match as (
  select distinct on (d.fid)
    s.fid,
    s.display_name,
    s.state_code,
    null::text       as cpad_unit_name,
    d.source_url     as park_url,
    d.agency_url     as agncy_web,
    null::numeric    as cpad_distance_m,
    null::integer    as candidate_rank,
    'discovered'::text as discovery_source,
    'cpad_source_crawl'::text as extraction_type
  from public.discovered_park_pages d
  join public.locations_stage s on s.fid = d.fid
  where s.is_active = true
    and not exists (
      select 1 from public.beach_cpad_candidates c
      where c.fid = d.fid
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
  q.cpad_distance_m,
  q.candidate_rank,
  q.discovery_source,
  q.extraction_type,
  coalesce(p.scraped_at, '1970-01-01'::timestamptz) as last_scraped_at,
  p.extraction_status as last_status
from (
  select * from cpad_match
  union all
  select * from discovered_match
) q
left join public.park_url_extractions p
  on p.fid = q.fid and p.source_url = q.park_url
where p.scraped_at is null
   or p.scraped_at < now() - interval '90 days';
