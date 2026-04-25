-- park_url_scrape_queue — candidate fan-out (2026-04-25)
--
-- Was: one row per beach, picking the smallest-area containing CPAD's
--      park_url via DISTINCT ON area ASC.
-- Now: one row per (beach, distinct park_url) across all CPAD candidates
--      within 300m (sourced from beach_cpad_candidates), so we try every
--      distinct URL nearby instead of just one.
--
-- Empirical lift on the current corpus:
--   • 97 beaches gain ≥2 distinct park_urls (43 of those are non-placeholder)
--   • 156 beaches gain ≥2 distinct useful agncy_web hosts
--   • Total queue grows roughly proportionally to candidate count (~1.5× today)
--
-- discovered_match still gates on "no CPAD url at all for the beach" so
-- we don't over-fan-out into discovered URLs when CPAD already has good
-- candidates.

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
    'cpad'::text     as discovery_source
  from public.locations_stage s
  join public.beach_cpad_candidates c on c.fid = s.fid
  where s.is_active = true
    and c.park_url is not null
    and c.park_url !~* 'encinitasca\.gov'
  -- Tie-break a duplicate URL across multiple candidates by closest distance
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
    'discovered'::text as discovery_source
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
