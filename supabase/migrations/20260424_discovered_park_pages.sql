-- discovered_park_pages — discover beach-specific pages on agency sites
-- when CPAD doesn't have a curated park_url for that beach. (2026-04-24)
--
-- Coverage: CPAD park_url is filled for ~290 of CA's 861 beaches. The
-- other ~570 still need discovery. This table holds candidate URLs
-- found via sitemap-grep / depth-1 crawl on the agency's website
-- (CPAD agncy_web). The scrape queue UNIONs these with CPAD park_urls
-- so the existing extract_from_park_url.py works on both seamlessly.

create table if not exists public.discovered_park_pages (
  id              bigserial primary key,
  fid             int  not null references public.locations_stage(fid) on delete cascade,
  source_url      text not null,
  source_method   text not null check (source_method in
    ('sitemap','site_search','homepage_crawl','manual')),
  agency_url      text,            -- the CPAD agncy_web we crawled from
  match_score     numeric(3,2),    -- 0.00-1.00 confidence in the match
  discovered_at   timestamptz not null default now(),
  notes           text,
  unique (fid, source_url)
);

create index if not exists dpp_fid_idx     on public.discovered_park_pages(fid);
create index if not exists dpp_method_idx  on public.discovered_park_pages(source_method);

comment on table public.discovered_park_pages is
  'Candidate beach-specific URLs discovered by crawling agency websites for beaches that lack a CPAD-curated park_url. Read by park_url_scrape_queue (UNIONed with CPAD URLs) so extract_from_park_url.py processes them uniformly.';

-- ── Queue view extension: UNION CPAD park_urls + discovered URLs ────────────
-- Per-fid: prefer CPAD park_url when available (higher quality), else
-- pick the highest-scored discovered URL.
create or replace view public.park_url_scrape_queue as
with cpad_match as (
  select distinct on (s.fid)
    s.fid, s.display_name, s.state_code,
    c.unit_name as cpad_unit_name,
    c.park_url, c.agncy_web,
    'cpad'::text as discovery_source
  from public.locations_stage s
  join public.cpad_units c on st_contains(c.geom, s.geom::geometry)
  where s.is_active = true
    and c.park_url is not null
    and c.park_url !~* '(parks\.ca\.gov|encinitasca\.gov)'
  order by s.fid, st_area(c.geom::geography) asc
),
discovered_match as (
  select distinct on (d.fid)
    s.fid, s.display_name, s.state_code,
    null::text as cpad_unit_name,
    d.source_url as park_url,
    d.agency_url as agncy_web,
    'discovered'::text as discovery_source
  from public.discovered_park_pages d
  join public.locations_stage s on s.fid = d.fid
  where s.is_active = true
    -- Only emit when CPAD doesn't already cover this fid
    and not exists (
      select 1 from public.cpad_units c
      where st_contains(c.geom, s.geom::geometry)
        and c.park_url is not null
        and c.park_url !~* '(parks\.ca\.gov|encinitasca\.gov)'
    )
  order by d.fid, d.match_score desc nulls last
)
select q.fid, q.display_name, q.state_code, q.cpad_unit_name,
       q.park_url, q.agncy_web,
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
where (p.scraped_at is null or p.scraped_at < now() - interval '90 days');

comment on view public.park_url_scrape_queue is
  'Beaches that have a candidate URL to scrape (CPAD park_url OR discovered via discover_park_pages.py). discovery_source column tells you which.';
