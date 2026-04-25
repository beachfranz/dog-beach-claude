-- Enrich cpad_source_crawl rows with the CPAD they were crawled from (2026-04-25)
--
-- A 'cpad_source_crawl' row's URL was discovered by crawling a CPAD
-- agency website (agncy_web). We can resolve "which CPAD originally
-- supplied that agency_url" by joining discovered_park_pages.agency_url
-- to beach_cpad_candidates.agncy_web for the same fid. When multiple
-- CPAD candidates for one beach share the same agncy_web, we pick the
-- closest one (smallest distance_m) as the attribution.
--
-- Two changes:
--   1. One-shot UPDATE of existing rows (32 rows in park_url_extractions
--      + matching evidence rows in beach_enrichment_provenance) to set
--      cpad_unit_name on crawl-sourced extractions.
--   2. park_url_scrape_queue v4: queue surfaces the resolved cpad_unit_name
--      for discovered rows so extract_from_park_url.py writes it on insert
--      from now on, not just via backfill.

-- ── 1a. Backfill park_url_extractions ────────────────────────────────────
update public.park_url_extractions p
   set cpad_unit_name = (
     select c.unit_name
       from public.discovered_park_pages d
       join public.beach_cpad_candidates c
         on c.fid = d.fid
        and c.agncy_web = d.agency_url
      where d.fid = p.fid
        and d.source_url = p.source_url
      order by c.distance_m asc
      limit 1
   )
 where p.extraction_type = 'cpad_source_crawl'
   and p.cpad_unit_name is null;

-- ── 1b. Backfill beach_enrichment_provenance ─────────────────────────────
-- Evidence rows emitted from the same crawl extractions need the same fix
-- so the audit trail stays consistent.
update public.beach_enrichment_provenance e
   set cpad_unit_name = pe.cpad_unit_name
  from public.park_url_extractions pe
 where e.source     = 'park_url'
   and e.fid        = pe.fid
   and e.source_url = pe.source_url
   and e.extraction_type = 'cpad_source_crawl'
   and e.cpad_unit_name is null
   and pe.cpad_unit_name is not null;

-- ── 2. Queue view v4 — resolve cpad_unit_name for discovered rows ────────
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
    -- Resolve which CPAD's agncy_web seeded this discovery: closest CPAD
    -- candidate for this fid that shares the agency_url. Null when the
    -- agency wasn't a CPAD-listed one (e.g., manually-seeded URLs).
    (select c.unit_name
       from public.beach_cpad_candidates c
      where c.fid = d.fid
        and c.agncy_web = d.agency_url
      order by c.distance_m asc
      limit 1) as cpad_unit_name,
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
