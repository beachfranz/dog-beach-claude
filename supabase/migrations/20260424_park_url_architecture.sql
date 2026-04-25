-- park_url extraction architecture (2026-04-24)
--
-- CPAD's park_url is curated by GreenInfo Network and points to the
-- agency's authoritative page for that specific park. When extracted
-- correctly, this is gold-standard metadata — beats LLM-research,
-- agency defaults, and CCC point matches.
--
-- Architecture:
--   1. park_url_extractions table — stores raw scrape + parsed fields
--      per (fid, source_url). Populated by an external Python script.
--   2. populate_from_park_url() — reads parsed fields, emits evidence
--      rows to beach_enrichment_provenance at high confidence.
--   3. source_precedence: 'park_url' = 5 (between manual=0 and cpad=10)
--
-- Coverage: ~290 of CA's 861 beaches have a CPAD park_url available.
-- The other ~570 still need other sources (research, agency defaults).

-- ── 1. Source enum + precedence ─────────────────────────────────────────────
alter table public.beach_enrichment_provenance
  drop constraint if exists beach_enrichment_provenance_source_check;

alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_source_check
  check (source in (
    'manual','plz','cpad','tiger_places','ccc','llm','web_scrape',
    'research','csp_parks','park_operators','nps_places','tribal_lands',
    'military_bases','pad_us','sma_code_mappings','jurisdictions',
    'csp_places','name','governing_body',
    'park_url'              -- NEW: CPAD-curated authoritative park page
  ));

create or replace function public.source_precedence(p_source text)
returns int
language sql immutable
as $$
  select case p_source
    when 'manual'             then 0
    when 'park_url'           then 5    -- gold-standard authoritative scrape
    when 'cpad'               then 10
    when 'pad_us'             then 11
    when 'park_operators'     then 12
    when 'plz'                then 15
    when 'nps_places'         then 20
    when 'tribal_lands'       then 21
    when 'csp_parks'          then 22
    when 'sma_code_mappings'  then 23
    when 'ccc'                then 30
    when 'research'           then 35
    when 'tiger_places'       then 40
    when 'military_bases'     then 50
    when 'state_config'       then 60
    when 'governing_body'     then 65
    when 'web_scrape'         then 70
    when 'name'               then 75
    when 'llm'                then 80
    else                            99
  end;
$$;

-- ── 2. park_url_extractions table ───────────────────────────────────────────
create table if not exists public.park_url_extractions (
  id                       bigserial primary key,
  fid                      int not null references public.locations_stage(fid) on delete cascade,
  source_url               text not null,
  scraped_at               timestamptz not null default now(),
  extraction_status        text not null check (extraction_status in
    ('pending','success','fetch_failed','parse_failed','no_data')),
  http_status              int,
  content_hash             text,                -- sha256 of fetched content
  raw_text                 text,                -- BS4-cleaned page text
  -- Parsed structured fields (populator reads these)
  dogs_allowed             text,
  dogs_leash_required      text,
  dogs_restricted_hours    jsonb,
  dogs_seasonal_rules      jsonb,
  dogs_zone_description    text,
  dogs_policy_notes        text,
  hours_text               text,
  open_time                time,
  close_time               time,
  has_parking              boolean,
  parking_type             text,
  parking_notes            text,
  description              text,
  has_restrooms            boolean,
  has_showers              boolean,
  has_drinking_water       boolean,
  has_lifeguards           boolean,
  has_disabled_access      boolean,
  has_food                 boolean,
  has_fire_pits            boolean,
  has_picnic_area          boolean,
  -- Extraction metadata
  extraction_confidence    numeric(3,2),        -- 0.00–1.00
  extraction_model         text,                -- 'claude-haiku-4-5-20251001'
  extraction_notes         text,
  unique (fid, source_url)
);

create index if not exists pue_fid_idx on public.park_url_extractions(fid);
create index if not exists pue_status_idx on public.park_url_extractions(extraction_status);
create index if not exists pue_scraped_at_idx on public.park_url_extractions(scraped_at desc);

comment on table public.park_url_extractions is
  'Raw + parsed extraction results from CPAD park_url scrapes. Written by the external Python extraction script. Read by populate_from_park_url() to emit evidence to beach_enrichment_provenance. Source URL stored so re-extractions are idempotent (UPSERT on fid+source_url).';

comment on column public.park_url_extractions.extraction_confidence is
  'LLM-derived confidence on the parse. 0.85+ for clear structured pages, 0.65 for ambiguous prose, lower for partial parses. Used directly as confidence in evidence rows emitted by populate_from_park_url.';

-- ── 3. The populator ────────────────────────────────────────────────────────
create or replace function public.populate_from_park_url(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with successful as (
    select * from public.park_url_extractions
    where extraction_status = 'success'
      and (p_fid is null or fid = p_fid)
  ),
  -- Build dogs jsonb (only emit when at least one dog field is set)
  dogs_built as (
    select fid, source_url, scraped_at, extraction_confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',          dogs_allowed,
        'leash_required',   dogs_leash_required,
        'restricted_hours', dogs_restricted_hours,
        'seasonal_rules',   dogs_seasonal_rules,
        'zone_description', dogs_zone_description,
        'notes',            dogs_policy_notes
      )) as v
    from successful
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, source_url, updated_at)
    select fid, 'dogs', 'park_url',
      coalesce(extraction_confidence, 0.85),  -- default 0.85 when LLM didn't set it
      v, source_url, now()
    from dogs_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          source_url     = excluded.source_url,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  -- Practical jsonb (hours + parking + amenities)
  practical_built as (
    select fid, source_url, extraction_confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'hours_text',         hours_text,
        'open_time',          open_time::text,    -- jsonb-friendly time as text
        'close_time',         close_time::text,
        'has_parking',        has_parking,
        'parking_type',       parking_type,
        'parking_notes',      parking_notes,
        'has_restrooms',      has_restrooms,
        'has_showers',        has_showers,
        'has_drinking_water', has_drinking_water,
        'has_lifeguards',     has_lifeguards,
        'has_disabled_access',has_disabled_access,
        'has_food',           has_food,
        'has_fire_pits',      has_fire_pits,
        'has_picnic_area',    has_picnic_area
      )) as v
    from successful
  ),
  ins_practical as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, source_url, updated_at)
    select fid, 'practical', 'park_url',
      coalesce(extraction_confidence, 0.85),
      v, source_url, now()
    from practical_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          source_url     = excluded.source_url,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from ins_dogs union all select * from ins_practical
  ) _;
  return rows_touched;
end;
$$;

comment on function public.populate_from_park_url(int) is
  'Layer 2 populator: emit dogs + practical evidence from park_url_extractions where extraction_status=success. Confidence carried through from extraction_confidence (default 0.85 when null). Reads only — Python script writes to park_url_extractions.';

-- ── 4. Wire into orchestrator ───────────────────────────────────────────────
-- Add park_url between research and the resolver round 2. It runs late so
-- by-then the extraction table is up to date.
create or replace function public.populate_all(p_fid int default null)
returns jsonb
language plpgsql
as $$
declare
  result jsonb := '{}'::jsonb;
  c int;
begin
  c := public.populate_layer1_geographic(p_fid);              result := result || jsonb_build_object('layer1_geographic', c);
  c := public.populate_from_cpad(p_fid);                      result := result || jsonb_build_object('cpad', c);
  c := public.populate_from_ccc(p_fid);                       result := result || jsonb_build_object('ccc', c);
  c := public.populate_from_jurisdictions(p_fid);             result := result || jsonb_build_object('jurisdictions', c);
  c := public.populate_from_csp_parks(p_fid);                 result := result || jsonb_build_object('csp_parks', c);
  c := public.populate_from_park_operators(p_fid);            result := result || jsonb_build_object('park_operators', c);
  c := public.populate_from_nps_places(p_fid);                result := result || jsonb_build_object('nps_places', c);
  c := public.populate_from_tribal_lands(p_fid);              result := result || jsonb_build_object('tribal_lands', c);
  c := public.populate_from_military_bases(p_fid);            result := result || jsonb_build_object('military_bases', c);
  c := public.populate_from_private_land_zones(p_fid);        result := result || jsonb_build_object('private_land_zones', c);
  c := public.populate_governance_from_name(p_fid);           result := result || jsonb_build_object('name', c);
  c := public.populate_from_research(p_fid);                  result := result || jsonb_build_object('research', c);
  c := public.populate_from_park_url(p_fid);                  result := result || jsonb_build_object('park_url', c);

  declare
    gov_count int := 0; access_count int := 0; f int;
  begin
    for f in select fid from public.locations_stage where p_fid is null or fid = p_fid loop
      if public.resolve_governance(f) is not null then gov_count := gov_count + 1; end if;
      if public.resolve_access(f)     is not null then access_count := access_count + 1; end if;
    end loop;
    result := result || jsonb_build_object('resolve_governance', gov_count, 'resolve_access', access_count);
  end;

  c := public.populate_dogs_from_governing_body(p_fid);
  result := result || jsonb_build_object('governing_body', c);

  declare
    dogs_count int := 0; practical_count int := 0; f int;
  begin
    for f in select fid from public.locations_stage where p_fid is null or fid = p_fid loop
      if public.resolve_dogs(f)      is not null then dogs_count := dogs_count + 1; end if;
      if public.resolve_practical(f) is not null then practical_count := practical_count + 1; end if;
    end loop;
    result := result || jsonb_build_object('resolve_dogs', dogs_count, 'resolve_practical', practical_count);
  end;

  c := public.flag_dogs_consistency(p_fid);
  result := result || jsonb_build_object('dogs_consistency_flagged', c);

  return result;
end;
$$;

-- ── 5. Helper view: queue of beaches needing park_url scrape ────────────────
-- Not yet scraped (or stale) AND has a CPAD park_url available.
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
  and (p.scraped_at is null or p.scraped_at < now() - interval '90 days')
order by s.fid, st_area(c.geom::geography) asc;

comment on view public.park_url_scrape_queue is
  'Queue of beaches with a CPAD park_url that have not been scraped (or were scraped >90d ago). Driven by the Python scrape script: SELECT fid, park_url FROM park_url_scrape_queue ORDER BY last_scraped_at LIMIT N.';

-- ── 6. (deferred) Python script integration ────────────────────────────────
-- scripts/extract_from_park_url.py — reads park_url_scrape_queue, fetches
-- pages, runs LLM extraction (Haiku 4.5), upserts park_url_extractions.
-- Mirrors phase2_extract.py architecture: page caching, checkpoint+resume,
-- bounded concurrency, allowed-key filter on writeback.
-- (Built in next session.)
