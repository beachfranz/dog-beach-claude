-- 20260503_harmony_phase6_1_jurisdictions_counties.sql
--
-- Phase 6.1 of the harmony migration (see docs/harmony.md).
--
-- Adds containment populators for two more layers — incorporated cities
-- (TIGER C1) + Census Designated Places (TIGER U1) and counties — and
-- updates the resolver to handle multiple polygon kinds per beach.
--
-- Key design change: _resolve_polygon_containment now partitions by
-- (gold_fid, polygon_kind) instead of (gold_fid). A beach contained in
-- CPAD + C1 + County now has THREE canonical rows (one per kind), each
-- promotable to its own typed FK column on beaches_gold (cpad_unit_id +
-- c1_jurisdiction_id + county_id, the latter two added in a follow-up
-- when promoters are wired).
--
-- Source priority within the same polygon_kind: manual > llm > spatial
-- (cpad / jurisdictions / counties / etc. all share priority 3; ties
-- broken by confidence desc, id asc).

begin;

-- 1. Extend source CHECK to add 'counties' (jurisdictions, military_bases,
--    tribal_lands, nps_places, tiger_places already in enum).
alter table public.beach_enrichment_provenance
  drop constraint if exists beach_enrichment_provenance_source_check;

alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_source_check
  check (source = any (array[
    'manual'::text, 'plz'::text, 'cpad'::text, 'tiger_places'::text,
    'ccc'::text, 'llm'::text, 'web_scrape'::text, 'research'::text,
    'csp_parks'::text, 'park_operators'::text, 'nps_places'::text,
    'tribal_lands'::text, 'military_bases'::text, 'pad_us'::text,
    'sma_code_mappings'::text, 'jurisdictions'::text, 'csp_places'::text,
    'name'::text, 'governing_body'::text, 'park_url'::text,
    'park_url_buffer_attribution'::text, 'old_school_llm'::text,
    'counties'::text
  ]));

-- 2. Resolver upgrade: partition by polygon_kind so a beach can have
--    multiple canonical rows (one per kind).
create or replace function public._resolve_polygon_containment(p_fid bigint default null)
returns int
language plpgsql
as $$
declare touched int := 0;
begin
  update public.beach_enrichment_provenance e
     set is_canonical = false
   where e.field_group = 'polygon_containment'
     and e.gold_fid is not null
     and (p_fid is null or e.gold_fid = p_fid);

  with ranked as (
    select e.id,
      row_number() over (
        partition by e.gold_fid, e.claimed_values->>'polygon_kind'
        order by case e.source
                   when 'manual'         then 1
                   when 'llm'            then 2
                   when 'cpad'           then 3
                   when 'jurisdictions'  then 3
                   when 'counties'       then 3
                   when 'military_bases' then 3
                   when 'tribal_lands'   then 3
                   when 'nps_places'     then 3
                   else                       9
                 end,
                 e.confidence desc nulls last,
                 e.id asc
      ) as rnk
    from public.beach_enrichment_provenance e
    where e.field_group = 'polygon_containment'
      and e.gold_fid is not null
      and (p_fid is null or e.gold_fid = p_fid)
  ),
  upd as (
    update public.beach_enrichment_provenance e
       set is_canonical = true
      from ranked r
     where r.id = e.id and r.rnk = 1
    returning 1
  )
  select count(*) into touched from upd;

  return touched;
end;
$$;

-- 3. Jurisdictions containment: emit one row per beach. Prefer C1
--    (incorporated place) over U1 (CDP); among C1s pick smallest containing.
create or replace function public.populate_jurisdictions_containment_gold(p_fid bigint default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with candidates as (
    select g.fid, j.id as jurisdiction_id, j.name, j.place_type, j.county,
           st_area(j.geom::geography) as area_m2,
           case when j.place_type = 'C1' then 'c1_city'
                when j.place_type = 'U1' then 'cdp'
                else                          'jurisdiction_other'
           end as polygon_kind,
           case when j.place_type = 'C1' then 1
                when j.place_type = 'U1' then 2
                else                          9
           end as kind_priority
    from public.beaches_gold g
    join public.jurisdictions j on st_intersects(j.geom, g.geom::geometry)
    where (p_fid is null or g.fid = p_fid)
      and g.geom is not null
      and g.is_active
  ),
  ranked as (
    select *,
      row_number() over (
        partition by fid
        order by kind_priority asc, area_m2 asc, jurisdiction_id asc
      ) as rnk
    from candidates
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, updated_at)
  select fid, 'polygon_containment', 'jurisdictions', 0.90,
    jsonb_build_object(
      'polygon_kind', polygon_kind,
      'polygon_id',   jurisdiction_id,
      'polygon_name', name,
      'place_type',   place_type,
      'county',       county,
      'area_m2',      area_m2
    ),
    now()
  from best
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update
    set confidence     = excluded.confidence,
        claimed_values = excluded.claimed_values,
        updated_at     = now(),
        is_canonical   = false;

  get diagnostics rows_touched = row_count;
  return rows_touched;
end;
$$;

-- 4. Counties containment: every beach is in exactly one county polygon.
create or replace function public.populate_counties_containment_gold(p_fid bigint default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with candidates as (
    select g.fid, c.geoid as county_geoid, c.name as county_name, c.name_full
    from public.beaches_gold g
    join public.counties c on st_intersects(c.geom, g.geom::geometry)
    where (p_fid is null or g.fid = p_fid)
      and g.geom is not null
      and g.is_active
  ),
  -- One row per beach (counties don't overlap; geoid tiebreak for safety)
  ranked as (
    select *, row_number() over (partition by fid order by county_geoid asc) as rnk
    from candidates
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, updated_at)
  select fid, 'polygon_containment', 'counties', 0.99,
    jsonb_build_object(
      'polygon_kind', 'county',
      'polygon_id',   county_geoid,
      'polygon_name', county_name,
      'name_full',    name_full
    ),
    now()
  from best
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update
    set confidence     = excluded.confidence,
        claimed_values = excluded.claimed_values,
        updated_at     = now(),
        is_canonical   = false;

  get diagnostics rows_touched = row_count;
  return rows_touched;
end;
$$;

-- 5. Update the orchestrator to fan out to all containment populators.
--    Keeps signature returning a single (emitted, resolved, promoted) tuple,
--    where emitted is now the sum across sources.
create or replace function public.populate_polygon_containment_gold(p_fid bigint default null)
returns table (emitted int, resolved int, promoted int)
language plpgsql
as $$
declare e_cpad int; e_juris int; e_county int; r_count int; p_count int;
begin
  e_cpad   := public.populate_cpad_containment_gold(p_fid);
  e_juris  := public.populate_jurisdictions_containment_gold(p_fid);
  e_county := public.populate_counties_containment_gold(p_fid);
  r_count  := public._resolve_polygon_containment(p_fid);
  p_count  := public._promote_polygon_containment_to_gold(p_fid);
  return query select (e_cpad + e_juris + e_county), r_count, p_count;
end;
$$;

comment on function public.populate_jurisdictions_containment_gold(bigint) is
  'Phase 6.1 of harmony migration. Emits one polygon_containment row per beach for the smallest containing TIGER place (C1 incorporated > U1 CDP).';

comment on function public.populate_counties_containment_gold(bigint) is
  'Phase 6.1 of harmony migration. Emits one polygon_containment row per beach for the containing TIGER county.';

commit;
