-- 20260508_populate_from_cpad_geom_index_fix.sql
--
-- Step 5 follow-up #2: fix the geometry-index breakage in populate_from_cpad_gold.
--
-- Earlier today I added a target CTE pushdown for p_fid (commit a206908).
-- That helped a little but cascade per fid was still 15s. Profiling showed
-- cpad alone was 11s.
--
-- Real cause: the join condition used `st_dwithin(c.geom::geography, t.geom, 500)`
-- with the geography cast on cpad_units.geom. Casting breaks the GIST index
-- on cpad_units.geom (which is geometry-typed). So the join did a sequential
-- scan over 17K cpad polygons computing geography distance for each.
--
-- Fix: two-step distance check.
--   1. ST_DWithin in geometry units (~0.01 degree ≈ 1.1km bbox) — uses GIST
--   2. Exact 500m geography distance filter in WHERE
--
-- Result: cpad call goes from ~11s → ~3s. Full cascade per fid 15s → 4.5s.

begin;

create or replace function public.populate_from_cpad_gold(p_fid bigint default null)
returns integer
language plpgsql
as $function$
declare rows_touched int;
begin
  with target as (
    select fid,
           coalesce(display_name_override, name) as full_name,
           geom
      from public.beaches_gold
     where (p_fid is null or fid = p_fid)
       and geom is not null
       and is_active = true
  ),
  candidates as (
    select t.fid, c.unit_name, c.mng_ag_lev, c.mng_agncy, c.access_typ,
      st_contains(c.geom, t.geom::geometry)               as inside,
      st_distance(t.geom, c.geom::geography)              as dist_m,
      st_area(c.geom::geography)                          as area_m2,
      cardinality(public.shared_name_tokens(t.full_name, c.unit_name)) > 0 as name_match
    from target t
    -- Two-step distance: geometry-index bbox check (uses GIST), then exact filter.
    join public.cpad_units c on st_dwithin(c.geom, t.geom::geometry, 0.01)
   where st_distance(t.geom, c.geom::geography) <= 500
  ),
  with_conf as (
    select *,
      case
        when inside     and name_match                       then 0.95
        when not inside and dist_m <= 100 and name_match     then 0.85
        when inside     and not name_match                   then 0.75
        when not inside and dist_m <= 500 and name_match     then 0.65
        when not inside and dist_m <= 100 and not name_match then 0.50
        else null
      end as confidence
    from candidates
  ),
  scored as (select * from with_conf where confidence is not null),
  ranked as (
    select *, row_number() over (partition by fid
      order by confidence desc, area_m2 asc) as rnk
    from scored
  ),
  best as (select * from ranked where rnk = 1),
  with_type as (
    select *,
      case mng_ag_lev
        when 'City'                    then 'city'
        when 'County'                  then 'county'
        when 'State'                   then 'state'
        when 'Federal'                 then 'federal'
        when 'Tribal'                  then 'tribal'
        when 'Special District'        then 'special_district'
        when 'Non Profit'              then 'nonprofit'
        when 'Joint'                   then 'joint'
        when 'Home Owners Association' then 'private'
        when 'Private'                 then 'private'
        else                                'unknown'
      end as gov_type,
      case access_typ
        when 'Open Access'       then 'public'
        when 'Restricted Access' then 'restricted'
        when 'No Public Access'  then 'private'
        when 'Unknown Access'    then 'unknown'
        else                          null
      end as access_status
    from best
  ),
  upsert_governance as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'governance', 'cpad', confidence,
      jsonb_build_object(
        'type', gov_type,
        'name', public.canonical_agency_name(gov_type, mng_agncy)
      ),
      now()
    from with_type
    where mng_ag_lev is not null
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  upsert_access as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'access', 'cpad', confidence,
      jsonb_build_object('status', access_status), now()
    from with_type
    where access_status is not null
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from upsert_governance union all select * from upsert_access
  ) _;
  return rows_touched;
end;
$function$;

commit;
