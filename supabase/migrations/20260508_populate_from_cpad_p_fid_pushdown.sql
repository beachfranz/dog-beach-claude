-- 20260508_populate_from_cpad_p_fid_pushdown.sql
--
-- Step 5 follow-up: fix the p_fid filter pushdown in populate_from_cpad_gold().
--
-- Original anti-pattern: the function joins beaches_gold to cpad_units
-- (17,239 polygons) via st_dwithin first, then filters by p_fid in the WHERE.
-- When called per-fid (which refire_bep_cascade does in a loop), Postgres
-- couldn't push the filter into the spatial join, so each call scanned the
-- full join space — 700 active gold * 17K cpad polys = 12M candidate rows
-- before the p_fid narrowing.
--
-- Tonight's refire_bep_cascade for 276 fids hit the 8-min Supabase statement
-- timeout because of this. Fix is to pre-narrow beaches_gold via a target
-- CTE so the spatial join only fires for the row(s) being processed.
--
-- Same logic, materially different plan when p_fid is set.

begin;

create or replace function public.populate_from_cpad_gold(p_fid bigint default null)
returns integer
language plpgsql
as $function$
declare rows_touched int;
begin
  with target as (
    -- Narrow to the target row(s) BEFORE joining cpad_units. When p_fid is
    -- set, this is a single-row CTE that the spatial join can hit cheaply.
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
    join public.cpad_units c on st_dwithin(c.geom::geography, t.geom, 500)
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
