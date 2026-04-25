-- Re-weight CPAD match confidence (2026-04-24, Franz)
--
-- Updated matrix — name match matters more than pure spatial containment:
--
--   Inside polygon + name match       0.95
--   Within 100m buffer + name match   0.85
--   Inside polygon (no name match)    0.75
--   Within 500m buffer + name match   0.65   ← NEW tier
--   Within 100m buffer (no name match) 0.50
--   Beyond 500m / no name beyond 100m  skip
--
-- Key change vs old: a name-matched buffer hit (0.85) now beats an
-- inside-polygon-no-name hit (0.75). And we extend the name-matched
-- search out to 500m to catch cases where the polygon boundary is
-- conservatively drawn relative to the actual beach point.

create or replace function public.populate_from_cpad(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with candidates as (
    select
      s.fid,
      c.unit_name,
      c.mng_ag_lev,
      c.mng_agncy,
      c.access_typ,
      st_contains(c.geom, s.geom::geometry)               as inside,
      st_distance(s.geom, c.geom::geography)              as dist_m,
      st_area(c.geom::geography)                          as area_m2,
      cardinality(public.shared_name_tokens(s.display_name, c.unit_name)) > 0
                                                          as name_match
    from public.locations_stage s
    join public.cpad_units c
      on st_dwithin(c.geom::geography, s.geom, 500)        -- widened to 500m
    where (p_fid is null or s.fid = p_fid)
      and s.geom is not null
  ),
  with_conf as (
    select *,
      case
        when inside     and name_match                 then 0.95
        when not inside and dist_m <= 100 and name_match then 0.85
        when inside     and not name_match             then 0.75
        when not inside and dist_m <= 500 and name_match then 0.65
        when not inside and dist_m <= 100 and not name_match then 0.50
        else null  -- 100-500m without name match → drop
      end as confidence
    from candidates
  ),
  -- Drop rows below threshold
  scored as (
    select * from with_conf where confidence is not null
  ),
  -- Within a beach, rank by confidence DESC, then most-specific (smallest area)
  ranked as (
    select *,
      row_number() over (
        partition by fid
        order by confidence desc, area_m2 asc
      ) as rnk
    from scored
  ),
  best as (
    select * from ranked where rnk = 1
  ),
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
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'governance', 'cpad', confidence,
      jsonb_build_object('type', gov_type, 'name', mng_agncy),
      now()
    from with_type
    where mng_ag_lev is not null
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  upsert_access as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'access', 'cpad', confidence,
      jsonb_build_object('status', access_status),
      now()
    from with_type
    where access_status is not null
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from upsert_governance
    union all
    select * from upsert_access
  ) _;

  return rows_touched;
end;
$$;

comment on function public.populate_from_cpad(int) is
  'Layer 2: governance + access from CPAD. Match-quality matrix:
   inside+name=0.95, buffer<=100m+name=0.85, inside-only=0.75,
   buffer<=500m+name=0.65, buffer<=100m-only=0.50, else skip.
   Ranks per beach by confidence DESC then smallest-area for specificity.';
