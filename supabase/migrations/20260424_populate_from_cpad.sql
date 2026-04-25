-- populate_from_cpad — first source-specific Layer 2 population RPC (2026-04-24)
--
-- For each in-scope beach, finds the best matching CPAD polygon and emits
-- evidence rows for governance + access. Per the design, CPAD is the most
-- authoritative source for CA so this is the template the other source-
-- specific populators will copy.
--
-- Within-source candidate selection (concern B):
--   1. Inside-polygon + name match → 0.95 confidence
--   2. Inside-polygon, no name match → 0.85
--   3. Within 100m buffer + name match → 0.75
--   4. Within 100m buffer, no name match → 0.60
--   When multiple inside-polygon candidates: smallest area wins (most specific).
--
-- The function always upserts (one row per fid+field_group+source) — re-runs
-- update in place (rule #10 same-source freshness).

create or replace function public.populate_from_cpad(p_fid int default null)
returns int
language plpgsql
as $$
declare
  rows_touched int := 0;
begin
  with candidates as (
    select
      s.fid,
      c.unit_name,
      c.mng_ag_lev,
      c.mng_agncy,
      c.access_typ,
      c.geom              as cpad_geom,
      s.display_name,
      st_contains(c.geom, s.geom::geometry)             as inside,
      st_distance(s.geom, c.geom::geography)            as dist_m,
      st_area(c.geom::geography)                        as area_m2,
      cardinality(public.shared_name_tokens(s.display_name, c.unit_name)) > 0
                                                         as name_match
    from public.us_beach_points_staging s
    join public.cpad_units c
      on st_dwithin(c.geom::geography, s.geom, 100)     -- 100m max buffer
    where (p_fid is null or s.fid = p_fid)
      and s.geom is not null
  ),
  ranked as (
    select *,
      row_number() over (
        partition by fid
        order by inside desc,            -- inside polygon beats outside
                 name_match desc,        -- name match preferred
                 area_m2 asc             -- smallest polygon among ties = most specific
      ) as rnk
    from candidates
  ),
  best as (
    select * from ranked where rnk = 1
  ),
  -- Compute confidence per match-quality matrix
  with_conf as (
    select *,
      case
        when inside     and name_match then 0.95
        when inside                    then 0.85
        when not inside and name_match then 0.75
        else                                0.60
      end as confidence
    from best
  ),
  -- Map mng_ag_lev to our governing_body_type enum
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
    from with_conf
  ),
  -- Emit governance evidence rows
  upsert_governance as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select
      fid, 'governance', 'cpad', confidence,
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
  -- Emit access evidence rows (only when CPAD has access_typ filled)
  upsert_access as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select
      fid, 'access', 'cpad', confidence,
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
  'Layer-2 population: emit governance + access evidence rows from CPAD into beach_enrichment_provenance. Picks best matching polygon per beach (inside > buffer, name-match preferred, smallest area among ties). Re-runs UPSERT in place. Pass p_fid for one beach, null for all. Returns count of evidence rows touched.';
