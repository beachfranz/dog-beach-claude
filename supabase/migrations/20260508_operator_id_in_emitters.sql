-- 20260508_operator_id_in_emitters.sql
--
-- Move 1 (Pin #32): wire operator_id into BEP governance emitters.
--
-- For each emitter that produces 'governance' field_group, look up
-- operators.id via the explicit cross-reference column for that source.
-- Write operator_id into BEP. claimed_values still includes the name
-- string for backward compatibility.

begin;

-- ============================================================================
-- CPAD: cross-reference via operators.cpad_agncy_name = cpad_units.mng_agncy
-- ============================================================================
create or replace function public.populate_from_cpad_gold(p_fid bigint default null)
returns integer
language plpgsql
as $function$
declare rows_touched int;
begin
  with target as (
    select fid,
           coalesce(display_name_override, name) as full_name,
           geom, state
      from public.beaches_gold
     where (p_fid is null or fid = p_fid)
       and geom is not null
       and is_active = true
  ),
  candidates as (
    select t.fid, t.state, c.unit_name, c.mng_ag_lev, c.mng_agncy, c.access_typ,
      st_contains(c.geom, t.geom::geometry)               as inside,
      st_distance(t.geom, c.geom::geography)              as dist_m,
      st_area(c.geom::geography)                          as area_m2,
      cardinality(public.shared_name_tokens(t.full_name, c.unit_name)) > 0 as name_match
    from target t
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
      end as access_status,
      -- ★ Operator FK lookup: cpad_agncy_name on operators table
      (select o.id from public.operators o
        where o.cpad_agncy_name = best.mng_agncy
          and o.state_code = best.state
          and o.is_active = true
        limit 1) as operator_id
    from best
  ),
  upsert_governance as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, operator_id, updated_at)
    select fid, 'governance', 'cpad', confidence,
      jsonb_build_object(
        'type', gov_type,
        'name', public.canonical_agency_name(gov_type, mng_agncy)
      ),
      operator_id, now()
    from with_type
    where mng_ag_lev is not null
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          operator_id    = excluded.operator_id,
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

-- ============================================================================
-- PAD-US: cross-reference via operators.pad_us_mng_name array
-- ============================================================================
create or replace function public.populate_from_pad_us_gold(p_fid bigint default null)
returns integer
language plpgsql
as $function$
declare rows_touched int;
begin
  with target as (
    select fid,
           coalesce(display_name_override, name) as full_name,
           geom, state
      from public.beaches_gold
     where (p_fid is null or fid = p_fid)
       and geom is not null
       and is_active = true
  ),
  candidates as (
    select t.fid, t.state, c.unit_name, c.mng_type, c.mng_name,
      c.raw_attrs->>'Pub_Access' as pub_access_code,
      st_contains(c.geom, t.geom::geometry)               as inside,
      st_distance(t.geom, c.geom::geography)              as dist_m,
      st_area(c.geom::geography)                          as area_m2,
      cardinality(public.shared_name_tokens(t.full_name, c.unit_name)) > 0 as name_match
    from target t
    join public.pad_us_units c on st_dwithin(c.geom, t.geom::geometry, 0.01)
   where st_distance(t.geom, c.geom::geography) <= 500
  ),
  with_conf as (
    select *,
      case
        when inside     and name_match                       then 0.90
        when not inside and dist_m <= 100 and name_match     then 0.80
        when inside     and not name_match                   then 0.70
        when not inside and dist_m <= 500 and name_match     then 0.60
        when not inside and dist_m <= 100 and not name_match then 0.45
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
      case mng_type
        when 'FED'  then 'federal'
        when 'STAT' then 'state'
        when 'LOC'  then 'local'
        when 'DIST' then 'special_district'
        when 'NGO'  then 'nonprofit'
        when 'PVT'  then 'private'
        when 'TRIB' then 'tribal'
        when 'JNT'  then 'joint'
        else             'unknown'
      end as gov_type,
      case pub_access_code
        when 'OA' then 'public'
        when 'RA' then 'restricted'
        when 'XA' then 'private'
        when 'UK' then 'unknown'
        else case mng_type
          when 'PVT'  then 'private'
          when 'TRIB' then 'restricted'
          when 'UNK'  then 'unknown'
          else             'public'
        end
      end as access_status,
      -- ★ Operator FK lookup: pad_us_mng_name array on operators
      (select o.id from public.operators o
        where best.mng_name = any(o.pad_us_mng_name)
          and o.state_code = best.state
          and o.is_active = true
        limit 1) as operator_id
    from best
  ),
  upsert_governance as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, operator_id, updated_at)
    select fid, 'governance', 'pad_us', confidence,
      jsonb_build_object(
        'type', gov_type,
        'name', public.canonical_agency_name(gov_type, mng_name)
      ),
      operator_id, now()
    from with_type
    where mng_type is not null
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          operator_id    = excluded.operator_id,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  upsert_access as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'access', 'pad_us', confidence,
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

-- ============================================================================
-- PAD-US name seed: many CA operators have a similar canonical_name to
-- the pad_us mng_name. Seed pad_us_mng_name from canonical_name pattern.
-- This is a starting point; manual curation can refine.
-- ============================================================================
update public.operators o
   set pad_us_mng_name = array[o.canonical_name]
 where o.pad_us_mng_name is null
   and o.canonical_name is not null;

commit;
