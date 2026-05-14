-- 20260514_pad_us_cpad_populators_from_membership.sql
--
-- Phase 3 of beach_polygon_membership. Rewrites the remaining per-fid
-- populators that still ran their own ST_Intersects:
--   - populate_from_pad_us_gold       (access evidence from PAD-US)
--   - populate_from_cpad_gold         (governance evidence from CPAD, CA)
--   - _emit_evidence_from_pad_us_dogs_policy (dogs evidence from agency defaults)
--
-- All three now read from beach_polygon_membership instead of running
-- ST_Intersects against the source polygon tables. Combined with the
-- Phase 2 rewrite of populate_polygon_containment_gold, the per-fid
-- build_beach_evidence chain has zero ST_Intersects on hot path (when
-- membership is warm).
--
-- claimed_values shape preserved so downstream consensus/resolvers
-- continue unchanged.

begin;

-- ────────────────────────────────────────────────────────────────────────
-- 1. populate_from_pad_us_gold (access)
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.populate_from_pad_us_gold(p_fid bigint default null)
returns integer
language plpgsql
set statement_timeout to '600s'
as $function$
declare rows_touched int;
begin
  with cands as (
    select m.gold_fid as fid, m.polygon_name as unit_name,
           m.mng_type, m.mng_name,
           m.raw_attrs->>'Pub_Access' as pub_access_code,
           (m.match_strength = 4) as inside,
           m.dist_m, m.area_m2
      from public.beach_polygon_membership m
     where m.polygon_kind = 'pad_us_unit'
       and (p_fid is null or m.gold_fid = p_fid)
       and m.dist_m <= 500
  ),
  best as (
    select * from (
      select *, row_number() over (
        partition by fid
        order by case when inside then 0 else 1 end, dist_m, area_m2
      ) as rnk from cands
    ) s where rnk = 1
  ),
  with_access as (
    select fid,
      case pub_access_code
        when 'OA' then 'public' when 'RA' then 'restricted'
        when 'XA' then 'private' when 'UK' then 'unknown'
        else case mng_type
          when 'PVT' then 'private' when 'TRIB' then 'restricted'
          when 'UNK' then 'unknown' else 'public' end
      end as access_status
    from best
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'access', 'pad_us', 0.70,
      jsonb_build_object('status', access_status), now()
    from with_access where access_status is not null
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update set confidence = excluded.confidence,
                  claimed_values = excluded.claimed_values,
                  updated_at = now(),
                  is_canonical = false
    returning 1
  )
  select count(*) into rows_touched from upserted;
  return rows_touched;
end $function$;

-- ────────────────────────────────────────────────────────────────────────
-- 2. populate_from_cpad_gold (governance, CA)
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.populate_from_cpad_gold(p_fid bigint default null)
returns integer
language plpgsql
set statement_timeout to '600s'
as $function$
declare rows_touched int;
begin
  with target as (
    select g.fid, g.state,
           coalesce(g.display_name_override, g.name) as full_name
      from public.beaches_gold g
     where (p_fid is null or g.fid = p_fid)
       and g.is_active = true and g.geom is not null
  ),
  cands as (
    select t.fid, t.state, t.full_name,
           m.polygon_name as unit_name,
           m.mng_type as mng_ag_lev,             -- in membership for cpad_unit, mng_type holds mng_ag_lev
           m.mng_name as mng_agncy,
           m.raw_attrs->>'access_typ' as access_typ,
           (m.match_strength = 4) as inside,
           m.dist_m, m.area_m2,
           cardinality(public.shared_name_tokens(t.full_name, m.polygon_name)) > 0 as name_match
      from target t
      join public.beach_polygon_membership m
        on m.gold_fid = t.fid
       and m.polygon_kind = 'cpad_unit'
     where m.dist_m <= 500
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
    from cands
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
        when 'Special District'        then 'special-district'
        when 'Non Profit'              then 'private'
        when 'Private'                 then 'private'
        else 'other'
      end as op_level
    from best
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, cpad_unit_name, updated_at)
    select fid, 'governance', 'cpad', confidence,
           jsonb_build_object(
             'operator_name', mng_agncy,
             'operator_level', op_level,
             'mng_ag_lev', mng_ag_lev,
             'access_typ', access_typ,
             'inside', inside,
             'dist_m', dist_m,
             'area_m2', area_m2),
           unit_name, now()
      from with_type
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update set confidence = excluded.confidence,
                  claimed_values = excluded.claimed_values,
                  cpad_unit_name = excluded.cpad_unit_name,
                  updated_at = now(),
                  is_canonical = false
    returning 1
  )
  select count(*) into rows_touched from upserted;
  return rows_touched;
end $function$;

-- ────────────────────────────────────────────────────────────────────────
-- 3. _emit_evidence_from_pad_us_dogs_policy (agency-default dogs)
-- ────────────────────────────────────────────────────────────────────────

create or replace function public._emit_evidence_from_pad_us_dogs_policy(p_fid bigint default null)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with ranked as (
    select m.gold_fid, m.polygon_id::bigint as unit_id, m.polygon_name as unit_name,
           m.mng_name, m.mng_type,
           coalesce(unit_pol.dogs_allowed, mng_pol.dogs_allowed) as dogs_allowed,
           coalesce(unit_pol.default_rule, mng_pol.default_rule) as default_rule,
           coalesce(unit_pol.leash_required, mng_pol.leash_required) as leash_required,
           coalesce(unit_pol.area_sand, mng_pol.area_sand) as area_sand,
           coalesce(unit_pol.area_water, mng_pol.area_water) as area_water,
           coalesce(unit_pol.area_picnic_area, mng_pol.area_picnic_area) as area_picnic_area,
           coalesce(unit_pol.area_parking_lot, mng_pol.area_parking_lot) as area_parking_lot,
           coalesce(unit_pol.area_trails, mng_pol.area_trails) as area_trails,
           coalesce(unit_pol.area_campground, mng_pol.area_campground) as area_campground,
           coalesce(unit_pol.designated_dog_zones, mng_pol.designated_dog_zones) as designated_dog_zones,
           coalesce(unit_pol.prohibited_areas, mng_pol.prohibited_areas) as prohibited_areas,
           coalesce(unit_pol.source_quote, mng_pol.source_quote) as source_quote,
           coalesce(unit_pol.url_used, mng_pol.url_used) as url_used,
           (unit_pol.id is not null) as is_per_unit,
           m.area_m2 as unit_area_m2,
           row_number() over (
             partition by m.gold_fid
             order by (unit_pol.id is not null) desc,
                      (mng_pol.id is not null) desc,
                      m.area_m2 asc
           ) as rk
      from public.beach_polygon_membership m
      left join public.pad_us_unit_dogs_policy unit_pol on unit_pol.unit_id = m.polygon_id::bigint
      left join public.pad_us_unit_dogs_policy mng_pol
        on mng_pol.mng_name = m.mng_name and mng_pol.unit_id is null
     where m.polygon_kind = 'pad_us_unit'
       and m.match_strength = 4    -- inside only — matches original ST_Intersects semantics
       and (p_fid is null or m.gold_fid = p_fid)
       and (unit_pol.id is not null or mng_pol.id is not null)
  ),
  picks as (select * from ranked where rk = 1),
  payload as (
    select gold_fid,
      coalesce(unit_name, 'pad_us:' || unit_id::text) as cpad_unit_name,
      url_used,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed', dogs_allowed, 'default_rule', default_rule,
        'leash_required',
          case when leash_required is true  then 'on_leash'
               when leash_required is false then 'off_leash'
               else null end,
        'off_leash_exists',
          case when (area_sand='off_leash' or area_water='off_leash'
                  or area_trails='off_leash' or area_campground='off_leash')
               then 'true'
               when (area_sand is not null or area_water is not null
                  or area_trails is not null) then 'false' else null end,
        'designated_dog_zones', designated_dog_zones,
        'prohibited_areas',     prohibited_areas,
        'areas_evidence',
          nullif(trim(both ' ' from concat_ws('; ',
            case when area_sand        is not null then 'sand: '         || area_sand        end,
            case when area_water       is not null then 'water: '        || area_water       end,
            case when area_trails      is not null then 'trails: '       || area_trails      end,
            case when area_picnic_area is not null then 'picnic_area: '  || area_picnic_area end,
            case when area_parking_lot is not null then 'parking_lot: '  || area_parking_lot end,
            case when area_campground  is not null then 'campground: '   || area_campground  end
          )), ''),
        'source_quote', source_quote,
        'mng_name', mng_name, 'mng_type', mng_type
      )) as claimed_values,
      case when is_per_unit then 0.70 else 0.55 end as confidence
      from picks
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values,
       confidence, is_canonical, cpad_unit_name, extraction_type, cpad_role, updated_at)
    select p.gold_fid, 'dogs', 'pad_us_dogs_policy_v1', p.url_used, p.claimed_values,
           p.confidence, false, p.cpad_unit_name, 'derived_url_crawl', 'beach_access', now()
    from payload p where p.claimed_values <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set source_url = excluded.source_url,
          claimed_values = excluded.claimed_values,
          confidence = excluded.confidence,
          cpad_unit_name = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          cpad_role = excluded.cpad_role,
          updated_at = now()
      where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
         or beach_enrichment_provenance.confidence is distinct from excluded.confidence
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end $function$;

commit;
