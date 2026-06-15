-- 20260614g_bep_operator_id_emitter_wiring.sql
--
-- Wire bep.operator_id (column added 20260508_operator_canon_schema.sql:30,
-- index bep_operator_id_idx waiting unused since) into the seven dogs BEP
-- emitters that already know their operator at INSERT time:
--
--   populate_from_city_dog_policy_gold        — op.id from existing JOIN
--   populate_from_county_dog_policy_gold      — op.id from existing JOIN
--   _emit_evidence_from_operator_dogs_policy  — o.id from existing JOIN
--   _emit_evidence_from_operator_policy_exceptions — operator_id from existing JOIN
--   _emit_evidence_from_cpad_unit_dogs_policy — NEW LEFT JOIN operators on cu.mng_agncy
--   _emit_evidence_from_pad_us_dogs_policy    — NEW LEFT JOIN operators on mng_name
--   _emit_bep_from_policy_source              — policy_source.issuing_operator_id
--
-- Foundation for 20260614i operator-consistency tiebreak in the resolver.
-- Sibling 20260614h backfills operator_id on existing rows.
--
-- Reversibility: revert the seven CREATE OR REPLACE blocks. No schema
-- change, no data change at apply time (only the function bodies).

BEGIN;

-- ─── 1. populate_from_city_dog_policy_gold ────────────────────────────────
CREATE OR REPLACE FUNCTION public.populate_from_city_dog_policy_gold(p_fid bigint DEFAULT NULL::bigint)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare rows_touched int;
begin
  with matches as (
    select g.fid, g.name as beach_name,
           op.canonical_name as operator_name, op.dog_policy_url, op.id as op_id,
           odp.default_rule, odp.leash_required, odp.summary,
           odp.area_sand, odp.area_water, odp.area_picnic_area,
           odp.area_parking_lot, odp.area_trails, odp.area_campground,
           odp.designated_dog_zones, odp.prohibited_areas,
           odp.time_windows, odp.seasonal_closures, odp.spatial_zones,
           coalesce(odp.pass_b_confidence, odp.pass_a_confidence) as op_conf,
           odp.source_url
      from public.beaches_gold g
      join public.operators op
        on op.jurisdiction_id = g.c1_jurisdiction_id
       and op.level = 'city'
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id
       and odp.policy_found = true
     where (p_fid is null or g.fid = p_fid)
       and g.is_active
       and g.c1_jurisdiction_id is not null
       and not public._url_slug_specific_beach_mismatch(
             coalesce(odp.source_url, op.dog_policy_url), g.name
           )
  ),
  built as (
    select fid, op_id, operator_name, dog_policy_url, op_conf, source_url,
           jsonb_strip_nulls(jsonb_build_object(
             'allowed', case
               when default_rule in ('yes','allowed','restricted','seasonal') then 'yes'
               when default_rule in ('no','prohibited','not_allowed') then 'no'
               else null end,
             'leash_required', case
               when leash_required is true  then 'on_leash'
               when leash_required is false then 'off_leash' else null end,
             'notes', summary,
             'area_sand',         area_sand,
             'area_water',        area_water,
             'area_picnic_area',  area_picnic_area,
             'area_parking_lot',  area_parking_lot,
             'area_trails',       area_trails,
             'area_campground',   area_campground,
             'designated_dog_zones', designated_dog_zones,
             'prohibited_areas',     prohibited_areas,
             'time_windows',         time_windows,
             'seasonal_closures',    seasonal_closures,
             'operator_name',        operator_name
           )) as v
      from matches
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, source_url, operator_id, updated_at)
    select fid, 'dogs', 'city_policy',
      coalesce(op_conf, 0.85),
      v, coalesce(source_url, dog_policy_url), op_id, now()
    from built where v <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          source_url     = excluded.source_url,
          operator_id    = excluded.operator_id,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from upserted;
  return rows_touched;
end;
$function$;

-- ─── 2. populate_from_county_dog_policy_gold ──────────────────────────────
CREATE OR REPLACE FUNCTION public.populate_from_county_dog_policy_gold(p_fid bigint DEFAULT NULL::bigint)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare rows_touched int;
begin
  with matches as (
    select g.fid, g.name as beach_name,
           op.canonical_name as operator_name, op.dog_policy_url, op.id as op_id,
           odp.default_rule, odp.leash_required, odp.summary,
           odp.area_sand, odp.area_water, odp.area_picnic_area,
           odp.area_parking_lot, odp.area_trails, odp.area_campground,
           odp.designated_dog_zones, odp.prohibited_areas,
           odp.time_windows, odp.seasonal_closures, odp.spatial_zones,
           coalesce(odp.pass_b_confidence, odp.pass_a_confidence) as op_conf,
           odp.source_url
      from public.beaches_gold g
      join public.operators op
        on op.county_geoid = g.county_geoid
       and op.level = 'county'
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id
       and odp.policy_found = true
     where (p_fid is null or g.fid = p_fid)
       and g.is_active
       and g.county_geoid is not null
       and not exists (
         select 1 from public.operators op_city
           join public.operator_dogs_policy odp_city
             on odp_city.operator_id = op_city.id
            and odp_city.policy_found = true
          where op_city.level = 'city'
            and op_city.jurisdiction_id = g.c1_jurisdiction_id
       )
       and not public._url_slug_specific_beach_mismatch(
             coalesce(odp.source_url, op.dog_policy_url), g.name
           )
  ),
  built as (
    select fid, op_id, operator_name, dog_policy_url, op_conf, source_url,
           jsonb_strip_nulls(jsonb_build_object(
             'allowed', case
               when default_rule in ('yes','allowed','restricted','seasonal') then 'yes'
               when default_rule in ('no','prohibited','not_allowed') then 'no'
               else null end,
             'leash_required', case
               when leash_required is true  then 'on_leash'
               when leash_required is false then 'off_leash' else null end,
             'notes', summary,
             'area_sand',         area_sand,
             'area_water',        area_water,
             'area_picnic_area',  area_picnic_area,
             'area_parking_lot',  area_parking_lot,
             'area_trails',       area_trails,
             'area_campground',   area_campground,
             'designated_dog_zones', designated_dog_zones,
             'prohibited_areas',     prohibited_areas,
             'time_windows',         time_windows,
             'seasonal_closures',    seasonal_closures,
             'operator_name',        operator_name
           )) as v
      from matches
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, source_url, operator_id, updated_at)
    select fid, 'dogs', 'county_policy',
      LEAST(coalesce(op_conf, 0.85), 0.93),
      v, coalesce(source_url, dog_policy_url), op_id, now()
    from built where v <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          source_url     = excluded.source_url,
          operator_id    = excluded.operator_id,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from upserted;
  return rows_touched;
end;
$function$;

-- ─── 3. _emit_evidence_from_operator_dogs_policy ──────────────────────────
CREATE OR REPLACE FUNCTION public._emit_evidence_from_operator_dogs_policy(p_fid bigint DEFAULT NULL::bigint)
 RETURNS TABLE(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
 LANGUAGE plpgsql
AS $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with ranked as (
    select bg.fid as gold_fid, o.id as operator_id, o.canonical_name, o.short_name,
           o.level, o.website, o.dog_policy_url,
           odp.default_rule, odp.leash_required,
           odp.area_sand, odp.area_water, odp.area_picnic_area,
           odp.area_parking_lot, odp.area_trails, odp.area_campground,
           odp.designated_dog_zones, odp.prohibited_areas,
           odp.summary, odp.ordinance_reference, odp.source_url,
           o.footprint_area_km2,
           row_number() over (
             partition by bg.fid
             order by (odp.default_rule is not null) desc,
                      coalesce(o.footprint_area_km2, 1e9) asc
           ) as rk
    from public.beaches_gold bg
    join public.operators o on ST_Intersects(o.geom, bg.geom)
    join public.operator_dogs_policy odp on odp.operator_id = o.id
    where bg.is_active and (p_fid is null or bg.fid = p_fid)
      and odp.default_rule is not null
  ),
  picks as (select * from ranked where rk = 1),
  payload as (
    select gold_fid, operator_id,
      coalesce(canonical_name, short_name, 'operator:' || operator_id::text) as cpad_unit_name,
      coalesce(dog_policy_url, source_url, website) as url_used,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed', default_rule, 'default_rule', default_rule,
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
        'summary', summary, 'ordinance_ref', ordinance_reference,
        'operator_level', level, 'operator_id', operator_id
      )) as claimed_values,
      0.70 as confidence
    from picks
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values,
       confidence, is_canonical, cpad_unit_name, extraction_type, cpad_role, operator_id, updated_at)
    select p.gold_fid, 'dogs', 'operator_dogs_policy_v1', p.url_used, p.claimed_values,
           p.confidence, false, p.cpad_unit_name, 'derived_url_crawl', 'beach_access', p.operator_id, now()
    from payload p where p.claimed_values <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set source_url = excluded.source_url,
          claimed_values = excluded.claimed_values,
          confidence = excluded.confidence,
          cpad_unit_name = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          cpad_role = excluded.cpad_role,
          operator_id = excluded.operator_id,
          updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
       or beach_enrichment_provenance.confidence is distinct from excluded.confidence
       or beach_enrichment_provenance.operator_id is distinct from excluded.operator_id
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end $function$;

-- ─── 4. _emit_evidence_from_operator_policy_exceptions ────────────────────
CREATE OR REPLACE FUNCTION public._emit_evidence_from_operator_policy_exceptions(p_fid bigint DEFAULT NULL::bigint)
 RETURNS TABLE(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
 LANGUAGE plpgsql
AS $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with candidate_pairs as (
    select bg.fid as gold_fid, ope.id as exception_id, ope.rule as raw_rule,
           ope.beach_name as exception_beach_name, ope.source_quote, ope.source_url,
           ope.operator_id, o.canonical_name as operator_name,
           similarity(lower(bg.name), lower(ope.beach_name)) as sim_score,
           (lower(bg.name) like '%' || lower(ope.beach_name) || '%'
            or lower(ope.beach_name) like '%' || lower(bg.name) || '%') as substring_match,
           (select count(*) from regexp_split_to_table(lower(bg.name), '[^a-z0-9]+') t
             where t <> '' and lower(ope.beach_name) like '%' || t || '%') as token_overlap
      from public.beaches_gold bg
      join public.operators o on ST_Intersects(o.geom, bg.geom)
      join public.operator_policy_exceptions ope on ope.operator_id = o.id
     where bg.is_active and (p_fid is null or bg.fid = p_fid)
       and similarity(lower(bg.name), lower(ope.beach_name)) > 0.4
  ),
  best_match_per_beach as (
    select distinct on (gold_fid) * from candidate_pairs
     order by gold_fid, substring_match desc, token_overlap desc, sim_score desc, exception_id asc
  ),
  payload as (
    select gold_fid, operator_name, source_url, exception_beach_name,
      sim_score, operator_id, raw_rule, source_quote, substring_match,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed', case raw_rule when 'allowed' then 'yes' when 'prohibited' then 'no'
          when 'no' then 'no' when 'off_leash' then 'yes' when 'restricted' then 'mixed' else null end,
        'leash_required', case raw_rule when 'off_leash' then 'off_leash'
          when 'allowed' then 'on_leash' else null end,
        'off_leash_exists', case when raw_rule = 'off_leash' then 'true' else null end,
        'source_quote', source_quote, 'matched_exception_name', exception_beach_name,
        'similarity_score', round(sim_score::numeric, 3)::text,
        'substring_match', case when substring_match then 'true' else 'false' end,
        'operator_id', operator_id::text)) as cv,
      case when substring_match then 0.95
           when sim_score > 0.7 then 0.80
           when sim_score > 0.5 then 0.70
           else 0.60 end as confidence
    from best_match_per_beach
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, cpad_unit_name, extraction_type, cpad_role, operator_id, updated_at)
    select gold_fid, 'dogs', 'operator_policy_exceptions_v1', source_url,
           cv, confidence, false, operator_name, 'derived_url_crawl', 'beach_access', operator_id, now()
      from payload where cv <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values, confidence = excluded.confidence,
          source_url = excluded.source_url, cpad_unit_name = excluded.cpad_unit_name,
          operator_id = excluded.operator_id, updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
       or beach_enrichment_provenance.confidence is distinct from excluded.confidence
       or beach_enrichment_provenance.operator_id is distinct from excluded.operator_id
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end $function$;

-- ─── 5. _emit_evidence_from_cpad_unit_dogs_policy ─────────────────────────
-- Adds LEFT JOIN to operators on cu.mng_agncy → operators.canonical_name.
CREATE OR REPLACE FUNCTION public._emit_evidence_from_cpad_unit_dogs_policy(p_fid bigint DEFAULT NULL::bigint)
 RETURNS TABLE(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
 LANGUAGE plpgsql
AS $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with ranked as (
    select bg.fid as gold_fid, cu.cpad_unit_id, c.unit_name, cu.url_used,
           cu.dogs_allowed, cu.default_rule, cu.leash_required,
           cu.area_sand, cu.area_water, cu.area_picnic_area,
           cu.area_parking_lot, cu.area_trails, cu.area_campground,
           cu.designated_dog_zones, cu.prohibited_areas,
           cu.seasonal_rules, cu.time_windows,
           cu.source_quote, cu.ordinance_ref,
           op.id as operator_id,
           ST_Area(c.geom::geography) as unit_area_m2,
           row_number() over (
             partition by bg.fid
             order by (cu.dogs_allowed is not null) desc, ST_Area(c.geom::geography) asc
           ) as rk
    from public.beaches_gold bg
    join public.cpad_units c on ST_Intersects(c.geom, bg.geom)
    join public.cpad_unit_dogs_policy cu on cu.cpad_unit_id = c.unit_id
    left join public.operators op
      on lower(op.canonical_name) = lower(c.mng_agncy)
     and op.is_canonical and op.is_active
    where bg.is_active and (p_fid is null or bg.fid = p_fid)
      and (cu.dogs_allowed is not null or cu.default_rule is not null)
  ),
  picks as (select * from ranked where rk = 1),
  payload as (
    select gold_fid, operator_id,
      coalesce(unit_name, 'cpad:' || cpad_unit_id::text) as cpad_unit_name,
      url_used,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed', dogs_allowed, 'default_rule', default_rule,
        'leash_required',
          case when leash_required is true  then 'on_leash'
               when leash_required is false then 'off_leash'
               else null end,
        'off_leash_exists',
          case when (area_sand='off_leash' or area_water='off_leash'
                  or area_picnic_area='off_leash' or area_parking_lot='off_leash'
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
        'seasonal_rules', seasonal_rules,
        'time_windows',   time_windows,
        'ordinance_ref',  ordinance_ref,
        'source_quote',   source_quote
      )) as claimed_values,
      case when dogs_allowed is not null then 0.70 else 0.55 end as confidence
    from picks
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values,
       confidence, is_canonical, cpad_unit_name, extraction_type, cpad_role, operator_id, updated_at)
    select p.gold_fid, 'dogs', 'cpad_unit_dogs_policy_v1', p.url_used, p.claimed_values,
           p.confidence, false, p.cpad_unit_name, 'cpad_source', 'beach_access', p.operator_id, now()
    from payload p where p.claimed_values <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set source_url = excluded.source_url,
          claimed_values = excluded.claimed_values,
          confidence = excluded.confidence,
          cpad_unit_name = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          cpad_role = excluded.cpad_role,
          operator_id = excluded.operator_id,
          updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
       or beach_enrichment_provenance.confidence is distinct from excluded.confidence
       or beach_enrichment_provenance.operator_id is distinct from excluded.operator_id
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end $function$;

-- ─── 6. _emit_evidence_from_pad_us_dogs_policy ────────────────────────────
-- Adds LEFT JOIN to operators on pad_us.mng_name → operators.pad_us_mng_name[].
-- Uses beaches_gold for state context since beach_polygon_membership doesn't carry it.
CREATE OR REPLACE FUNCTION public._emit_evidence_from_pad_us_dogs_policy(p_fid bigint DEFAULT NULL::bigint)
 RETURNS TABLE(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
 LANGUAGE plpgsql
AS $function$
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
           op.id as operator_id,
           row_number() over (
             partition by m.gold_fid
             order by (unit_pol.id is not null) desc,
                      (mng_pol.id is not null) desc,
                      m.area_m2 asc
           ) as rk
      from public.beach_polygon_membership m
      join public.beaches_gold bg on bg.fid = m.gold_fid
      left join public.pad_us_unit_dogs_policy unit_pol on unit_pol.unit_id = m.polygon_id::bigint
      left join public.pad_us_unit_dogs_policy mng_pol
        on mng_pol.mng_name = m.mng_name and mng_pol.unit_id is null
      left join public.operators op
        on m.mng_name = any(op.pad_us_mng_name)
       and op.state_code = bg.state
       and op.is_canonical and op.is_active
     where m.polygon_kind = 'pad_us_unit'
       and m.match_strength = 4
       and (p_fid is null or m.gold_fid = p_fid)
       and (unit_pol.id is not null or mng_pol.id is not null)
  ),
  picks as (select * from ranked where rk = 1),
  payload as (
    select gold_fid, operator_id,
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
      case when is_per_unit then 0.85 else 0.75 end as confidence
      from picks
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values,
       confidence, is_canonical, cpad_unit_name, extraction_type, cpad_role, operator_id, updated_at)
    select p.gold_fid, 'dogs', 'pad_us_dogs_policy_v1', p.url_used, p.claimed_values,
           p.confidence, false, p.cpad_unit_name, 'derived_url_crawl', 'beach_access', p.operator_id, now()
    from payload p where p.claimed_values <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set source_url = excluded.source_url,
          claimed_values = excluded.claimed_values,
          confidence = excluded.confidence,
          cpad_unit_name = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          cpad_role = excluded.cpad_role,
          operator_id = excluded.operator_id,
          updated_at = now()
      where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
         or beach_enrichment_provenance.confidence is distinct from excluded.confidence
         or beach_enrichment_provenance.operator_id is distinct from excluded.operator_id
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end $function$;

-- ─── 7. _emit_bep_from_policy_source ──────────────────────────────────────
-- Pulls policy_source.issuing_operator_id and writes to bep.operator_id.
CREATE OR REPLACE FUNCTION public._emit_bep_from_policy_source(p_beach_fid bigint, p_policy_source_id bigint)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_source_tag TEXT := 'codified_v1:ps_' || p_policy_source_id::TEXT;
  v_n_bps      INT;
  v_ps_url     TEXT;
  v_ps_cite    TEXT;
  v_ps_subtype TEXT;
  v_ps_operator BIGINT;
  v_claimed    JSONB;
  v_has_temporal BOOL;
  v_rule_set     JSONB;
  v_temporal_arr JSONB;
  v_verbatim_arr JSONB;
  v_sand_off_leash       BOOL;
  v_sand_on_leash        BOOL;
  v_sand_not_allowed     BOOL;
  v_any_off_leash        BOOL;
  v_any_on_leash         BOOL;
  v_any_not_allowed      BOOL;
  v_temporal_not_allowed BOOL;
  v_temporal_on_leash    BOOL;
  v_temporal_off_leash   BOOL;
BEGIN
  SELECT count(*) INTO v_n_bps
    FROM public.beach_policy_source
   WHERE beach_fid = p_beach_fid
     AND policy_source_id = p_policy_source_id
     AND operative_status = 'operative';

  IF v_n_bps = 0 THEN
    DELETE FROM public.beach_enrichment_provenance
     WHERE gold_fid = p_beach_fid
       AND source   = v_source_tag
       AND field_group = 'dogs';
    RETURN;
  END IF;

  SELECT ps.source_url, ps.citation, ps.subtype, ps.issuing_operator_id
    INTO v_ps_url, v_ps_cite, v_ps_subtype, v_ps_operator
    FROM public.policy_source ps
   WHERE ps.id = p_policy_source_id;

  SELECT
      jsonb_agg(DISTINCT jsonb_build_object(
        'section',       bps.section,
        'rule',          bps.rule,
        'rule_modifier', bps.rule_modifier,
        'region_name',   bps.region_name
      )),
      jsonb_agg(DISTINCT bps.evidence_verbatim) FILTER (WHERE bps.evidence_verbatim IS NOT NULL)
    INTO v_rule_set, v_verbatim_arr
    FROM public.beach_policy_source bps
   WHERE bps.beach_fid = p_beach_fid
     AND bps.policy_source_id = p_policy_source_id
     AND bps.operative_status = 'operative';

  SELECT
      jsonb_agg(jsonb_build_object(
        'section',           bpst.section,
        'exception_rule',    bpst.exception_rule,
        'window_kind',       bpst.window_kind,
        'effective_from_md', bpst.effective_from_md,
        'effective_to_md',   bpst.effective_to_md,
        'daily_start',       to_char(bpst.daily_start, 'HH24:MI'),
        'daily_end',         to_char(bpst.daily_end,   'HH24:MI'),
        'season_label',      bpst.season_label
      ))
    INTO v_temporal_arr
    FROM public.beach_policy_source_temporal bpst
    JOIN public.beach_policy_source bps ON bps.id = bpst.bps_id
   WHERE bps.beach_fid = p_beach_fid
     AND bps.policy_source_id = p_policy_source_id
     AND bps.operative_status = 'operative';

  v_has_temporal := v_temporal_arr IS NOT NULL AND jsonb_array_length(v_temporal_arr) > 0;

  v_sand_off_leash := EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_rule_set) r
     WHERE r->>'section' = 'sand'
       AND r->>'rule' IN ('off_leash','off_leash_voice_control')
  );
  v_sand_on_leash := EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_rule_set) r
     WHERE r->>'section' = 'sand'
       AND r->>'rule' IN ('on_leash','on_leash_or_voice')
  );
  v_sand_not_allowed := EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_rule_set) r
     WHERE r->>'section' = 'sand'
       AND r->>'rule' IN ('not_allowed','prohibited','closed')
  );
  v_any_off_leash := EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_rule_set) r
     WHERE r->>'rule' IN ('off_leash','off_leash_voice_control')
  );
  v_any_on_leash := EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_rule_set) r
     WHERE r->>'rule' IN ('on_leash','on_leash_or_voice')
  );
  v_any_not_allowed := EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_rule_set) r
     WHERE r->>'rule' IN ('not_allowed','prohibited','closed')
  );

  v_temporal_not_allowed := v_has_temporal AND EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_temporal_arr) t
     WHERE t->>'exception_rule' = 'not_allowed'
  );
  v_temporal_on_leash := v_has_temporal AND EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_temporal_arr) t
     WHERE t->>'exception_rule' = 'on_leash'
  );
  v_temporal_off_leash := v_has_temporal AND EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_temporal_arr) t
     WHERE t->>'exception_rule' IN ('off_leash','off_leash_voice_control')
  );

  v_claimed := jsonb_build_object(
    'allowed', CASE
      WHEN v_temporal_not_allowed THEN 'mixed'
      WHEN v_sand_not_allowed AND NOT v_sand_off_leash AND NOT v_sand_on_leash THEN 'no'
      WHEN v_sand_off_leash OR v_sand_on_leash THEN 'yes'
      WHEN v_any_not_allowed AND NOT v_any_off_leash AND NOT v_any_on_leash THEN 'no'
      WHEN v_any_off_leash OR v_any_on_leash THEN 'yes'
      ELSE 'yes'
    END,
    'leash_required', CASE
      WHEN v_sand_off_leash AND v_sand_on_leash THEN 'mixed'
      WHEN v_sand_off_leash AND v_temporal_on_leash THEN 'mixed'
      WHEN v_sand_on_leash  AND v_temporal_off_leash THEN 'mixed'
      WHEN v_sand_off_leash THEN 'not_required'
      WHEN v_sand_on_leash THEN 'required'
      WHEN v_any_off_leash AND v_temporal_on_leash THEN 'mixed'
      WHEN v_any_on_leash  AND v_temporal_off_leash THEN 'mixed'
      WHEN v_any_off_leash AND v_any_on_leash THEN 'required'
      WHEN v_any_off_leash THEN 'not_required'
      WHEN v_any_on_leash THEN 'required'
      ELSE 'unknown'
    END,
    'rules',         v_rule_set,
    'time_windows',  v_temporal_arr,
    'evidence',      v_verbatim_arr,
    'citation',      v_ps_cite,
    'subtype',       v_ps_subtype
  );

  INSERT INTO public.beach_enrichment_provenance (
    gold_fid, field_group, source, source_url,
    claimed_values, confidence, is_canonical, policy_source_id,
    operator_id, notes, updated_at
  ) VALUES (
    p_beach_fid, 'dogs', v_source_tag, v_ps_url,
    v_claimed, 0.95, TRUE, p_policy_source_id,
    v_ps_operator,
    'auto-emitted from beach_policy_source by _emit_bep_from_policy_source',
    now()
  )
  ON CONFLICT (gold_fid, field_group, source) DO UPDATE
   SET source_url       = EXCLUDED.source_url,
       claimed_values   = EXCLUDED.claimed_values,
       confidence       = EXCLUDED.confidence,
       is_canonical     = EXCLUDED.is_canonical,
       policy_source_id = EXCLUDED.policy_source_id,
       operator_id      = EXCLUDED.operator_id,
       notes            = EXCLUDED.notes,
       updated_at       = now();
END;
$function$;

COMMIT;

NOTIFY pgrst, 'reload schema';
