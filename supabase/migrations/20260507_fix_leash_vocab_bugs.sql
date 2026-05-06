-- 20260507_fix_leash_vocab_bugs.sql
--
-- Two compounding bugs surfaced 2026-05-07 in leash_required voting:
--
-- (a) `off_leash_ok` not in _consensus_normalize for leash_policy.
--     old_school_llm (648 votes at 0.74) and research (324 at 0.85)
--     emit `off_leash_ok` as a values; both got silently dropped to
--     NULL during normalization. Hundreds of "off-leash" votes vanishing.
--
-- (b) cpad_unit/pad_us/operator emitters emit boolean 'true'/'false' for
--     leash_required. That column on the source tables IS bool ("is a
--     leash required") but the BEP claim_key is the leash_policy enum
--     (on_leash | off_leash | mixed). 46 votes silently dropped.
--
-- Fix:
--   1. Add `off_leash_ok` → `off_leash` to _consensus_normalize.
--   2. Update cpad_unit / pad_us / operator emitters to emit string
--      enum: leash_required=true → 'on_leash', false → 'off_leash'.
--   3. Re-fan emitters; recompute consensus + promote.

begin;

-- ---- (a) _consensus_normalize ----
create or replace function public._consensus_normalize(p_field text, p_value text)
returns text language sql immutable as $$
  select case
    when p_value is null then null
    when lower(trim(p_value)) in ('unclear','unknown','null','none','') then null
    else case p_field
      when 'dogs_allowed' then case lower(trim(p_value))
        when 'yes' then 'yes'
        when 'no'  then 'no'
        when 'restricted' then 'mixed'
        when 'seasonal'   then 'mixed'
        when 'mixed' then 'mixed'
        else null end
      when 'leash_policy' then case lower(trim(p_value))
        when 'on_leash' then 'on_leash' when 'off_leash' then 'off_leash'
        when 'mixed' then 'mixed' when 'mixed_by_zone' then 'mixed'
        when 'required' then 'on_leash' when 'optional' then 'off_leash'
        when 'off_leash_ok' then 'off_leash'   -- ADDED 2026-05-07
        when 'varies_by_time' then 'mixed'
        else null end
      when 'off_leash_flag' then case lower(trim(p_value))
        when 'true' then 'true' when 'yes' then 'true' when '1' then 'true'
        when 'false' then 'false' when 'no' then 'false' when '0' then 'false'
        else null end
      when 'has_lifeguards' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      when 'has_restrooms' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      when 'has_parking' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      when 'has_showers' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      when 'has_disabled_access' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      else lower(trim(p_value))
    end
  end;
$$;

-- ---- (b) Rewrite existing bool-string votes to enum. Cheaper than
--          re-fanning the emitters; same end-state.
update public.beach_enrichment_provenance
   set claimed_values = jsonb_set(claimed_values, '{leash_required}', '"on_leash"'::jsonb),
       updated_at = now()
 where field_group = 'dogs'
   and source in ('cpad_unit_dogs_policy_v1','pad_us_dogs_policy_v1','operator_dogs_policy_v1')
   and claimed_values->>'leash_required' = 'true';

update public.beach_enrichment_provenance
   set claimed_values = jsonb_set(claimed_values, '{leash_required}', '"off_leash"'::jsonb),
       updated_at = now()
 where field_group = 'dogs'
   and source in ('cpad_unit_dogs_policy_v1','pad_us_dogs_policy_v1','operator_dogs_policy_v1')
   and claimed_values->>'leash_required' = 'false';

-- ---- (b cont.) Update the three emitter functions so future runs emit
--                the correct vocab. Each is one CASE block; the rest of
--                the function is unchanged.

create or replace function public._emit_evidence_from_cpad_unit_dogs_policy(
  p_fid bigint default null
) returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $$
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
           ST_Area(c.geom::geography) as unit_area_m2,
           row_number() over (
             partition by bg.fid
             order by (cu.dogs_allowed is not null) desc, ST_Area(c.geom::geography) asc
           ) as rk
    from public.beaches_gold bg
    join public.cpad_units c on ST_Intersects(c.geom, bg.geom)
    join public.cpad_unit_dogs_policy cu on cu.cpad_unit_id = c.unit_id
    where bg.is_active and (p_fid is null or bg.fid = p_fid)
      and (cu.dogs_allowed is not null or cu.default_rule is not null)
  ),
  picks as (select * from ranked where rk = 1),
  payload as (
    select gold_fid,
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
       confidence, is_canonical, cpad_unit_name, extraction_type, cpad_role, updated_at)
    select p.gold_fid, 'dogs', 'cpad_unit_dogs_policy_v1', p.url_used, p.claimed_values,
           p.confidence, false, p.cpad_unit_name, 'cpad_source', 'beach_access', now()
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
end $$;


create or replace function public._emit_evidence_from_operator_dogs_policy(
  p_fid bigint default null
) returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $$
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
    select gold_fid,
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
       confidence, is_canonical, cpad_unit_name, extraction_type, cpad_role, updated_at)
    select p.gold_fid, 'dogs', 'operator_dogs_policy_v1', p.url_used, p.claimed_values,
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
end $$;


create or replace function public._emit_evidence_from_pad_us_dogs_policy(
  p_fid bigint default null
) returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with ranked as (
    select bg.fid as gold_fid, pu.unit_id, pu.unit_name, pu.mng_name, pu.mng_type,
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
           ST_Area(pu.geom::geography) as unit_area_m2,
           row_number() over (
             partition by bg.fid
             order by (unit_pol.id is not null) desc,
                      (mng_pol.id is not null) desc,
                      ST_Area(pu.geom::geography) asc
           ) as rk
    from public.beaches_gold bg
    join public.pad_us_units pu on ST_Intersects(pu.geom, bg.geom)
    left join public.pad_us_unit_dogs_policy unit_pol on unit_pol.unit_id = pu.unit_id
    left join public.pad_us_unit_dogs_policy mng_pol
      on mng_pol.mng_name = pu.mng_name and mng_pol.unit_id is null
    where bg.is_active and (p_fid is null or bg.fid = p_fid)
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
end $$;

-- ---- Recompute consensus + repromote (sweeping all dogs fids — leash_required
--      changes affect leash_policy promotion).
do $do$
declare fid_iter bigint;
begin
  for fid_iter in
    select distinct gold_fid from public.beach_enrichment_provenance
     where field_group='dogs' and gold_fid is not null
  loop
    perform public.compute_beach_field_consensus(fid_iter);
    perform public.promote_canonical_dogs_to_beach_dog_policy(fid_iter, 0.5);
  end loop;
end $do$;

commit;
