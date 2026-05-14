-- 20260514_pad_us_dogs_confidence_bump.sql
--
-- Bumps the confidence assigned to BEP dogs rows emitted by
-- _emit_evidence_from_pad_us_dogs_policy.
--
-- Before:  agency-default = 0.55, per-unit override = 0.70
-- After:   agency-default = 0.75, per-unit override = 0.85
--
-- Why: federal land-mgmt agencies (NPS, USFWS, USFS, USACE, BLM) have
-- STATUTORY authority via CFR (36 CFR §2.15, 50 CFR §26.21, etc.) over
-- their units. Today these defaults were outranked by state default
-- (Oregon Beach Bill at conf 0.65), which is wrong for federal land —
-- ORS 390.605 doesn't apply to USFWS refuges.
--
-- Concretely: 48 of OR's 49 federal-managed scoreable beaches show
-- dogs_allowed='yes' because state default (0.65) > federal default (0.55).
-- After this bump, federal default (0.75) > state default (0.65) and
-- NPS/FWS beaches correctly show 'no', USFS/USACE beaches show 'mixed'.
--
-- Same fix applies to MI (its 5 federal beaches were also mis-attributed).

begin;

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
       and m.match_strength = 4
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
      -- 2026-05-14: confidence bumped (0.55→0.75, 0.70→0.85) so federal
      -- agency rule outranks state default (Oregon Beach Bill at 0.65).
      case when is_per_unit then 0.85 else 0.75 end as confidence
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
