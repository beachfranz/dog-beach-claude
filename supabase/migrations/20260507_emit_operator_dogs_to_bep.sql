-- 20260507_emit_operator_dogs_to_bep.sql
--
-- Bridges operator_dogs_policy into the BEP-driven consumer pipeline.
-- Closes the third (and last expected) parallel-policy gap, after
-- _emit_evidence_from_cpad_unit_dogs_policy and
-- _emit_evidence_from_pad_us_dogs_policy (both 2026-05-06).
--
-- Operators carry geometry directly (operators.geom), so the spatial
-- join is single-hop: beaches_gold × operators.geom × operator_dogs_policy.
-- When multiple operators cover one beach (e.g., a CDPR unit nested
-- inside a county), pick the smallest-area operator (most specific).
--
-- Source name: 'operator_dogs_policy_v1'. Confidence: 0.70 if
-- default_rule is set (operator-level extraction is the strongest
-- per-operator signal we have).

begin;

-- Allow the new source value
alter table public.beach_enrichment_provenance
  drop constraint beach_enrichment_provenance_source_check;
alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_source_check
  check (source = any (array[
    'manual','manual_curator','plz','cpad','tiger_places','ccc','llm',
    'web_scrape','research','csp_parks','park_operators','nps_places',
    'tribal_lands','military_bases','pad_us','sma_code_mappings',
    'jurisdictions','csp_places','name','governing_body','park_url',
    'park_url_buffer_attribution','park_url_governance','old_school_llm',
    'counties','json_explode','unified_v1','city_policy','county_policy',
    'text_repass_v1','cpad_unit_dogs_policy_v1','pad_us_dogs_policy_v1',
    'operator_dogs_policy_v1'
  ]));

-- ----------------------------------------------------------------------
-- Emitter
-- ----------------------------------------------------------------------
create or replace function public._emit_evidence_from_operator_dogs_policy(
  p_fid bigint default null
) returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $$
declare
  ins int := 0; upd int := 0; skp int := 0;
begin
  with ranked as (
    select
      bg.fid as gold_fid,
      o.id as operator_id,
      o.canonical_name,
      o.short_name,
      o.level,
      o.website,
      o.dog_policy_url,
      odp.default_rule,
      odp.leash_required,
      odp.area_sand, odp.area_water, odp.area_picnic_area,
      odp.area_parking_lot, odp.area_trails, odp.area_campground,
      odp.designated_dog_zones, odp.prohibited_areas,
      odp.summary, odp.ordinance_reference, odp.source_url,
      o.footprint_area_km2,
      row_number() over (
        partition by bg.fid
        order by
          (odp.default_rule is not null) desc,
          coalesce(o.footprint_area_km2, 1e9) asc  -- smallest operator wins
      ) as rk
    from public.beaches_gold bg
    join public.operators o on ST_Intersects(o.geom, bg.geom)
    join public.operator_dogs_policy odp on odp.operator_id = o.id
    where bg.is_active
      and (p_fid is null or bg.fid = p_fid)
      and odp.default_rule is not null
  ),
  picks as (select * from ranked where rk = 1),
  payload as (
    select
      gold_fid,
      coalesce(canonical_name, short_name, 'operator:' || operator_id::text) as cpad_unit_name,
      coalesce(dog_policy_url, source_url, website)                          as url_used,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',           default_rule,
        'default_rule',      default_rule,
        'leash_required',    case when leash_required is true then 'true'
                                  when leash_required is false then 'false'
                                  else null end,
        'off_leash_exists',  case when (
                                area_sand = 'off_leash' or area_water = 'off_leash'
                                or area_trails = 'off_leash' or area_campground = 'off_leash'
                              ) then 'true'
                              when (
                                area_sand is not null or area_water is not null
                                or area_trails is not null
                              ) then 'false'
                              else null end,
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
        'summary',         summary,
        'ordinance_ref',   ordinance_reference,
        'operator_level',  level,
        'operator_id',     operator_id
      )) as claimed_values,
      0.70 as confidence
    from picks
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values,
       confidence, is_canonical, cpad_unit_name, extraction_type, cpad_role,
       updated_at)
    select
      p.gold_fid, 'dogs', 'operator_dogs_policy_v1', p.url_used, p.claimed_values,
      p.confidence, false,
      p.cpad_unit_name, 'derived_url_crawl', 'beach_access',
      now()
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

grant execute on function public._emit_evidence_from_operator_dogs_policy(bigint)
  to authenticated, service_role;

comment on function public._emit_evidence_from_operator_dogs_policy(bigint) is
  'Emits BEP rows from operator_dogs_policy. One row per beach, smallest
   covering operator with non-null default_rule wins. Triggers consensus
   + policy promote chain via tg_promote_dogs_chain.';

commit;
