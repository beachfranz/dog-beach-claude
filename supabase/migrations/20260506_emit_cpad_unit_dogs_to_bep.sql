-- 20260506_emit_cpad_unit_dogs_to_bep.sql
--
-- Bridges cpad_unit_dogs_policy into the BEP-driven consumer pipeline.
--
-- Until tonight there were two parallel policy paths:
--   * Legacy verdict cascade (compute_dogs_verdict_core) reads
--     cpad_unit_dogs_policy + operator_dogs_policy -> writes
--     beach_verdicts (parity-only, doesn't drive consumer surface).
--   * BEP-driven consensus (per-beach LLM extraction -> BEP -> consensus
--     -> beach_dog_policy) is the consumer surface.
--
-- Result: cpad-unit-level evidence never reached the consumer surface.
-- Phase-01 audit on 2026-05-06 found 14 beaches in CPAD units with
-- loaded policy that never got stamped to beach_dog_policy. This
-- migration closes that gap.
--
-- The function follows the standard Catalog-Ingest emit pattern:
-- one BEP row per (gold_fid, field_group, source). Pick the smallest
-- CPAD unit (most specific) when multiple cover one beach. Source name:
-- 'cpad_unit_dogs_policy_v1'. Confidence: 0.70 if dogs_allowed is set,
-- else 0.55 (default_rule only).
--
-- Trigger chain takes over from there: tg_promote_dogs_chain fires
-- compute_beach_field_consensus -> _resolve_dogs_gold ->
-- promote_canonical_dogs_to_beach_dog_policy. Beaches advance phases
-- automatically.

begin;

create or replace function public._emit_evidence_from_cpad_unit_dogs_policy(
  p_fid bigint default null
) returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $$
declare
  ins int := 0; upd int := 0; skp int := 0;
begin
  -- For each (beach, smallest-covering CPAD unit with loaded policy),
  -- synthesize a BEP row. Confidence reflects whether dogs_allowed is
  -- set (highest signal) vs only default_rule.
  with ranked as (
    select
      bg.fid as gold_fid,
      cu.cpad_unit_id,
      c.unit_name,
      cu.url_used,
      cu.dogs_allowed,
      cu.default_rule,
      cu.leash_required,
      cu.area_sand, cu.area_water, cu.area_picnic_area,
      cu.area_parking_lot, cu.area_trails, cu.area_campground,
      cu.designated_dog_zones, cu.prohibited_areas,
      cu.seasonal_rules, cu.time_windows,
      cu.source_quote, cu.ordinance_ref,
      ST_Area(c.geom::geography) as unit_area_m2,
      row_number() over (
        partition by bg.fid
        order by
          (cu.dogs_allowed is not null) desc,
          ST_Area(c.geom::geography) asc
      ) as rk
    from public.beaches_gold bg
    join public.cpad_units c on ST_Intersects(c.geom, bg.geom)
    join public.cpad_unit_dogs_policy cu on cu.cpad_unit_id = c.unit_id
    where bg.is_active
      and (p_fid is null or bg.fid = p_fid)
      and (cu.dogs_allowed is not null or cu.default_rule is not null)
  ),
  picks as (
    select * from ranked where rk = 1
  ),
  payload as (
    select
      gold_fid,
      coalesce(unit_name, 'cpad:' || cpad_unit_id::text) as cpad_unit_name,
      url_used,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',           dogs_allowed,
        'default_rule',      default_rule,
        'leash_required',    case when leash_required is true then 'true'
                                  when leash_required is false then 'false'
                                  else null end,
        'off_leash_exists',  case when (
                                area_sand = 'off_leash' or area_water = 'off_leash'
                                or area_picnic_area = 'off_leash' or area_parking_lot = 'off_leash'
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
       confidence, is_canonical, cpad_unit_name, extraction_type, cpad_role,
       updated_at)
    select
      p.gold_fid, 'dogs', 'cpad_unit_dogs_policy_v1', p.url_used, p.claimed_values,
      p.confidence, false,  -- is_canonical decided by _resolve_dogs_gold
      p.cpad_unit_name, 'cpad_source', 'beach_access',
      now()
      from payload p
      where p.claimed_values <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set source_url = excluded.source_url,
          claimed_values = excluded.claimed_values,
          confidence = excluded.confidence,
          cpad_unit_name = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          cpad_role = excluded.cpad_role,
          updated_at = now()
      where beach_enrichment_provenance.claimed_values
              is distinct from excluded.claimed_values
         or beach_enrichment_provenance.confidence
              is distinct from excluded.confidence
    returning (xmax = 0) as inserted
  )
  select
    count(*) filter (where inserted),
    count(*) filter (where not inserted),
    0
    into ins, upd, skp
    from upserted;

  return query select ins::bigint, upd::bigint, skp::bigint;
end $$;

grant execute on function public._emit_evidence_from_cpad_unit_dogs_policy(bigint)
  to authenticated, service_role;

comment on function public._emit_evidence_from_cpad_unit_dogs_policy(bigint) is
  'Emits BEP rows from cpad_unit_dogs_policy. One row per beach, smallest
   covering CPAD unit with loaded policy wins. Triggers consensus +
   policy promote chain via tg_promote_dogs_chain.';

commit;
