-- 20260514_parking_fee_subtype_structural.sql
--
-- Structural parking fidelity fix. The pipeline already captures
-- parking fee + subtype info at TWO sources (OSM osm_amenities.raw_tags
-- carries `fee` and `parking` keys; LLM extracts parking_type + free-
-- text parking_notes). Both signals were being dropped at the BEP →
-- consensus → beach_amenities funnel because the consumer schema only
-- carried a 5-bucket parking_type enum and prose notes.
--
-- This migration:
--   1. Adds beach_amenities.parking_fee (boolean, nullable: TRUE=paid,
--      FALSE=free, NULL=unknown/mixed) + parking_subtype (text — OSM-
--      style values: surface, street_side, multi-storey, underground,
--      carports, etc.).
--   2. Rewrites _emit_evidence_from_osm_amenities to capture the
--      closest parking POI's fee + subtype tags and emit them into the
--      BEP claimed_values jsonb.
--   3. Rewrites promote_canonical_practical_to_beach_amenities to
--      copy parking_fee + parking_subtype from BEP into beach_amenities.
--
-- Coverage caveat: of ~2.1M osm parking POIs nationwide, only ~170K
-- (8%) carry an explicit `fee` tag. For beaches whose closest POI
-- lacks the tag, parking_fee stays NULL — the UI's parking_notes
-- regex (parses LLM-supplied prose for $/fee/paid/free keywords) is
-- still the secondary signal in that case.
--
-- Backfill happens after apply: rerun the emitter + promoter against
-- all active beaches.

begin;

alter table public.beach_amenities
  add column if not exists parking_fee boolean,
  add column if not exists parking_subtype text;

comment on column public.beach_amenities.parking_fee is
  'TRUE=paid, FALSE=free, NULL=unknown/mixed. Sourced primarily from osm_amenities.raw_tags->>''fee''. NULL when the closest parking POI lacks the tag.';
comment on column public.beach_amenities.parking_subtype is
  'OSM-style parking subtype from the closest parking POI: surface, street_side, multi-storey, underground, carports, etc.';

-- ─── Emitter rewrite ───────────────────────────────────────────────
-- Adds a parking_hits CTE that picks the CLOSEST parking POI per
-- beach and pulls its fee + parking subtype tags. Joined into the
-- combined CTE and emitted as parking_fee + parking_subtype in the
-- claimed_values jsonb.

create or replace function public._emit_evidence_from_osm_amenities(p_fid bigint default null)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with hits_amenities as (
    select bg.fid as gold_fid,
           bool_or(o.amenity_type = 'parking')        as has_parking,
           bool_or(o.amenity_type = 'toilets')        as has_toilets,
           bool_or(o.amenity_type = 'shower')         as has_shower,
           bool_or(o.amenity_type = 'picnic_area')    as has_picnic,
           bool_or(o.amenity_type = 'drinking_water') as has_drinking_water,
           bool_or(o.amenity_type = 'food')           as has_food,
           bool_or(o.amenity_type = 'lifeguard')      as has_lifeguard,
           bool_or(o.amenity_type = 'fire_pit')       as has_fire_pit,
           bool_or(o.raw_tags->>'wheelchair' in ('yes','designated')) as has_wc_positive,
           bool_or(o.raw_tags->>'wheelchair' = 'no')                  as has_wc_negative
      from public.beaches_gold bg
      join public.osm_amenities o on ST_DWithin(bg.geom, o.geom, 0.0015)
     where bg.is_active and (p_fid is null or bg.fid = p_fid)
     group by bg.fid
  ),
  -- Closest parking POI within ~150m. Its tags carry the fee + subtype
  -- signal for THIS beach. distinct on (fid) order by distance picks
  -- the nearest one.
  parking_hits as (
    select distinct on (bg.fid)
           bg.fid as gold_fid,
           o.raw_tags->>'fee'     as fee_tag,
           o.raw_tags->>'parking' as subtype_tag
      from public.beaches_gold bg
      join public.osm_amenities o
        on o.amenity_type = 'parking'
       and ST_DWithin(bg.geom, o.geom, 0.0015)
     where bg.is_active and (p_fid is null or bg.fid = p_fid)
     order by bg.fid, ST_Distance(bg.geom, o.geom)
  ),
  hits_landing as (
    select bg.fid as gold_fid,
           bool_or(o.tags->>'wheelchair' in ('yes','designated','limited'))
             filter (where o.tags->>'wheelchair' in ('yes','designated','limited')) as wc_positive,
           bool_or(o.tags->>'wheelchair' = 'no')
             filter (where o.tags->>'wheelchair' = 'no')                            as wc_negative
      from public.beaches_gold bg
      join public.osm_landing o
        on o.is_active and o.geom_full is not null and (o.tags ? 'wheelchair')
       and ST_DWithin(bg.geom::geography, o.geom_full::geography, 200)
     where bg.is_active and (p_fid is null or bg.fid = p_fid)
     group by bg.fid
  ),
  combined as (
    select coalesce(a.gold_fid, l.gold_fid, ph.gold_fid)        as gold_fid,
           a.has_parking, a.has_toilets, a.has_shower, a.has_picnic,
           a.has_drinking_water, a.has_food, a.has_lifeguard, a.has_fire_pit,
           coalesce(a.has_wc_positive, false) or coalesce(l.wc_positive, false) as wc_positive,
           coalesce(a.has_wc_negative, false) or coalesce(l.wc_negative, false) as wc_negative,
           ph.fee_tag, ph.subtype_tag
      from hits_amenities a
      full outer join hits_landing  l  on a.gold_fid = l.gold_fid
      full outer join parking_hits  ph on coalesce(a.gold_fid, l.gold_fid) = ph.gold_fid
  ),
  payload as (
    select gold_fid,
      jsonb_strip_nulls(jsonb_build_object(
        'has_parking',        case when has_parking        then 'true' else null end,
        'has_restrooms',      case when has_toilets        then 'true' else null end,
        'has_showers',        case when has_shower         then 'true' else null end,
        'has_picnic_area',    case when has_picnic         then 'true' else null end,
        'has_drinking_water', case when has_drinking_water then 'true' else null end,
        'has_food',           case when has_food           then 'true' else null end,
        'has_lifeguards',     case when has_lifeguard      then 'true' else null end,
        'has_fire_pits',      case when has_fire_pit       then 'true' else null end,
        'has_disabled_access',
          case when wc_positive then 'true'
               when wc_negative then 'false'
               else null end,
        -- NEW 2026-05-14: parking fee + subtype from the closest POI
        'parking_fee',
          case when fee_tag = 'yes' then 'true'
               when fee_tag = 'no'  then 'false'
               else null end,
        'parking_subtype', subtype_tag
      )) as cv
    from combined
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'practical', 'osm_amenities_v1',
           'https://www.openstreetmap.org/', cv, 0.65,
           false, 'derived_url_crawl', 'beach_access', now()
    from payload where cv <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values, updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end $function$;

-- ─── Promoter rewrite ──────────────────────────────────────────────
-- Adds parking_fee + parking_subtype to the upsert, pulling from the
-- canonical BEP row's claimed_values. coalesce on UPDATE so the new
-- fields don't get wiped when a less-rich evidence row wins.

create or replace function public.promote_canonical_practical_to_beach_amenities(
  p_fid bigint default null,
  p_min_confidence numeric default 0.5
)
returns table(rows_inserted bigint, rows_updated bigint, gold_open_close_set bigint)
language plpgsql
as $function$
declare ins int := 0; upd int := 0; gold_set int := 0;
begin
  with consensus_practical as (
    select gold_fid,
      max(winning_value) filter (where field_name = 'has_lifeguards')      as v_lifeguards,
      max(winning_value) filter (where field_name = 'has_restrooms')       as v_restrooms,
      max(winning_value) filter (where field_name = 'has_parking')         as v_parking,
      max(winning_value) filter (where field_name = 'has_showers')         as v_showers,
      max(winning_value) filter (where field_name = 'has_disabled_access') as v_disabled,
      max(winning_value) filter (where field_name = 'has_food')            as v_food,
      max(winning_value) filter (where field_name = 'has_fire_pits')       as v_fire,
      max(winning_value) filter (where field_name = 'has_picnic_area')     as v_picnic,
      max(winning_value) filter (where field_name = 'has_drinking_water')  as v_water,
      max(confidence) as max_confidence,
      bool_or(disagreement) as any_disagreement
      from public.beach_field_consensus
     where field_group = 'practical' and (p_fid is null or gold_fid = p_fid)
     group by gold_fid
  ),
  canon_payload as (
    select e.gold_fid as fid, e.claimed_values
      from public.beach_enrichment_provenance e
      join public.beaches_gold g on g.fid = e.gold_fid
     where e.field_group = 'practical' and e.is_canonical = true
       and (p_fid is null or e.gold_fid = p_fid)
  ),
  protected as (
    select arena_group_id from public.beach_amenities where source = 'manual_curator'
  ),
  upsert_amen as (
    insert into public.beach_amenities
      (arena_group_id, has_restrooms, has_lifeguards, has_showers,
       has_drinking_water, has_disabled_access, has_food, has_fire_pits,
       has_picnic_area, parking_type, parking_fee, parking_subtype,
       hours_text, source, curated_at, notes,
       consensus_confidence, disagreement_flag)
    select c.gold_fid,
      case lower(c.v_restrooms)  when 'true' then true when 'false' then false end,
      case lower(c.v_lifeguards) when 'true' then true when 'false' then false end,
      case lower(c.v_showers)    when 'true' then true when 'false' then false end,
      case lower(c.v_water)      when 'true' then true when 'false' then false end,
      case lower(c.v_disabled)   when 'true' then true when 'false' then false end,
      case lower(c.v_food)       when 'true' then true when 'false' then false end,
      case lower(c.v_fire)       when 'true' then true when 'false' then false end,
      case lower(c.v_picnic)     when 'true' then true when 'false' then false end,
      p.claimed_values->>'parking_type',
      -- NEW 2026-05-14: parking_fee + parking_subtype from BEP
      case lower(p.claimed_values->>'parking_fee')
        when 'true'  then true
        when 'false' then false
        else null end,
      p.claimed_values->>'parking_subtype',
      coalesce(p.claimed_values->>'hours_text',
        case when (p.claimed_values->>'hours_open') is not null
              or (p.claimed_values->>'hours_close') is not null
        then trim(both '-' from format('%s-%s',
                  coalesce(p.claimed_values->>'hours_open',''),
                  coalesce(p.claimed_values->>'hours_close','')))
        end),
      'auto_promoted_from_consensus', now(),
      format('Phase B consensus: max_confidence=%s%s', coalesce(c.max_confidence, 0),
             case when c.any_disagreement then ' DISAGREEMENT' else '' end),
      c.max_confidence, c.any_disagreement
      from consensus_practical c
      left join canon_payload p on p.fid = c.gold_fid
     where c.gold_fid not in (select arena_group_id from protected)
    on conflict (arena_group_id) do update
      set has_restrooms=excluded.has_restrooms,
          has_lifeguards=excluded.has_lifeguards,
          has_showers=excluded.has_showers,
          has_drinking_water=excluded.has_drinking_water,
          has_disabled_access=excluded.has_disabled_access,
          has_food=excluded.has_food,
          has_fire_pits=excluded.has_fire_pits,
          has_picnic_area=excluded.has_picnic_area,
          parking_type=coalesce(excluded.parking_type, beach_amenities.parking_type),
          parking_fee=coalesce(excluded.parking_fee, beach_amenities.parking_fee),
          parking_subtype=coalesce(excluded.parking_subtype, beach_amenities.parking_subtype),
          hours_text=coalesce(excluded.hours_text, beach_amenities.hours_text),
          source=excluded.source, curated_at=now(), notes=excluded.notes,
          consensus_confidence=excluded.consensus_confidence,
          disagreement_flag=excluded.disagreement_flag
      where beach_amenities.source <> 'manual_curator'
    returning (xmax = 0) as inserted, arena_group_id
  ),
  parsed_hours as (
    select c.gold_fid as fid,
           public._norm_time_text(p.claimed_values->>'hours_open')  as t_open,
           public._norm_time_text(p.claimed_values->>'hours_close') as t_close
      from consensus_practical c
      join canon_payload p on p.fid = c.gold_fid
  ),
  upd_gold as (
    update public.beaches_gold g
       set open_time  = coalesce(g.open_time,  ph.t_open),
           close_time = coalesce(g.close_time, ph.t_close)
      from parsed_hours ph
     where g.fid = ph.fid
       and ((g.open_time is null and ph.t_open is not null)
         or (g.close_time is null and ph.t_close is not null))
    returning g.fid
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted),
         (select count(*) from upd_gold)
    into ins, upd, gold_set from upsert_amen;
  return query select ins::bigint, upd::bigint, gold_set::bigint;
end;
$function$;

commit;
