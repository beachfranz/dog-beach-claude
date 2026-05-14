-- 20260514_parking_options_multi.sql
--
-- Multi-option parking. Schema collapses lot+street+metered into one
-- triple today; LLM notes routinely describe MULTIPLE options
-- (e.g. Long Beach City Beach: "Free street parking available, or
-- metered parking lot at beach level off Junipero Avenue"). This
-- migration adds a jsonb array column + emitter logic to parse out
-- distinct options.
--
-- Schema:
--   beach_amenities.parking_options jsonb — array of:
--     [{type: 'lot'|'street'|'garage'|'metered', subtype: 'surface'|
--       'street_side'|..., fee: bool|null, source: text}, ...]
--   Single legacy fields (parking_type, parking_fee, parking_subtype)
--   remain populated with the FIRST option for backwards compat.
--
-- LLM emitter: parse parking_notes with two passes:
--   1. Look for {street, lot, garage, metered} keyword anchors.
--   2. For each anchor found, infer fee from the same notes string
--      (free / fee / paid / $ / metered).
-- Same-source de-dup so we don't emit two lot options.

begin;

alter table public.beach_amenities
  add column if not exists parking_options jsonb;

comment on column public.beach_amenities.parking_options is
  'Array of distinct parking options at this beach. Each element: '
  '{type, subtype, fee, source}. Single-option beaches store one element.';

-- ─── LLM parking emitter (rewritten) ──────────────────────────────
-- Emits parking_options array + keeps single-field legacy values.
create or replace function public._emit_evidence_from_park_url_parking(p_fid bigint default null)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with per_beach as (
    select distinct on (arena_group_id)
           arena_group_id as gold_fid,
           parking_type,
           parking_notes,
           source_url
      from public.park_url_extractions
     where arena_group_id is not null
       and (parking_type is not null or parking_notes is not null)
       and (p_fid is null or arena_group_id = p_fid)
     order by arena_group_id,
              (parking_type is not null) desc,
              scraped_at desc nulls last
  ),
  parsed as (
    select gold_fid, source_url, parking_type, parking_notes,
      -- Each option is non-null iff its anchor keyword fires in notes
      -- OR parking_type itself indicates the option.
      -- Street option fires on the word "street" near "parking".
      case when parking_notes ~* '\mstreet[- ]?(side|parking)?\M'
             or parking_type = 'street' then
        jsonb_build_object(
          'type',    'street',
          'subtype', 'street_side',
          'fee',
            case when parking_notes ~* '\mfree[ \w]{0,30}street\M'
                  or parking_notes ~* '\mstreet[ \w]{0,30}free\M'  then false
                 when parking_notes ~* '\mmetered\M.*\mstreet\M'
                  or parking_notes ~* '\mstreet\M.*\mmetered\M'
                  or parking_notes ~* '\mpaid[ \w]{0,30}street\M'
                  or parking_notes ~* '\mstreet[ \w]{0,30}paid\M' then true
                 else null end,
          'source', 'park_url_llm'
        )
      end as street_opt,
      -- Lot option fires on "lot" keyword OR parking_type lot/mixed.
      case when parking_notes ~* '\m(parking )?lot\M'
             or parking_type in ('lot', 'mixed') then
        jsonb_build_object(
          'type',    'lot',
          'subtype', 'surface',
          'fee',
            case when parking_notes ~* '\mfree[ \w]{0,30}lot\M'
                  or parking_notes ~* '\mlot[ \w]{0,30}free\M'    then false
                 when parking_notes ~* '\mmetered[ \w]{0,30}lot\M'
                  or parking_notes ~* '\mlot[ \w]{0,30}metered\M'
                  or parking_notes ~* '\$|\mfee\M|\mpaid\M'       then true
                 else null end,
          'source', 'park_url_llm'
        )
      end as lot_opt,
      -- Garage option (multi-storey / underground).
      case when parking_notes ~* '\m(garage|multi.?stor|underground)\M' then
        jsonb_build_object(
          'type',    'garage',
          'subtype', 'multi-storey',
          'fee',
            case when parking_notes ~* '\$|\mfee\M|\mpaid\M' then true
                 when parking_notes ~* '\mfree\M' then false
                 else null end,
          'source', 'park_url_llm'
        )
      end as garage_opt
    from per_beach
  ),
  options_array as (
    select gold_fid, source_url, parking_type, parking_notes,
      jsonb_strip_nulls(jsonb_build_array(street_opt, lot_opt, garage_opt))
        as opts_raw
    from parsed
  ),
  filtered as (
    -- Drop nulls from the array (jsonb_strip_nulls only handles object keys)
    select gold_fid, source_url, parking_type, parking_notes,
      (select coalesce(jsonb_agg(o), '[]'::jsonb)
         from jsonb_array_elements(opts_raw) o
        where o is not null and o::text <> 'null') as opts
    from options_array
  ),
  payload as (
    select gold_fid, source_url,
      jsonb_strip_nulls(jsonb_build_object(
        'parking_type',  parking_type,
        'parking_notes', parking_notes,
        'parking_fee',
          case when parking_notes ~* '(\$|\mfee\M|\mpaid\M)' then 'true'
               when parking_notes ~* '\mfree\M'             then 'false'
               else null end,
        'parking_subtype',
          case parking_type
            when 'lot'     then 'surface'
            when 'street'  then 'street_side'
            when 'metered' then 'street_side'
            else null
          end,
        'parking_options', case when jsonb_array_length(opts) > 0 then opts else null end
      )) as cv
    from filtered
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'practical', 'park_url_parking_v1', source_url, cv,
           0.70, false, 'derived_url_crawl', 'beach_access', now()
    from payload where cv <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          source_url = excluded.source_url,
          updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end $function$;

-- ─── Promoter — copy parking_options through ──────────────────────
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
  supplemental as (
    select gold_fid as fid, jsonb_object_agg(k, v) as extra
      from (
        select distinct on (e.gold_fid, ek.key)
               e.gold_fid, ek.key as k, e.claimed_values -> ek.key as v
          from public.beach_enrichment_provenance e
          cross join lateral jsonb_object_keys(e.claimed_values) ek(key)
         where e.field_group = 'practical'
           and not e.is_canonical
           and e.claimed_values -> ek.key is not null
           and (p_fid is null or e.gold_fid = p_fid)
         order by e.gold_fid, ek.key, e.confidence desc nulls last
      ) t
     group by gold_fid
  ),
  merged as (
    select c.fid,
           coalesce(s.extra, '{}'::jsonb) || c.claimed_values as claimed_values
      from canon_payload c
      left join supplemental s on s.fid = c.fid
  ),
  protected as (
    select arena_group_id from public.beach_amenities where source = 'manual_curator'
  ),
  upsert_amen as (
    insert into public.beach_amenities
      (arena_group_id, has_parking, has_restrooms, has_lifeguards, has_showers,
       has_drinking_water, has_disabled_access, has_food, has_fire_pits,
       has_picnic_area, parking_type, parking_fee, parking_subtype, parking_options,
       hours_text, source, curated_at, notes,
       consensus_confidence, disagreement_flag)
    select c.gold_fid,
      case lower(c.v_parking)    when 'true' then true when 'false' then false end,
      case lower(c.v_restrooms)  when 'true' then true when 'false' then false end,
      case lower(c.v_lifeguards) when 'true' then true when 'false' then false end,
      case lower(c.v_showers)    when 'true' then true when 'false' then false end,
      case lower(c.v_water)      when 'true' then true when 'false' then false end,
      case lower(c.v_disabled)   when 'true' then true when 'false' then false end,
      case lower(c.v_food)       when 'true' then true when 'false' then false end,
      case lower(c.v_fire)       when 'true' then true when 'false' then false end,
      case lower(c.v_picnic)     when 'true' then true when 'false' then false end,
      p.claimed_values->>'parking_type',
      case lower(p.claimed_values->>'parking_fee')
        when 'true'  then true
        when 'false' then false
        else null end,
      p.claimed_values->>'parking_subtype',
      case when jsonb_typeof(p.claimed_values->'parking_options') = 'array'
           then p.claimed_values->'parking_options' end,
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
      left join merged p on p.fid = c.gold_fid
     where c.gold_fid not in (select arena_group_id from protected)
    on conflict (arena_group_id) do update
      set has_parking=excluded.has_parking,
          has_restrooms=excluded.has_restrooms,
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
          parking_options=coalesce(excluded.parking_options, beach_amenities.parking_options),
          hours_text=coalesce(excluded.hours_text, beach_amenities.hours_text),
          source=excluded.source, curated_at=now(), notes=excluded.notes,
          consensus_confidence=excluded.consensus_confidence,
          disagreement_flag=excluded.disagreement_flag
      where beach_amenities.source <> 'manual_curator'
    returning (xmax = 0) as inserted, arena_group_id
  ),
  parsed_hours as (
    select c.gold_fid as fid,
           public._norm_time_text(m.claimed_values->>'hours_open')  as t_open,
           public._norm_time_text(m.claimed_values->>'hours_close') as t_close
      from consensus_practical c
      join merged m on m.fid = c.gold_fid
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
