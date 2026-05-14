-- 20260514_promoter_override_default_hours.sql
--
-- promote_canonical_practical_to_beach_amenities only updated
-- beaches_gold.open_time/close_time when the EXISTING value was NULL.
-- But 1,868 of 1,873 scoreable beaches have the universal seed default
-- 05:00/22:00 pre-filled — so the LLM-extracted real hours (08:00
-- start, 18:00 close at Teachers Beach, etc.) never overwrote.
--
-- Fix: treat the (05:00, 22:00) pair as if it were null when deciding
-- whether to override. Any non-default pair (manually set or LLM
-- previously written) wins as before.

begin;

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
    -- 2026-05-14: treat the universal seed (05:00, 22:00) pair as if
    -- it were NULL so LLM-extracted real hours overwrite it.
    update public.beaches_gold g
       set open_time  = coalesce(ph.t_open,  g.open_time),
           close_time = coalesce(ph.t_close, g.close_time)
      from parsed_hours ph
     where g.fid = ph.fid
       and (ph.t_open is not null or ph.t_close is not null)
       and (
         g.open_time is null or g.close_time is null
         or (g.open_time = '05:00' and g.close_time = '22:00')
       )
    returning g.fid
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted),
         (select count(*) from upd_gold)
    into ins, upd, gold_set from upsert_amen;
  return query select ins::bigint, upd::bigint, gold_set::bigint;
end;
$function$;

commit;
