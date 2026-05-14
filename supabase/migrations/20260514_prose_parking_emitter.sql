-- 20260514_prose_parking_emitter.sql
--
-- Mine parking signal trapped in narrative/prose fields of
-- beach_policy_extractions. The structured parking_street question
-- often returns 'unclear' when the same LLM mentions street parking
-- incidentally in other fields' parsed_value text (cross_streets,
-- dogs_allowed_areas markdown blob, etc.).
--
-- HBDB case: parking_street structured = 'unclear' x4, but the
-- raw_address.cross_streets field carries "parking on Pacific Coast
-- Highway between these stoplights" — a clear street-parking mention.
--
-- This emitter regex-scans ALL parsed_value text per beach for:
--   STREET anchors: "street parking", "on-street", "curbside",
--                   "parking on <NamedStreet/Highway/Avenue/...>"
--   LOT anchors:    "parking lot(s)", "parking garage", "parking structure"
--   FEE qualifiers within the same parsed_value:
--     paid: $\d, fee, metered, paid, pay-station
--     free: free parking, no charge, complimentary
--
-- Confidence 0.50 — prose-mined signal is weaker than structured
-- Q&A. The beach_policy_v2 emitter at 0.75 wins when both fire.
-- This emitter fills the GAP where structured = unclear but prose
-- has it.

begin;

create or replace function public._emit_evidence_from_beach_policy_prose_parking(p_fid bigint default null)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with all_prose as (
    -- Concatenate every text field per beach so a single regex pass
    -- covers all places where parking might be mentioned.
    select arena_group_id as gold_fid,
           string_agg(coalesce(parsed_value, '') || ' ' || coalesce(raw_response, ''),
                      ' || ') as prose
      from public.beach_policy_extractions
     where arena_group_id is not null
       and parse_succeeded = true
       and (p_fid is null or arena_group_id = p_fid)
     group by arena_group_id
  ),
  signals as (
    select gold_fid, prose,
      -- STREET parking signal — multiple anchor patterns.
      (prose ~* '\m(street parking|on[- ]street|curbside|on-street|metered street|street side)\M'
       or prose ~* 'parking on (PCH|Pacific Coast Highway|[A-Z][a-z]+( [A-Z][a-z]+)* (Street|Avenue|Boulevard|Drive|Highway|Road|Way|Court))'
      ) as has_street,
      -- LOT parking signal — already covered by structured extractor
      -- but include for completeness when prose contradicts structured.
      (prose ~* '\m(parking lot|parking garage|parking structure)\M'
      ) as has_lot,
      -- FEE qualifiers (any in the whole prose blob)
      (prose ~* '\$\d|\mfee\M|\mmetered\M|\mpaid\M|pay[- ]station|pay[- ]to[- ]park') as has_paid,
      (prose ~* '\mfree (parking|lot)|no charge|complimentary parking|free street') as has_free
    from all_prose
  ),
  structured as (
    -- Skip beaches where the structured parking_v2 question already
    -- got a non-unclear answer for both lot AND street — no need to
    -- prose-mine when structured already won.
    select arena_group_id as gold_fid,
      bool_or(field_name = 'parking_street' and lower(parsed_value) in ('paid','free','none')) as street_decided,
      bool_or(field_name = 'parking_lot'    and lower(parsed_value) in ('paid','free','none')) as lot_decided
    from public.beach_policy_extractions
    where parse_succeeded = true
      and field_name in ('parking_street','parking_lot')
      and (p_fid is null or arena_group_id = p_fid)
    group by arena_group_id
  ),
  decide as (
    select s.gold_fid, s.prose,
      s.has_street and coalesce(not st.street_decided, true) as emit_street,
      s.has_lot    and coalesce(not st.lot_decided,    true) as emit_lot,
      s.has_paid, s.has_free
    from signals s
    left join structured st on st.gold_fid = s.gold_fid
  ),
  payload as (
    select gold_fid,
      (select coalesce(jsonb_agg(o), '[]'::jsonb) from (
        select * from (values
          (case when emit_street then
            jsonb_build_object(
              'type','street','subtype','street_side',
              'fee', case when has_paid and not has_free then true
                          when has_free and not has_paid then false
                          else null end,
              'source','beach_policy_prose',
              'confidence', 0.50
            ) end),
          (case when emit_lot then
            jsonb_build_object(
              'type','lot','subtype','surface',
              'fee', case when has_paid and not has_free then true
                          when has_free and not has_paid then false
                          else null end,
              'source','beach_policy_prose',
              'confidence', 0.50
            ) end)
        ) v(o) where o is not null
      ) z) as opts
    from decide
  ),
  cv as (
    select gold_fid,
      jsonb_strip_nulls(jsonb_build_object(
        'parking_options', case when jsonb_array_length(opts) > 0 then opts else null end
      )) as claimed_values
    from payload
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'practical', 'beach_policy_prose_parking_v1', null, claimed_values,
           0.50, false, 'derived_url_crawl', 'beach_access', now()
    from cv where claimed_values <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values, updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end $function$;

commit;
