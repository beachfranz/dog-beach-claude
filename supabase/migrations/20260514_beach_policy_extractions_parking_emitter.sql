-- 20260514_beach_policy_extractions_parking_emitter.sql
--
-- Lift parking_lot + parking_street rows from beach_policy_extractions
-- (populated by extract_research_v2.py) into BEP parking_options.
--
-- Extract_research_v2 already asks the right structured questions:
--   parking_lot:    "free/paid/none/unclear"
--   parking_street: "free/paid/none/unclear"
-- and stores one row per (fid, source, model, field). HBDB had 4 model
-- votes for parking_lot (3× paid, 1× ?) but no emitter promoted these
-- into BEP, so the single tile we render today came only from OSM.
--
-- This emitter aggregates votes per (fid, field) using majority rule
-- across models:
--   majority 'paid'    → {type: 'lot'|'street', fee: true}
--   majority 'free'    → {type: 'lot'|'street', fee: false}
--   majority 'none'    → option dropped (no parking of that kind)
--   else (all unclear) → option dropped
--
-- Confidence 0.75 — multi-model consensus is higher-trust than the
-- single-model park_url_parking emitter (0.70).

begin;

create or replace function public._emit_evidence_from_beach_policy_parking(p_fid bigint default null)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with raw_votes as (
    select arena_group_id as gold_fid, field_name,
           lower(parsed_value) as v
      from public.beach_policy_extractions
     where field_name in ('parking_lot','parking_street')
       and parse_succeeded = true
       and (p_fid is null or arena_group_id = p_fid)
  ),
  tallied as (
    select gold_fid, field_name,
           count(*) filter (where v = 'paid')                          as n_paid,
           count(*) filter (where v = 'free')                          as n_free,
           count(*) filter (where v = 'none')                          as n_none,
           count(*) filter (where v in ('unclear','no_match','unknown')) as n_unk,
           count(*)                                                    as n_total
      from raw_votes
     group by 1, 2
  ),
  decided as (
    select gold_fid, field_name,
      case
        when n_paid > n_free and n_paid > n_none then 'paid'
        when n_free > n_paid and n_free > n_none then 'free'
        when n_none >= greatest(n_paid, n_free, 1) and n_none > n_unk then 'none'
        else null
      end as decision,
      n_total
    from tallied
  ),
  per_fid as (
    select gold_fid,
      max(case when field_name = 'parking_lot'    then decision end) as lot_dec,
      max(case when field_name = 'parking_street' then decision end) as street_dec
      from decided
     group by gold_fid
  ),
  payload as (
    select gold_fid,
      (select coalesce(jsonb_agg(o), '[]'::jsonb) from (
        select * from (values
          (case when lot_dec in ('paid','free') then
            jsonb_build_object(
              'type', 'lot', 'subtype', 'surface',
              'fee', case lot_dec when 'paid' then true when 'free' then false end,
              'source', 'beach_policy_v2'
            ) end),
          (case when street_dec in ('paid','free') then
            jsonb_build_object(
              'type', 'street', 'subtype', 'street_side',
              'fee', case street_dec when 'paid' then true when 'free' then false end,
              'source', 'beach_policy_v2'
            ) end)
        ) v(o) where o is not null
      ) z) as opts
    from per_fid
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
    select gold_fid, 'practical', 'beach_policy_parking_v1', null, claimed_values,
           0.75, false, 'derived_url_crawl', 'beach_access', now()
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
