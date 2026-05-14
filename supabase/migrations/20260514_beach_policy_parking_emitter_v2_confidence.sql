-- 20260514_beach_policy_parking_emitter_v2_confidence.sql
--
-- Improve the v2 parking emitter to derive per-option confidence from
-- multi-model agreement instead of flat 0.75. The previous emitter
-- (shipped earlier today) collapsed 3-of-3 model agreement into a
-- single static confidence — losing the agreement signal at the BEP
-- boundary, which the consensus pipeline can never recover.
--
-- New per-option confidence:
--   3+ winning votes, 0 dissent  → 0.90
--   2 winning votes, 0 dissent   → 0.75
--   majority with dissent        → 0.55 + disagreement signal
--   single vote                  → 0.40
--
-- The BEP row's overall `confidence` = max(option confidences) so
-- consensus doesn't drop a high-trust option when its sibling is
-- low-trust. Each option carries `confidence`, `n_votes`, and
-- `agreement_ratio` in its jsonb so the consumer (admin UI, future
-- vote-aware consensus) can see the underlying agreement.

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
    select gold_fid, field_name, n_total, n_paid, n_free, n_none, n_unk,
      case
        when n_paid > n_free and n_paid > n_none then 'paid'
        when n_free > n_paid and n_free > n_none then 'free'
        when n_none >= greatest(n_paid, n_free, 1) and n_none > n_unk then 'none'
        else null
      end as decision,
      -- Winning vote count (only meaningful when decision is set)
      greatest(n_paid, n_free, n_none) as n_winning
    from tallied
  ),
  scored as (
    select gold_fid, field_name, decision, n_total, n_winning,
      -- Dissent = other value (paid/free/none, not unclear) got votes
      ((n_paid + n_free + n_none) - n_winning) as n_dissent,
      case
        when decision is null then null
        when n_winning >= 3 and (n_paid + n_free + n_none) = n_winning then 0.90
        when n_winning >= 2 and (n_paid + n_free + n_none) = n_winning then 0.75
        when n_winning >  ((n_paid + n_free + n_none) - n_winning)     then 0.55
        else                                                                0.40
      end as confidence
    from decided
  ),
  per_fid as (
    select gold_fid,
      max(case when field_name='parking_lot'    then decision   end) as lot_dec,
      max(case when field_name='parking_lot'    then confidence end) as lot_conf,
      max(case when field_name='parking_lot'    then n_total    end) as lot_total,
      max(case when field_name='parking_lot'    then n_winning  end) as lot_win,
      max(case when field_name='parking_lot'    then n_dissent  end) as lot_dis,
      max(case when field_name='parking_street' then decision   end) as st_dec,
      max(case when field_name='parking_street' then confidence end) as st_conf,
      max(case when field_name='parking_street' then n_total    end) as st_total,
      max(case when field_name='parking_street' then n_winning  end) as st_win,
      max(case when field_name='parking_street' then n_dissent  end) as st_dis
      from scored
     group by gold_fid
  ),
  payload as (
    select gold_fid,
      (select coalesce(jsonb_agg(o), '[]'::jsonb) from (
        select * from (values
          (case when lot_dec in ('paid','free') then
            jsonb_build_object(
              'type', 'lot', 'subtype', 'surface',
              'fee', case lot_dec when 'paid' then true else false end,
              'source', 'beach_policy_v2',
              'confidence',      lot_conf,
              'n_votes',         lot_total,
              'agreement_ratio', round((lot_win::numeric / nullif(lot_total,0))::numeric, 2),
              'has_dissent',     lot_dis > 0
            ) end),
          (case when st_dec in ('paid','free') then
            jsonb_build_object(
              'type', 'street', 'subtype', 'street_side',
              'fee', case st_dec when 'paid' then true else false end,
              'source', 'beach_policy_v2',
              'confidence',      st_conf,
              'n_votes',         st_total,
              'agreement_ratio', round((st_win::numeric / nullif(st_total,0))::numeric, 2),
              'has_dissent',     st_dis > 0
            ) end)
        ) v(o) where o is not null
      ) z) as opts,
      -- BEP-row-level confidence = max of option confidences so a
      -- high-trust option isn't dragged down by a low-trust sibling.
      greatest(coalesce(lot_conf, 0), coalesce(st_conf, 0)) as row_conf,
      -- Disagreement flag = any option had dissent
      (coalesce(lot_dis,0) + coalesce(st_dis,0)) > 0 as row_dissent
    from per_fid
  ),
  cv as (
    select gold_fid, row_conf, row_dissent,
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
           row_conf, false, 'derived_url_crawl', 'beach_access', now()
    from cv where claimed_values <> '{}'::jsonb and row_conf > 0
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          confidence = excluded.confidence,
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
