-- 20260524_consensus_canonical_boost.sql
--
-- Patch compute_beach_field_consensus to respect is_canonical=true on
-- BEP rows: ×3 weight multiplier for canonical rows.
--
-- WHY
-- ===
-- The _resolve_<field_group>(p_fid) functions pick the "winning source"
-- per (fid, field_group) and flag the chosen BEP row with
-- is_canonical=true. That signal was being IGNORED by
-- compute_beach_field_consensus, which weighted every BEP row equally
-- via _calibration_weight (×0.5 default). Two non-canonical operator
-- rows could outvote the canonical winner. 312/3567 (8.7%) of beaches
-- had consensus flip canonical for the `allowed` field; 17 went yes→no
-- (Huntington Cliffs being the surfaced symptom) and 12 went no→yes.
--
-- WHAT THIS DOES
-- ==============
-- 1. claims_direct + claims_derived now carry e.is_canonical through.
-- 2. weighted's vote_weight multiplies by 3.0 when is_canonical=true,
--    otherwise ×1.0. is_canonical is also surfaced into top_member's
--    JSON payload for downstream debugging.
--
-- ROLLOUT
-- =======
-- Function-body change → safe REPLACE. After applying, run:
--   SELECT public.compute_beach_field_consensus();      -- recompute all
--   SELECT public.promote_canonical_dogs_to_beach_dog_policy();
-- to propagate the new consensus into beach_dog_policy.

BEGIN;

CREATE OR REPLACE FUNCTION public.compute_beach_field_consensus(p_fid bigint DEFAULT NULL::bigint)
 RETURNS TABLE(rows_inserted bigint, rows_updated bigint, beaches_touched bigint)
 LANGUAGE plpgsql
 SET statement_timeout TO '60s'
AS $function$
declare
  ins_count bigint := 0; upd_count bigint := 0; beaches_count bigint := 0;
begin
  with claims_direct as (
    -- Existing path: pull claim_key directly from claimed_values
    select e.gold_fid, c.field_group, c.claim_key, c.field_kind, e.source,
           e.is_canonical,
           public._source_family(e.source) as family,
           coalesce(e.confidence, 0.5) as source_confidence,
           e.claimed_values->>c.claim_key as raw_value,
           public._consensus_normalize(c.field_kind, e.claimed_values->>c.claim_key) as norm_value
      from public.beach_enrichment_provenance e
      join public.beaches_gold g on g.fid = e.gold_fid
      join public.consensus_field_config c on c.field_group = e.field_group
     where e.gold_fid is not null
       and (p_fid is null or e.gold_fid = p_fid)
       and e.claimed_values ? c.claim_key
  ),
  claims_derived as (
    -- New path: derive has_on_leash / has_off_leash from each dogs source's
    -- existing claim shape. Each source emits up to two votes (one per
    -- binary). Abstains (NULL derivation) are filtered out. If the source
    -- has the literal claim_key already, prefer that — the lateral skips
    -- to avoid double-counting.
    select e.gold_fid, 'dogs'::text as field_group,
           d.claim_key, d.claim_key as field_kind,
           e.source,
           e.is_canonical,
           public._source_family(e.source) as family,
           coalesce(e.confidence, 0.5) as source_confidence,
           d.derived_value as raw_value,
           public._consensus_normalize(d.claim_key, d.derived_value) as norm_value
      from public.beach_enrichment_provenance e
      join public.beaches_gold g on g.fid = e.gold_fid
      cross join lateral (values
        ('has_on_leash',  public._derive_has_on_leash(e.claimed_values)),
        ('has_off_leash', public._derive_has_off_leash(e.claimed_values))
      ) as d(claim_key, derived_value)
     where e.field_group = 'dogs'
       and e.gold_fid is not null
       and (p_fid is null or e.gold_fid = p_fid)
       and d.derived_value is not null
       and not (e.claimed_values ? d.claim_key)
  ),
  claims as (
    select * from claims_direct
    union all
    select * from claims_derived
  ),
  weighted as (
    -- ★ Canonical boost (2026-05-24): is_canonical=true rows carry 3×
    --   the weight. Honors the BEP resolver's pick; without this,
    --   non-canonical operator misattributions can outvote canonical
    --   evidence. See migration header comment.
    select c.*,
           c.source_confidence
           * public._calibration_weight(c.field_kind, c.source)
           * case when c.is_canonical then 3.0::numeric else 1.0::numeric end
             as vote_weight
      from claims c
     where c.norm_value is not null
  ),
  family_votes as (
    select gold_fid, field_group, claim_key, field_kind, norm_value, family,
           max(vote_weight) as family_weight,
           (array_agg(jsonb_build_object(
             'source', source, 'weight', round(vote_weight::numeric, 3),
             'src_conf', round(source_confidence::numeric, 3),
             'canonical', is_canonical
           ) order by vote_weight desc))[1] as top_member,
           count(*) as n_in_family
      from weighted
     group by gold_fid, field_group, claim_key, field_kind, norm_value, family
  ),
  votes as (
    select gold_fid, field_group, claim_key, field_kind, norm_value,
           sum(family_weight) as bucket_weight,
           jsonb_agg(jsonb_build_object(
             'family', family, 'weight', round(family_weight::numeric, 3),
             'top_member', top_member, 'n_in_family', n_in_family
           ) order by family_weight desc) as voters
      from family_votes
     group by gold_fid, field_group, claim_key, field_kind, norm_value
  ),
  ranked as (
    select v.*,
           sum(bucket_weight) over (partition by gold_fid, claim_key) as total_weight,
           row_number() over (partition by gold_fid, claim_key order by bucket_weight desc) as rnk
      from votes v
  ),
  winners as (
    select gold_fid, field_group, claim_key, field_kind,
           norm_value as winning_value, bucket_weight as winner_weight, total_weight,
           (select bucket_weight from ranked r2 where r2.gold_fid=ranked.gold_fid
              and r2.claim_key=ranked.claim_key and r2.rnk=2 limit 1) as runner_up_weight,
           (select jsonb_object_agg(r3.norm_value,
              jsonb_build_object('weight', round(r3.bucket_weight::numeric,3), 'voters', r3.voters))
              from ranked r3 where r3.gold_fid=ranked.gold_fid
                            and r3.claim_key=ranked.claim_key) as breakdown,
           (select count(*)::int from ranked r4 where r4.gold_fid=ranked.gold_fid
              and r4.claim_key=ranked.claim_key) as bucket_count
      from ranked where rnk = 1
  ),
  upsert as (
    insert into public.beach_field_consensus
      (gold_fid, field_name, field_group, field_kind, winning_value,
       confidence, disagreement, vote_breakdown, source_count, computed_at)
    select w.gold_fid, w.claim_key, w.field_group, w.field_kind, w.winning_value,
           round((w.winner_weight / nullif(w.total_weight, 0))::numeric, 3),
           public._consensus_disagreement(w.total_weight, w.winner_weight, w.runner_up_weight),
           w.breakdown, w.bucket_count, now()
      from winners w
    on conflict (gold_fid, field_name) do update
      set winning_value=excluded.winning_value, confidence=excluded.confidence,
          disagreement=excluded.disagreement, vote_breakdown=excluded.vote_breakdown,
          source_count=excluded.source_count, field_group=excluded.field_group,
          field_kind=excluded.field_kind, computed_at=now()
      where beach_field_consensus.winning_value is distinct from excluded.winning_value
         or beach_field_consensus.confidence is distinct from excluded.confidence
         or beach_field_consensus.disagreement is distinct from excluded.disagreement
         or beach_field_consensus.source_count is distinct from excluded.source_count
         or beach_field_consensus.vote_breakdown::text is distinct from excluded.vote_breakdown::text
    returning (xmax=0) as inserted, gold_fid
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted),
         count(distinct gold_fid)
    into ins_count, upd_count, beaches_count
    from upsert;

  return query select ins_count, upd_count, beaches_count;
end;
$function$;

COMMIT;
