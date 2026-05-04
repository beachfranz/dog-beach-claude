-- 20260504_unified_pipeline_phase1_consensus_source_family.sql
--
-- Consensus ensemble fix: collapse correlated sources into families before
-- summing votes. Caught 2026-05-04 on Stinson Beach — has_lifeguards
-- resolved to "true" because old_school_llm (0.46) + unified_v1 (0.29)
-- summed to 0.74, beating park_url's lone 0.60 vote of "false". But
-- old_school_llm + unified_v1 + json_explode are all "LLM looks at a
-- page, says yes/no" — same kind of evidence. Treating them as 3
-- independent votes triple-counts.
--
-- Fix: within a source family (e.g. all llm_extraction), take MAX weight
-- (best representative). Across families, SUM. Manual stays its own
-- family with calibration_weight=1.0 (heavy single vote).

begin;

create or replace function public._source_family(p_source text)
returns text language sql immutable as $$
  select case p_source
    when 'manual'                      then 'manual'
    when 'manual_curator'              then 'manual'
    when 'park_url'                    then 'park_url'
    when 'park_url_governance'         then 'park_url'
    when 'park_url_buffer_attribution' then 'park_url'
    when 'park_operators'              then 'park_operators'
    when 'research'                    then 'research'
    when 'governing_body'              then 'governing_body'
    when 'old_school_llm'              then 'llm_extraction'
    when 'unified_v1'                  then 'llm_extraction'
    when 'json_explode'                then 'llm_extraction'
    when 'llm'                         then 'llm_extraction'
    when 'cpad'                        then 'spatial'
    when 'jurisdictions'               then 'spatial'
    when 'counties'                    then 'spatial'
    when 'military_bases'              then 'spatial'
    when 'tribal_lands'                then 'spatial'
    when 'csp_parks'                   then 'spatial'
    when 'csp_places'                  then 'spatial'
    when 'nps_places'                  then 'spatial'
    when 'tiger_places'                then 'spatial'
    when 'pad_us'                      then 'spatial'
    when 'sma_code_mappings'           then 'spatial'
    when 'name'                        then 'name_heuristic'
    when 'plz'                         then 'plz'
    when 'ccc'                         then 'ccc'
    when 'web_scrape'                  then 'park_url'
    else p_source
  end;
$$;

comment on function public._source_family(text) is
  'Maps a beach_enrichment_provenance.source value to its independence '
  'family. Within a family, sources measure the same underlying signal '
  '(e.g. LLM-extraction sources all read a page and judge). Consensus '
  'aggregates by taking MAX weight within family before summing across '
  'families — prevents triple-counting correlated sources.';


-- Updated compute_beach_field_consensus with family aggregation
create or replace function public.compute_beach_field_consensus(
  p_fid bigint default null
)
returns table(rows_inserted bigint, rows_updated bigint, beaches_touched bigint) as $$
declare
  ins_count bigint := 0;
  upd_count bigint := 0;
  beaches_count bigint := 0;
begin
  with claims as (
    select e.gold_fid, c.field_group, c.claim_key, c.field_kind, e.source,
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
  weighted as (
    select c.*,
           c.source_confidence * public._calibration_weight(c.field_kind, c.source) as vote_weight
      from claims c
     where c.norm_value is not null
  ),
  -- WITHIN family: take MAX-weight representative (collapses correlation)
  family_votes as (
    select gold_fid, field_group, claim_key, field_kind, norm_value, family,
           max(vote_weight) as family_weight,
           (array_agg(jsonb_build_object(
             'source', source,
             'weight', round(vote_weight::numeric, 3),
             'src_conf', round(source_confidence::numeric, 3)
           ) order by vote_weight desc))[1] as top_member,
           count(*) as n_in_family
      from weighted
     group by gold_fid, field_group, claim_key, field_kind, norm_value, family
  ),
  -- ACROSS families: SUM (independent evidence stacks)
  votes as (
    select gold_fid, field_group, claim_key, field_kind, norm_value,
           sum(family_weight) as bucket_weight,
           jsonb_agg(jsonb_build_object(
             'family', family,
             'weight', round(family_weight::numeric, 3),
             'top_member', top_member,
             'n_in_family', n_in_family
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
           (select bucket_weight from ranked r2
             where r2.gold_fid=ranked.gold_fid and r2.claim_key=ranked.claim_key
               and r2.rnk=2 limit 1) as runner_up_weight,
           (select jsonb_object_agg(r3.norm_value,
              jsonb_build_object('weight', round(r3.bucket_weight::numeric,3), 'voters', r3.voters))
              from ranked r3 where r3.gold_fid=ranked.gold_fid
                            and r3.claim_key=ranked.claim_key) as breakdown,
           (select count(*)::int from ranked r4
              where r4.gold_fid=ranked.gold_fid and r4.claim_key=ranked.claim_key) as bucket_count
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
    returning (xmax=0) as inserted, gold_fid
  )
  select count(*) filter (where inserted),
         count(*) filter (where not inserted),
         count(distinct gold_fid)
    into ins_count, upd_count, beaches_count
    from upsert;

  return query select ins_count, upd_count, beaches_count;
end;
$$ language plpgsql;

comment on function public.compute_beach_field_consensus(bigint) is
  'Phase B Layer 2 consensus with source-family aggregation (2026-05-04). '
  'Within an independence family (e.g. llm_extraction = old_school_llm + '
  'unified_v1 + json_explode), take MAX-weight representative — prevents '
  'correlated sources from triple-counting. Across families, SUM. Manual '
  'is its own family with calibration_weight=1.0 (heavy single vote). '
  'See _source_family() for the taxonomy.';

commit;
