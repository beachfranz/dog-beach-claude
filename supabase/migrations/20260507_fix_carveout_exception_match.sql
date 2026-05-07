-- 20260507_fix_carveout_exception_match.sql
--
-- Fix the carve-out matching bug in _emit_evidence_from_operator_policy_exceptions.
--
-- SYMPTOM (Coronado Dog Beach, fid 6202):
--   Matcher picked exception "Coronado Beach" (rule=prohibited, sim 0.789)
--   over "Dog Beach" (rule=off_leash) for the carve-out beach record.
--   Consensus then averaged across the wrong-scoped vote and produced
--   mixed/on_leash instead of yes/off_leash.
--
-- ROOT CAUSE:
--   Trigram similarity treats "Coronado Beach" (parent) and "Dog Beach"
--   (carve-out) as roughly equal matches to "Coronado Dog Beach" because
--   both share 2 of 3 tokens. Trigram alone can't disambiguate
--   parent-vs-carve-out semantics.
--
-- FIX:
--   Prefer matches where one name is a SUBSTRING of the other (after
--   case + whitespace normalization). Substring containment is a strong
--   semantic signal:
--     "Coronado Dog Beach" CONTAINS "Dog Beach"           ✅ carve-out match
--     "Coronado Dog Beach" CONTAINS "Coronado Beach"      ❌ ("dog" breaks contiguity)
--   Trigram sim falls through as tiebreaker for non-substring cases.
--
-- BACKWARD COMPATIBILITY:
--   Existing matches without substring relationships use trigram as
--   before. The change only re-ranks beaches where a substring match
--   exists (rare; mostly carve-out cases like Coronado Dog Beach).

begin;

create or replace function public._emit_evidence_from_operator_policy_exceptions(p_fid bigint default null)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with candidate_pairs as (
    -- All beach × exception combinations within the same operator polygon,
    -- where the beach name is at least somewhat similar to the exception
    -- beach_name. similarity > 0.4 prunes obvious mismatches.
    select bg.fid as gold_fid,
           ope.id as exception_id,
           ope.rule as raw_rule,
           ope.beach_name as exception_beach_name,
           ope.source_quote, ope.source_url,
           ope.operator_id,
           o.canonical_name as operator_name,
           similarity(lower(bg.name), lower(ope.beach_name)) as sim_score,
           -- Substring containment in either direction is a strong
           -- semantic match (parent/carve-out relationship). We use it
           -- as the primary ranking signal; trigram tiebreaks.
           (
             lower(bg.name) like '%' || lower(ope.beach_name) || '%'
             or lower(ope.beach_name) like '%' || lower(bg.name) || '%'
           ) as substring_match,
           -- Bonus signal: how many tokens overlap. Helps tiebreak when
           -- multiple exceptions are substrings (rare).
           (
             select count(*)
               from regexp_split_to_table(lower(bg.name), '[^a-z0-9]+') t
              where t <> ''
                and lower(ope.beach_name) like '%' || t || '%'
           ) as token_overlap
      from public.beaches_gold bg
      join public.operators o on ST_Intersects(o.geom, bg.geom)
      join public.operator_policy_exceptions ope on ope.operator_id = o.id
     where bg.is_active
       and (p_fid is null or bg.fid = p_fid)
       and similarity(lower(bg.name), lower(ope.beach_name)) > 0.4
  ),
  best_match_per_beach as (
    select distinct on (gold_fid) *
      from candidate_pairs
     order by gold_fid,
              substring_match desc,   -- carve-out semantic match wins first
              token_overlap  desc,    -- then most tokens shared
              sim_score      desc,    -- finally trigram (preserves prior behavior)
              exception_id   asc
  ),
  payload as (
    select
      gold_fid, operator_name, source_url, exception_beach_name,
      sim_score, operator_id, raw_rule, source_quote, substring_match,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed', case raw_rule
          when 'allowed'    then 'yes'
          when 'prohibited' then 'no'
          when 'no'         then 'no'
          when 'off_leash'  then 'yes'
          when 'restricted' then 'mixed'
          else null end,
        'leash_required', case raw_rule
          when 'off_leash' then 'off_leash'
          when 'allowed'   then 'on_leash'
          else null end,
        'off_leash_exists', case when raw_rule = 'off_leash' then 'true' else null end,
        'source_quote', source_quote,
        'matched_exception_name', exception_beach_name,
        'similarity_score', round(sim_score::numeric, 3)::text,
        'substring_match', case when substring_match then 'true' else 'false' end,
        'operator_id', operator_id::text
      )) as cv,
      -- Confidence: substring matches earn a bonus because they're
      -- semantically stronger. Otherwise fall back to sim-tiered.
      case
        when substring_match  then 0.85
        when sim_score > 0.7  then 0.80
        when sim_score > 0.5  then 0.70
        else 0.60
      end as confidence
    from best_match_per_beach
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, cpad_unit_name, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'operator_policy_exceptions_v1', source_url,
           cv, confidence, false, operator_name, 'derived_url_crawl', 'beach_access', now()
      from payload where cv <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          confidence     = excluded.confidence,
          source_url     = excluded.source_url,
          cpad_unit_name = excluded.cpad_unit_name,
          updated_at     = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
       or beach_enrichment_provenance.confidence is distinct from excluded.confidence
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end
$function$;

commit;
