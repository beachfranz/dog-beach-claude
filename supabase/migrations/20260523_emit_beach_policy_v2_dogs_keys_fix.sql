-- 20260523_emit_beach_policy_v2_dogs_keys_fix.sql
--
-- Same consensus-key drift as gap #46e but in
-- _emit_evidence_from_beach_policy_dogs_v2. The emitter writes
-- claimed_values with keys 'dogs_allowed' + 'leash_policy' but consensus
-- expects 'allowed' + 'leash_required' per consensus_field_config.
--
-- 100+43 BEP rows (gap #46f) silently no-op-ing in consensus voting.
--
-- Three steps mirror gap #46e migration:
--   1. CREATE OR REPLACE the emitter with consensus-aligned keys.
--   2. UPDATE existing wrong-key BEP rows in place.
--   3. Refresh consensus for affected beaches.

BEGIN;

CREATE OR REPLACE FUNCTION public._emit_evidence_from_beach_policy_dogs_v2(p_fid bigint DEFAULT NULL::bigint)
RETURNS TABLE(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
LANGUAGE plpgsql
AS $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with normalized as (
    select arena_group_id as gold_fid, field_name,
      case
        when field_name = 'dogs_allowed' then
          case lower(parsed_value)
            when 'yes' then 'yes'
            when 'no'  then 'no'
            when 'seasonal' then 'mixed'
            else null end
        when field_name = 'leash_policy' then
          case lower(parsed_value)
            when 'off_leash' then 'off_leash'
            when 'on_leash'  then 'on_leash'
            else null end
        when field_name = 'public_access' then
          case lower(parsed_value)
            when 'yes'        then 'yes'
            when 'restricted' then 'restricted'
            when 'no'         then 'no'
            else null end
      end as norm
    from public.beach_policy_extractions
    where field_name in ('dogs_allowed','leash_policy','public_access')
      and parse_succeeded = true
      and (p_fid is null or arena_group_id = p_fid)
  ),
  tallied as (
    select gold_fid, field_name, norm,
      count(*) as votes,
      sum(count(*)) over (partition by gold_fid, field_name
                          rows between unbounded preceding and unbounded following) as field_total
    from normalized
    where norm is not null
    group by 1,2,3
  ),
  winner as (
    select distinct on (gold_fid, field_name)
      gold_fid, field_name, norm as winning, votes as n_winning, field_total
    from tallied
    order by gold_fid, field_name, votes desc
  ),
  dissent_by_field as (
    select gold_fid, field_name, (field_total - n_winning) as n_dissent
    from winner
  ),
  scored as (
    select w.gold_fid, w.field_name, w.winning, w.n_winning, d.n_dissent,
      case
        when w.n_winning >= 3 and d.n_dissent = 0 then 0.90
        when w.n_winning >= 2 and d.n_dissent = 0 then 0.75
        when w.n_winning > d.n_dissent             then 0.55
        else                                            0.40
      end as conf
    from winner w
    join dissent_by_field d using (gold_fid, field_name)
  ),
  per_fid as (
    select gold_fid,
      -- Consensus-aligned keys (gap #46f): 'allowed' and 'leash_required'
      -- instead of 'dogs_allowed' and 'leash_policy'.
      -- public_access stays as-is — it's not in consensus_field_config
      -- for dogs field_group, but the raw extraction may be useful for
      -- future analyses (and harmless to keep).
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',        max(case when field_name='dogs_allowed'  then winning end),
        'leash_required', max(case when field_name='leash_policy'  then winning end),
        'public_access',  max(case when field_name='public_access' then winning end)
      )) as cv,
      max(conf) as row_conf,
      bool_or(n_dissent > 0) as row_dissent
    from scored
    group by gold_fid
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'beach_policy_v2_dogs', null, cv,
           row_conf, false, 'derived_url_crawl', 'beach_access', now()
    from per_fid where cv <> '{}'::jsonb and row_conf > 0
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          confidence     = excluded.confidence,
          updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
       or beach_enrichment_provenance.confidence    is distinct from excluded.confidence
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end $function$;

-- ── Rename keys in existing wrong-key BEP rows ──────────────────────
UPDATE public.beach_enrichment_provenance bep
   SET claimed_values = (
         (bep.claimed_values - 'dogs_allowed' - 'leash_policy')
         || CASE WHEN bep.claimed_values ? 'dogs_allowed'
                 THEN jsonb_build_object('allowed', bep.claimed_values->>'dogs_allowed')
                 ELSE '{}'::jsonb END
         || CASE WHEN bep.claimed_values ? 'leash_policy'
                 THEN jsonb_build_object('leash_required', bep.claimed_values->>'leash_policy')
                 ELSE '{}'::jsonb END
       ),
       updated_at = now()
 WHERE bep.field_group = 'dogs'
   AND bep.source = 'beach_policy_v2_dogs'
   AND (bep.claimed_values ? 'dogs_allowed' OR bep.claimed_values ? 'leash_policy');

-- ── Refresh consensus + resolver + consumer-table promotion ─────────
WITH affected AS (
  SELECT DISTINCT gold_fid
    FROM public.beach_enrichment_provenance
   WHERE field_group = 'dogs' AND source = 'beach_policy_v2_dogs'
     AND gold_fid IS NOT NULL
)
SELECT public._resolve_dogs_gold(a.gold_fid) FROM affected a;

WITH affected AS (
  SELECT DISTINCT gold_fid
    FROM public.beach_enrichment_provenance
   WHERE field_group = 'dogs' AND source = 'beach_policy_v2_dogs'
     AND gold_fid IS NOT NULL
)
SELECT (public.compute_beach_field_consensus(a.gold_fid)).rows_inserted FROM affected a;

WITH affected AS (
  SELECT DISTINCT gold_fid
    FROM public.beach_enrichment_provenance
   WHERE field_group = 'dogs' AND source = 'beach_policy_v2_dogs'
     AND gold_fid IS NOT NULL
)
SELECT public.promote_canonical_to_consumer_tables(a.gold_fid) FROM affected a;

COMMIT;
