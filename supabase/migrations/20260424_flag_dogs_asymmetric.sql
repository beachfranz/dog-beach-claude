-- flag_dogs_consistency — narrow to ASYMMETRIC disagreement (2026-04-24)
--
-- The previous version flagged any non-equal claim (465 of 860 = 54%).
-- 54% real-world disagreement is implausible — the number was inflated
-- by research correctly capturing beach-specific nuance the broad agency
-- default doesn't have ("city → yes/required" generic vs research's
-- "restricted, no dogs 10am-6pm summer").
--
-- New rule: only flag when research is MORE PERMISSIVE than the agency
-- default — that's where an LLM might have hallucinated allowance.
--
-- Permissiveness order on dogs_allowed:    no < restricted < seasonal < yes
-- Strictness order on dogs_leash_required: required < mixed < off_leash_ok
--
-- Only flag when both sides have a definite categorical answer (not
-- 'unknown' or NULL) — we can't conclude "disagreement" if one side
-- admits ignorance.
--
-- Also: clear any prior 'dogs disagreement' flag at the start so re-runs
-- give clean output (won't unflag rows that humans separately set
-- review_status='verified' or 'flagged').

create or replace function public.flag_dogs_consistency(p_fid int default null)
returns int
language plpgsql
as $$
declare flagged int;
begin
  -- Clear any prior dogs-disagreement flag this function set (idempotent re-run)
  update public.locations_stage
    set review_status = null,
        review_notes  = null
    where review_status = 'needs_review'
      and review_notes like 'dogs disagreement%'
      and (p_fid is null or fid = p_fid);

  -- Asymmetric flag rule
  with permissive_rank(v, r) as (values
    ('no'::text, 0), ('restricted', 1), ('seasonal', 2), ('yes', 3)
    -- 'unknown' deliberately omitted — we don't compare against unknowns
  ),
  leash_rank(v, r) as (values
    ('required'::text, 0), ('mixed', 1), ('off_leash_ok', 2)
    -- 'unknown' deliberately omitted
  ),
  pairs as (
    select r.fid,
      r.claimed_values->>'allowed'        as r_allowed,
      g.claimed_values->>'allowed'        as g_allowed,
      r.claimed_values->>'leash_required' as r_leash,
      g.claimed_values->>'leash_required' as g_leash,
      g.source_url                        as g_source_url
    from public.beach_enrichment_provenance r
    join public.beach_enrichment_provenance g
      on g.fid = r.fid and g.field_group = 'dogs' and g.source = 'governing_body'
    where r.field_group = 'dogs' and r.source = 'research'
      and (p_fid is null or r.fid = p_fid)
  ),
  ranked as (
    select p.*,
      pa_r.r as r_allowed_rank, pa_g.r as g_allowed_rank,
      pl_r.r as r_leash_rank,   pl_g.r as g_leash_rank
    from pairs p
    left join permissive_rank pa_r on pa_r.v = p.r_allowed
    left join permissive_rank pa_g on pa_g.v = p.g_allowed
    left join leash_rank      pl_r on pl_r.v = p.r_leash
    left join leash_rank      pl_g on pl_g.v = p.g_leash
  ),
  asym as (
    -- Research more permissive than agency on EITHER axis
    select fid, r_allowed, g_allowed, r_leash, g_leash, g_source_url,
      (r_allowed_rank is not null and g_allowed_rank is not null
                                  and r_allowed_rank > g_allowed_rank)  as too_permissive_allowed,
      (r_leash_rank   is not null and g_leash_rank   is not null
                                  and r_leash_rank   > g_leash_rank)    as too_lax_leash
    from ranked
  )
  update public.locations_stage s
    set review_status = case when s.review_status is null
                              then 'needs_review' else s.review_status end,
        review_notes  = 'dogs disagreement (research more permissive): ' ||
                          'allowed=' || coalesce(a.r_allowed, 'null') ||
                          ' (vs agency=' || coalesce(a.g_allowed, 'null') || ')' ||
                          ', leash=' || coalesce(a.r_leash, 'null') ||
                          ' (vs agency=' || coalesce(a.g_leash, 'null') || ')' ||
                          coalesce(' [' || a.g_source_url || ']', '')
    from asym a
    where s.fid = a.fid
      and (a.too_permissive_allowed or a.too_lax_leash);

  get diagnostics flagged = row_count;
  return flagged;
end;
$$;

comment on function public.flag_dogs_consistency(int) is
  'Asymmetric disagreement detector: flags only when per-beach research claims something MORE permissive than the agency default (research=yes vs agency=no/restricted; research=off_leash_ok vs agency=required). Skips when either side is unknown/null. Idempotent — clears prior dogs-disagreement flags before re-applying. Run after resolve_dogs.';
