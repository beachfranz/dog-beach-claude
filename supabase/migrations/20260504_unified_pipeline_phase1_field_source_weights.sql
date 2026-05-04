-- 20260504_unified_pipeline_phase1_field_source_weights.sql
--
-- Unified pipeline phase 1, move 4: per-field-group source priority in
-- the resolver, replacing the flat priority that ranked sources the same
-- across all field groups.
--
-- Calibration findings (from project_extraction_calibration.md, validated
-- 2026-05-03 against curator gold-set with vocabulary normalization):
--
--   dogs (n=35-49 each, vocab-normalized binary):
--     research        91.4%  ← best for dogs_allowed AND leash_policy
--     park_url        85.7%
--     governing_body  75%    (50% on leash specifically — coin-flip; demote)
--     old_school_llm  69.4%
--
--   practical (n=10-44 each):
--     park_url        90%    ← best for lifeguard
--     old_school_llm  82%
--
--   governance + access: no calibration yet → preserve flat priority
--
-- Today's _gold_source_priority puts park_url (3) above research (4), so
-- when both have evidence, park_url wins canonical — backwards for dogs.
-- This migration introduces _gold_field_group_source_priority(field_group,
-- source) which respects the per-field-group calibration ranking.
--
-- Manual always wins (priority 1) regardless of field group.

begin;

create or replace function public._gold_field_group_source_priority(
  p_field_group text,
  p_source text
) returns integer language sql immutable as $$
  select case
    -- Manual override always wins, regardless of field group
    when p_source = 'manual' then 1

    -- DOGS field group: research best (91%), park_url next (86%),
    --   governing_body weak on leash (50%) so demote, old_school_llm baseline
    when p_field_group = 'dogs' then case p_source
      when 'llm'              then 2
      when 'research'         then 3   -- 91% on allowed, 83% on leash
      when 'park_url'         then 4   -- 86% on allowed, 75% on leash
      when 'json_explode'     then 5   -- structured payload from old_school_llm extractions
      when 'old_school_llm'   then 5   -- 69% on allowed, 77% on leash
      when 'park_operators'   then 6
      when 'governing_body'   then 7   -- demoted: 50% on leash (coin-flip)
      when 'cpad'             then 8
      when 'jurisdictions'    then 9
      when 'counties'         then 10
      when 'nps_places'       then 11
      when 'military_bases'   then 12
      when 'tribal_lands'     then 12
      else                          99
    end

    -- PRACTICAL field group: park_url best on lifeguard (90%), old_school_llm
    --   second (82%), json_explode preserves structured payload
    when p_field_group = 'practical' then case p_source
      when 'llm'              then 2
      when 'park_url'         then 3   -- 90% on lifeguard
      when 'park_operators'   then 4
      when 'old_school_llm'   then 5   -- 82% on lifeguard
      when 'json_explode'     then 5   -- same priority as old_school_llm
      when 'ccc'              then 6   -- amenity overlay (when CCC integration ships)
      when 'research'         then 7
      else                          99
    end

    -- GOVERNANCE field group: no calibration yet — preserve flat priority
    --   (operator-style sources before spatial; spatial before name-based)
    when p_field_group = 'governance' then case p_source
      when 'park_url'                    then 2
      when 'park_operators'              then 2
      when 'park_url_buffer_attribution' then 3
      when 'csp_parks'                   then 4
      when 'nps_places'                  then 4
      when 'cpad'                        then 5
      when 'tiger_places'                then 6
      when 'governing_body'              then 7
      when 'name'                        then 8
      when 'tribal_lands'                then 9
      when 'military_bases'              then 9
      else                                    99
    end

    -- ACCESS field group: no calibration yet — preserve flat priority
    when p_field_group = 'access' then case p_source
      when 'plz'           then 2
      when 'cpad'          then 3
      when 'json_explode'  then 4
      when 'csp_parks'     then 5
      when 'military_bases' then 6
      else                       99
    end

    -- POLYGON_CONTAINMENT: deterministic spatial — flat priority is fine
    when p_field_group = 'polygon_containment' then case p_source
      when 'cpad'           then 2
      when 'jurisdictions'  then 3
      when 'counties'       then 4
      when 'military_bases' then 5
      when 'tribal_lands'   then 5
      else                       99
    end

    else 99
  end;
$$;

comment on function public._gold_field_group_source_priority(text, text) is
  'Phase 1 unified pipeline (2026-05-04). Per-field-group source priority '
  'replacing flat _gold_source_priority. Encodes calibration findings: '
  'dogs={research>park_url>old_school_llm>governing_body}, '
  'practical={park_url>old_school_llm}. Governance/access preserved at flat '
  'priority pending calibration. Manual always wins (priority 1) regardless.';

-- Update resolver to use field-group-aware priority
create or replace function public._resolve_field_group_gold(
  p_field_group text,
  p_fid bigint default null
) returns integer language plpgsql as $$
declare touched int := 0;
begin
  update public.beach_enrichment_provenance e
     set is_canonical = false
   where e.field_group = p_field_group
     and e.gold_fid is not null
     and (p_fid is null or e.gold_fid = p_fid);

  with ranked as (
    select e.id,
      row_number() over (
        partition by e.gold_fid
        order by public._gold_field_group_source_priority(e.field_group, e.source) asc,
                 e.confidence desc nulls last,
                 e.id asc
      ) as rnk
    from public.beach_enrichment_provenance e
    where e.field_group = p_field_group
      and e.gold_fid is not null
      and (p_fid is null or e.gold_fid = p_fid)
  ),
  upd as (
    update public.beach_enrichment_provenance e
       set is_canonical = true
      from ranked r
     where r.id = e.id and r.rnk = 1
    returning 1
  )
  select count(*) into touched from upd;

  return touched;
end;
$$;

comment on function public._resolve_field_group_gold(text, bigint) is
  'Phase 1 unified pipeline (2026-05-04). Per-beach canonical pick using '
  'field-group-aware source priority (calibration-derived). Manual wins always. '
  'Tiebreaks: confidence DESC, then id ASC.';

commit;
