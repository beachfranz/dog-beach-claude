-- Phase 7b of POLICY_RESEARCH_MIGRATION (2026-04-25)
--
-- Closes the gap where populate_from_research emits evidence rows but
-- nothing picks canonical from them. Adds:
--
--   1. _resolve_research_evidence(p_fid) — picks canonical from
--      research / old_school_llm evidence for dogs + practical
--      field_groups. Defers to ANY non-research canonical (park_url,
--      manual, etc.) when present, so this never displaces existing
--      canonical attributions.
--
--   2. populate_from_research now calls _resolve_research_evidence +
--      _promote_dogs_to_stage + _promote_practical_to_stage at end,
--      completing the evidence → canonical → locations_stage chain.
--
-- Conservative design — only fills gaps where no canonical existed.
-- Doesn't touch park_url canonicals or override the NEW pipeline's work.
-- A future pass can add cross-source override semantics for research vs
-- park_url (e.g., fresh research with multi-source agreement might beat
-- a single park_url scrape).

-- ── 1. The new resolver ──────────────────────────────────────────────────
create or replace function public._resolve_research_evidence(p_fid int default null)
returns void
language plpgsql
as $$
begin
  -- Rank research/old_school_llm evidence per (fid, field_group) by:
  --   1. source_precedence (research=71 < old_school_llm=75, lower wins)
  --   2. confidence desc
  --   3. updated_at desc (freshest tiebreaker)
  --   4. id asc (stable)
  drop table if exists _research_resolver_ranked;
  create temporary table _research_resolver_ranked on commit drop as
  select e.id, e.fid, e.field_group,
         row_number() over (
           partition by e.fid, e.field_group
           order by
             public.source_precedence(e.source) asc,
             e.confidence desc nulls last,
             e.updated_at desc,
             e.id asc
         ) as rnk
  from public.beach_enrichment_provenance e
  where e.field_group in ('dogs', 'practical')
    and e.source in ('research', 'old_school_llm')
    and (p_fid is null or e.fid = p_fid);

  -- Pass 1: clear losing research/old_school canonicals (rank > 1)
  update public.beach_enrichment_provenance e
     set is_canonical = false
    from _research_resolver_ranked r
   where e.id = r.id
     and r.rnk > 1
     and e.is_canonical = true;

  -- Pass 2: set winning research/old_school evidence as canonical IF no
  -- non-research-family canonical exists for this (fid, field_group).
  -- Defers to manual, park_url, park_url_buffer_attribution, ccc, cpad,
  -- etc. — anything that's not research-family wins precedence.
  update public.beach_enrichment_provenance e
     set is_canonical = true
    from _research_resolver_ranked r
   where e.id = r.id
     and r.rnk = 1
     and e.is_canonical = false
     and not exists (
       select 1
         from public.beach_enrichment_provenance other
        where other.fid = e.fid
          and other.field_group = e.field_group
          and other.id <> e.id
          and other.is_canonical = true
          and other.source not in ('research', 'old_school_llm')
     );
end;
$$;

comment on function public._resolve_research_evidence(int) is
  'Layer 2 resolver for research / old_school_llm evidence. Picks canonical per (fid, field_group) by source_precedence + confidence + freshness. Defers to ANY non-research canonical (park_url/manual/cpad/ccc/etc.) — only fills gaps, never displaces. Conservative: ensures research data flows to locations_stage where no other source has spoken.';

-- ── 2. Update populate_from_research to chain resolve + promote ──────────
-- Wraps the existing evidence-emit logic with downstream resolver +
-- promoter calls. Same shape as populate_from_park_url's orchestrator.
create or replace function public.populate_from_research(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int := 0;
begin
  -- Evidence emission (unchanged from Phase 4)
  with successful as (
    select * from public.policy_research_extractions
    where extraction_status in ('success', 'imported_legacy')
      and (p_fid is null or fid = p_fid)
  ),
  tagged as (
    select *,
      case origin
        when 'v2_dog_policy_old' then 'old_school_llm'
        when 'v2_dog_policy_v2'  then 'research'
        when 'manual'            then 'manual'
      end as evidence_source
    from successful
  ),
  dogs_built as (
    select fid, primary_source_url, evidence_source, extraction_confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',          dogs_allowed,
        'leash_required',   dogs_leash_required,
        'restricted_hours', dogs_restricted_hours,
        'seasonal_rules',   dogs_seasonal_rules,
        'zone_description', dogs_zone_description,
        'notes',            dogs_policy_notes
      )) as v
    from tagged
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, source_url, updated_at)
    select fid, 'dogs', evidence_source,
      coalesce(extraction_confidence, 0.65),
      v, primary_source_url, now()
    from dogs_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source, coalesce(source_url, '')) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  practical_built as (
    select fid, primary_source_url, evidence_source, extraction_confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'hours_text',         hours_text,
        'open_time',          open_time::text,
        'close_time',         close_time::text,
        'has_parking',        has_parking,
        'parking_type',       parking_type,
        'parking_notes',      parking_notes,
        'has_restrooms',      has_restrooms,
        'has_showers',        has_showers,
        'has_drinking_water', has_drinking_water,
        'has_lifeguards',     has_lifeguards,
        'has_disabled_access',has_disabled_access,
        'has_food',           has_food,
        'has_fire_pits',      has_fire_pits,
        'has_picnic_area',    has_picnic_area
      )) as v
    from tagged
  ),
  ins_practical as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, source_url, updated_at)
    select fid, 'practical', evidence_source,
      coalesce(extraction_confidence, 0.65),
      v, primary_source_url, now()
    from practical_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source, coalesce(source_url, '')) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from ins_dogs union all select * from ins_practical
  ) _;

  -- NEW: resolve canonical for research-family evidence (gap-fill only)
  perform public._resolve_research_evidence(p_fid);

  -- NEW: promote canonical evidence values to locations_stage columns
  perform public._promote_dogs_to_stage(p_fid);
  perform public._promote_practical_to_stage(p_fid);

  return rows_touched;
end;
$$;

comment on function public.populate_from_research(int) is
  'Layer 2 populator + resolver + promoter. Emits dogs + practical evidence rows from policy_research_extractions, resolves canonical via _resolve_research_evidence (defers to non-research canonicals), promotes canonical to locations_stage via _promote_dogs/practical_to_stage. Phase 7b of POLICY_RESEARCH_MIGRATION (2026-04-25).';
