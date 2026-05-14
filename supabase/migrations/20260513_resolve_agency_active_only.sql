-- 20260513_resolve_agency_active_only.sql
--
-- Two related fixes for the dupe-operator-resurrection bug:
--
-- 1. resolve_agency() must filter out inactive/deduped operators.
--    Previously: matched on agency_aliases.alias_normalized and returned
--    whatever operator_id was found, regardless of is_active. After
--    Pass 12 fuzzy consolidation set is_active=false + parent_operator_id
--    on dupes, resolve_agency happily kept resolving to them. Then
--    populate_governance_from_polygon_gold wrote new BEP rows pointing
--    to the dupes — even after the pass12_migrate_refs migration cleaned
--    up the OLD rows.
--
-- 2. state_operator_ids_for_scoreable_beaches must follow parent chains.
--    Defensive: even if a BEP row still points to a dupe (e.g. written
--    before the resolve_agency fix shipped), the helper returns the
--    canonical operator_id. Phase 26 then extracts policy for the
--    canonical only.

begin;

-- ────────────────────────────────────────────────────────────────────────
-- 1. resolve_agency — filter to is_active operators with no parent
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.resolve_agency(
  p_alias text,
  p_alias_source text default null,
  p_state_code text default null,
  p_min_confidence numeric default 0.65
)
returns table(operator_id bigint, confidence numeric, method text)
language plpgsql stable as $function$
declare
  v_norm text := public._normalize_agency_text(p_alias);
  v_stripped text := trim(regexp_replace(
    v_norm,
    '^(the |city of |town of |county of |state of |village of |borough of |township of )',
    '', 'i'
  ));
  v_stripped_with_county text := trim(regexp_replace(
    v_norm,
    ' (city|town|county|borough|township)$',
    '', 'i'
  ));
begin
  if p_alias is null or trim(p_alias) = '' then
    return;
  end if;

  -- Step a: exact alias match
  return query
    select aa.operator_id, 1.0::numeric, 'exact'::text
      from public.agency_aliases aa
      join public.operators op on op.id = aa.operator_id
     where aa.alias = p_alias
       and op.is_active                       -- ★ NEW: only canonical
       and op.parent_operator_id is null      -- ★ NEW: skip deduped
       and (p_state_code is null or op.state_code = p_state_code)
     order by aa.confidence desc
     limit 1;
  if found then return; end if;

  -- Step b: normalized exact
  return query
    select aa.operator_id, 0.90::numeric, 'normalized_exact'::text
      from public.agency_aliases aa
      join public.operators op on op.id = aa.operator_id
     where aa.alias_normalized = v_norm
       and op.is_active
       and op.parent_operator_id is null
       and (p_state_code is null or op.state_code = p_state_code)
     order by aa.confidence desc
     limit 1;
  if found then return; end if;

  -- Step c: stripped-variant exact
  if v_stripped <> v_norm then
    return query
      select aa.operator_id, 0.80::numeric, 'normalized_stripped'::text
        from public.agency_aliases aa
        join public.operators op on op.id = aa.operator_id
       where aa.alias_normalized = v_stripped
         and op.is_active
         and op.parent_operator_id is null
         and (p_state_code is null or op.state_code = p_state_code)
       order by aa.confidence desc
       limit 1;
    if found then return; end if;
  end if;
  if v_stripped_with_county <> v_norm then
    return query
      select aa.operator_id, 0.80::numeric, 'normalized_stripped'::text
        from public.agency_aliases aa
        join public.operators op on op.id = aa.operator_id
       where aa.alias_normalized = v_stripped_with_county
         and op.is_active
         and op.parent_operator_id is null
         and (p_state_code is null or op.state_code = p_state_code)
       order by aa.confidence desc
       limit 1;
    if found then return; end if;
  end if;

  -- Step d: trigram similarity
  perform set_limit(p_min_confidence::real);
  return query
    select aa.operator_id,
           similarity(aa.alias_normalized, v_norm)::numeric as sim,
           'fuzzy'::text
      from public.agency_aliases aa
      join public.operators op on op.id = aa.operator_id
     where aa.alias_normalized % v_norm
       and op.is_active
       and op.parent_operator_id is null
       and (p_state_code is null or op.state_code = p_state_code)
     order by sim desc
     limit 1;
end $function$;

-- ────────────────────────────────────────────────────────────────────────
-- 2. state_operator_ids_for_scoreable_beaches — follow parent chains
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.state_operator_ids_for_scoreable_beaches(p_state text)
returns table(operator_id bigint)
language sql stable as $$
  -- Defensive: if any BEP row still points to a deduped operator (e.g.
  -- written before the resolve_agency-is-active fix shipped), follow the
  -- parent_operator_id chain to the canonical. Phase 26 extracts policy
  -- for the canonical only.
  select distinct coalesce(o.parent_operator_id, o.id)
    from public.beach_enrichment_provenance bep
    join public.beaches_gold g on g.fid = bep.gold_fid
    join public.operators o on o.id = bep.operator_id
   where g.state = p_state
     and g.is_active
     and g.scoring_tier in ('daily','hourly')
     and bep.operator_id is not null;
$$;

commit;
