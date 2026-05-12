-- Quality gate: every active+scoreable beach must have a canonical
-- governance row in beach_enrichment_provenance.
--
-- Franz 2026-05-12: no beach goes to production without operator
-- attribution. Without it we can't apply operator-specific photo
-- strategies, surface operator dog policies, slice metrics, generate
-- descriptions anchored on the managing agency, or honor takedowns.
--
-- See feedback_governance_required_per_beach.md.
--
-- Used by the Dagster gate asset and as a launch-checklist query for
-- each new state.

-- View: per-beach attribution status. NULL governance row = unattributed.
create or replace view public.beach_governance_status as
  select g.fid,
         g.state,
         g.is_active,
         g.is_scoreable,
         coalesce(g.display_name_override, g.name) as name,
         b.claimed_values->>'name' as operator_name,
         b.claimed_values->>'type' as operator_type,
         b.source as governance_source,
         b.operator_id,
         (b.claimed_values->>'name' is not null) as is_attributed
    from public.beaches_gold g
    left join public.beach_enrichment_provenance b
      on b.gold_fid = g.fid
     and b.field_group = 'governance'
     and b.is_canonical = true;

comment on view public.beach_governance_status is
  'Per-beach governance attribution status. is_attributed=false on '
  'active+scoreable rows is a pipeline quality-gate failure.';

-- Function: count unattributed beaches for a state (or all states if null).
-- Returns 0 when the gate passes. Pipeline check asset asserts this is 0
-- for production states.
create or replace function public.count_unattributed_beaches(p_state text default null)
returns table(state text, unattributed_count bigint, total_scoreable bigint)
language sql
stable
security definer
set search_path to 'public'
as $$
  select g.state,
         count(*) filter (where not coalesce(s.is_attributed, false)) as unattributed_count,
         count(*) as total_scoreable
    from public.beaches_gold g
    left join public.beach_governance_status s on s.fid = g.fid
   where g.is_active
     and g.is_scoreable
     and (p_state is null or g.state = p_state)
   group by g.state
   order by unattributed_count desc;
$$;

-- Function: returns the actual unattributed beaches for ops/debugging.
-- Use this when the gate fails to see what's missing.
create or replace function public.list_unattributed_beaches(p_state text default null)
returns table(fid bigint, state text, name text, lat double precision, lon double precision)
language sql
stable
security definer
set search_path to 'public'
as $$
  select g.fid, g.state,
         coalesce(g.display_name_override, g.name) as name,
         g.lat, g.lon
    from public.beaches_gold g
    left join public.beach_governance_status s on s.fid = g.fid
   where g.is_active
     and g.is_scoreable
     and not coalesce(s.is_attributed, false)
     and (p_state is null or g.state = p_state)
   order by g.state, g.name;
$$;
