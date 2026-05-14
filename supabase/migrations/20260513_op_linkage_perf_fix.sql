-- 20260513_op_linkage_perf_fix.sql
--
-- _op_linkage timed out at 120s on OR with ~3.4K active operators. Root
-- cause: the join condition `ulm.lm_slug = os.op_slug OR ulm.lm_slug =
-- ANY(os.alias_slugs)` prevents the planner from picking a hash join.
-- It falls back to Nested Loop with per-pair `slugify_agency()` SubPlan
-- re-evaluation. For 3,424 operators × 2,205 loc_mangs = ~7.5M pairs,
-- each with ~3 alias slugify calls = 22M+ function calls.
--
-- Fix: flatten the alias-slugs into a separate row per operator, then
-- UNION with the primary-slug row. Now both branches are equi-joins
-- the planner can hash-join.

begin;

create or replace function public._op_linkage(p_state text)
returns integer language plpgsql as $function$
declare v_n int := 0;
begin
  with op_slugs as (
    select o.id, o.slug as op_slug,
           array(select public.slugify_agency(a) from unnest(o.aliases) a) as alias_slugs
      from public.operators o
     where o.state_code = p_state and o.is_active
  ),
  -- Flatten: one row per (operator_id, slug-variant). Combined size is
  -- bounded by total alias count, not pair-multiplication.
  all_op_slugs as (
    select id as operator_id, op_slug as slug from op_slugs
    union all
    select id as operator_id, als as slug
      from op_slugs, unnest(alias_slugs) als
       where als is not null and als <> ''
  ),
  unit_loc_mangs as (
    select distinct
      pu.loc_mang,
      public.slugify_agency(pu.loc_mang) as lm_slug
    from public.pad_us_units pu
    where pu.state = p_state
      and public._normalize_agency_text(pu.loc_mang) is not null
  ),
  matches as (
    -- Single equi-join now (hash-joinable). One operator may match
    -- multiple loc_mangs via primary slug + multiple alias slugs.
    select aos.operator_id,
           array_agg(distinct ulm.loc_mang) as new_strings
      from all_op_slugs aos
      join unit_loc_mangs ulm on ulm.lm_slug = aos.slug
     group by aos.operator_id
  ),
  upd as (
    update public.operators o
       set pad_us_mng_name = (
         select array_agg(distinct n)
           from unnest(
             coalesce(o.pad_us_mng_name, '{}'::text[]) || m.new_strings
           ) n
       ),
       updated_at = now()
      from matches m
     where o.id = m.operator_id
    returning 1
  )
  select count(*) into v_n from upd;
  return v_n;
end $function$;

commit;
