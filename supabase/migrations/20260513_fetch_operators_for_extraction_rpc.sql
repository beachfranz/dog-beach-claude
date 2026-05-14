-- 20260513_fetch_operators_for_extraction_rpc.sql
--
-- Helper RPC for extract_operator_dogs_policy.py. Replaces the script's
-- nested `subprocess.run(['supabase','db','query',...])` calls which
-- returned None stdout when invoked under Dagster's SubprocessResource
-- (Python 3.14 / Windows stdio quirk in double-nested subprocesses).
-- REST/PostgREST RPC has no such issue.

begin;

create or replace function public.fetch_operators_for_extraction(
  p_ids bigint[]
)
returns table(
  id             bigint,
  slug           text,
  canonical_name text,
  website        text,
  level          text,
  subtype        text,
  state_code     text,
  beach_count    integer
)
language sql
stable
security definer
set search_path to 'public', 'pg_catalog'
as $function$
  select op.id, op.slug, op.canonical_name, op.website, op.level, op.subtype, op.state_code,
         (select count(*)::int from public.beach_locations bl where bl.operator_id = op.id) as beach_count
    from public.operators op
   where op.id = any(p_ids)
   order by beach_count desc nulls last;
$function$;

grant execute on function public.fetch_operators_for_extraction(bigint[])
  to anon, authenticated, service_role;

-- Variant for the --limit / --counties path
create or replace function public.fetch_top_operators_for_extraction(
  p_limit    int default 100,
  p_counties text[] default null
)
returns table(
  id             bigint,
  slug           text,
  canonical_name text,
  website        text,
  level          text,
  subtype        text,
  state_code     text,
  beach_count    integer
)
language sql
stable
security definer
set search_path to 'public', 'pg_catalog'
as $function$
  with bl_scoped as (
    select bl.geom, bl.operator_id
      from public.beach_locations bl
      join public.counties c on st_intersects(c.geom, bl.geom)
     where p_counties is null or c.name = any(p_counties)
  )
  select op.id, op.slug, op.canonical_name, op.website, op.level, op.subtype, op.state_code,
         (select count(*)::int from bl_scoped where bl_scoped.operator_id = op.id) as beach_count
    from public.operators op
   where exists (select 1 from bl_scoped where bl_scoped.operator_id = op.id)
   order by (select count(*) from bl_scoped where bl_scoped.operator_id = op.id) desc nulls last
   limit p_limit;
$function$;

grant execute on function public.fetch_top_operators_for_extraction(int, text[])
  to anon, authenticated, service_role;

commit;
