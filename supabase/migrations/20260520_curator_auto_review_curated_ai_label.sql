-- 20260520_curator_auto_review_curated_ai_label.sql
--
-- Migrate the 4 curator-review RPCs to the new label taxonomy
-- (Franz 2026-05-20):
--   'auto:%'     → 'Curated:AI'        (replaced by relabel sweep earlier)
--   approve flip → 'Curated:Human'     (was per-user, now canonical)
--
-- Transition compatibility: filters still match 'auto:%' so any
-- legacy/leaked rows still surface. New auto_curate.py writes 'Curated:AI'.

begin;

create or replace function public.get_auto_curated_beach_fids(
  p_state text default null
) returns table (
  beach_fid     bigint,
  beach_name    text,
  state         text,
  county        text,
  n_auto_photos int,
  auto_marker   text
)
language sql stable parallel safe
set search_path = 'public'
as $$
  select g.fid                                   as beach_fid,
         coalesce(g.display_name_override, g.name) as beach_name,
         g.state                                 as state,
         g.county_name                           as county,
         count(*)::int                           as n_auto_photos,
         max(bp.curated_by)                      as auto_marker
    from public.beaches_gold g
    join public.beach_photos bp on bp.arena_group_id = g.fid
   where (p_state is null or g.state = upper(p_state))
     and g.is_active
     and (bp.curated_by = 'Curated:AI' or bp.curated_by like 'auto:%')
   group by g.fid, g.name, g.display_name_override, g.state, g.county_name
   order by g.state, g.county_name nulls last, beach_name
$$;

grant execute on function public.get_auto_curated_beach_fids(text)
  to anon, authenticated, service_role;


create or replace function public.approve_auto_curated_beach(
  p_beach_fid bigint,
  p_curator   text,
  p_comment   text default null
) returns int
language plpgsql
security definer
set search_path = 'public', 'pg_catalog'
as $$
declare
  v_marker text;
  v_n      int;
begin
  if p_curator is null or length(trim(p_curator)) = 0 then
    raise exception 'approve_auto_curated_beach: p_curator required';
  end if;
  select max(curated_by) into v_marker
    from public.beach_photos
   where arena_group_id = p_beach_fid
     and (curated_by = 'Curated:AI' or curated_by like 'auto:%');
  update public.beach_photos
     set curated_by = 'Curated:Human'
   where arena_group_id = p_beach_fid
     and (curated_by = 'Curated:AI' or curated_by like 'auto:%');
  get diagnostics v_n = row_count;
  if v_n > 0 then
    insert into public.beach_curator_review_log
      (beach_fid, curator, action, auto_marker, n_photos, comment)
    values (p_beach_fid, p_curator, 'approve', v_marker, v_n, p_comment);
  end if;
  return v_n;
end $$;

grant execute on function public.approve_auto_curated_beach(bigint, text, text)
  to anon, authenticated, service_role;


create or replace function public.clear_auto_curated_beach(
  p_beach_fid bigint,
  p_curator   text,
  p_comment   text default null
) returns int
language plpgsql
security definer
set search_path = 'public', 'pg_catalog'
as $$
declare
  v_marker text;
  v_n      int;
begin
  if p_curator is null or length(trim(p_curator)) = 0 then
    raise exception 'clear_auto_curated_beach: p_curator required';
  end if;
  select max(curated_by) into v_marker
    from public.beach_photos
   where arena_group_id = p_beach_fid
     and (curated_by = 'Curated:AI' or curated_by like 'auto:%');
  update public.beach_photos
     set curated_by = null,
         curated_at = null
   where arena_group_id = p_beach_fid
     and (curated_by = 'Curated:AI' or curated_by like 'auto:%');
  get diagnostics v_n = row_count;
  if v_n > 0 then
    insert into public.beach_curator_review_log
      (beach_fid, curator, action, auto_marker, n_photos, comment)
    values (p_beach_fid, p_curator, 'clear', v_marker, v_n, p_comment);
  end if;
  return v_n;
end $$;

grant execute on function public.clear_auto_curated_beach(bigint, text, text)
  to anon, authenticated, service_role;


create or replace function public.approve_auto_curated_state(
  p_state              text,
  p_curator            text,
  p_comment            text default null,
  p_expected_fid_count int  default null
) returns int
language plpgsql
security definer
set search_path = 'public', 'pg_catalog'
as $$
declare
  v_fids        bigint[];
  v_fid         bigint;
  v_total_rows  int := 0;
  v_per_beach   int;
  v_marker      text;
begin
  if p_curator is null or length(trim(p_curator)) = 0 then
    raise exception 'approve_auto_curated_state: p_curator required';
  end if;

  select array_agg(beach_fid order by beach_fid)
    into v_fids
    from public.get_auto_curated_beach_fids(p_state);

  if v_fids is null then v_fids := '{}'::bigint[]; end if;

  if p_expected_fid_count is not null
     and p_expected_fid_count <> array_length(v_fids, 1) then
    raise exception 'approve_auto_curated_state: expected_fid_count=% but found % auto-curated beaches in %; refresh and retry',
      p_expected_fid_count, coalesce(array_length(v_fids, 1), 0), p_state;
  end if;

  foreach v_fid in array v_fids loop
    select max(curated_by) into v_marker
      from public.beach_photos
     where arena_group_id = v_fid
       and (curated_by = 'Curated:AI' or curated_by like 'auto:%');
    update public.beach_photos
       set curated_by = 'Curated:Human'
     where arena_group_id = v_fid
       and (curated_by = 'Curated:AI' or curated_by like 'auto:%');
    get diagnostics v_per_beach = row_count;
    v_total_rows := v_total_rows + v_per_beach;
    if v_per_beach > 0 then
      insert into public.beach_curator_review_log
        (beach_fid, curator, action, auto_marker, n_photos, comment)
      values (v_fid, p_curator, 'bulk_approve', v_marker, v_per_beach, p_comment);
    end if;
  end loop;

  return v_total_rows;
end $$;

grant execute on function public.approve_auto_curated_state(text, text, text, int)
  to anon, authenticated, service_role;

commit;
