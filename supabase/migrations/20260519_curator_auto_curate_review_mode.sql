-- 20260519_curator_auto_curate_review_mode.sql
--
-- Per docs/curator_auto_curate_mode_spec.md.
-- Backs the curator's auto-curate review mode added to curate.html.
--
-- Adds:
--   beach_curator_review_log table (audit + comment)
--   4 RPCs:
--     get_auto_curated_beach_fids(state) → fids w/ ≥1 auto-curated photo
--     approve_auto_curated_beach(fid, curator, comment)
--     clear_auto_curated_beach(fid, curator, comment)
--     approve_auto_curated_state(state, curator, comment, expected_fid_count)
--
-- Design choices (Franz 2026-05-19):
--   a) global filter (mode dropdown switches the whole UI)
--   b) approve actions BOTH per-beach AND bulk
--   c) clear sets curated_by=NULL (no tombstone — see spec)
--   - Optional comment field next to Approve/Clear buttons
--   - Audit log captures action + curator + comment + n_photos

begin;

-- ────────────────────────────────────────────────────────────────────
-- Audit log
-- ────────────────────────────────────────────────────────────────────

create table if not exists public.beach_curator_review_log (
  id           bigserial primary key,
  beach_fid    bigint    not null references public.beaches_gold(fid) on delete cascade,
  curator      text      not null,
  action       text      not null check (action in ('approve','clear','override','bulk_approve')),
  auto_marker  text,                          -- e.g., 'auto:n=6' captured at review time
  n_photos     int,                           -- count of auto rows touched
  comment      text      check (char_length(comment) <= 500),
  created_at   timestamptz not null default now()
);

create index if not exists beach_curator_review_log_beach_idx
  on public.beach_curator_review_log (beach_fid, created_at desc);
create index if not exists beach_curator_review_log_curator_idx
  on public.beach_curator_review_log (curator, created_at desc);

comment on table public.beach_curator_review_log is
  'Audit log for curator actions in auto-curate review mode. One row per '
  'per-beach Approve / Clear, and one row per beach inside a bulk-approve. '
  'Comment is optional curator note (≤500 chars). See '
  'docs/curator_auto_curate_mode_spec.md.';


-- ────────────────────────────────────────────────────────────────────
-- get_auto_curated_beach_fids(state) → table of (fid, name, county, ...)
-- ────────────────────────────────────────────────────────────────────
-- Drives the curator's beach picker in auto-curate review mode.
-- p_state NULL → all states (used by the curator UI on mode switch to
-- pre-fetch the global set; cascade filters state/county pickers
-- client-side from this one fetch).

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
         g.county                                as county,
         count(*)::int                           as n_auto_photos,
         max(bp.curated_by)                      as auto_marker
    from public.beaches_gold g
    join public.beach_photos bp on bp.arena_group_id = g.fid
   where (p_state is null or g.state = upper(p_state))
     and g.is_active
     and bp.curated_by like 'auto:%'
   group by g.fid, g.name, g.display_name_override, g.state, g.county
   order by g.state, g.county nulls last, beach_name
$$;

grant execute on function public.get_auto_curated_beach_fids(text)
  to anon, authenticated, service_role;


-- ────────────────────────────────────────────────────────────────────
-- approve_auto_curated_beach(fid, curator, comment) → int (n_photos)
-- ────────────────────────────────────────────────────────────────────
-- Flips curated_by from 'auto:%' to the curator's name on every
-- auto-curated photo for the beach. Logs the action. Returns n_photos
-- updated (caller surfaces this in the UI).

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
  -- Capture the auto marker BEFORE we overwrite it
  select max(curated_by) into v_marker
    from public.beach_photos
   where arena_group_id = p_beach_fid
     and curated_by like 'auto:%';
  update public.beach_photos
     set curated_by = p_curator
   where arena_group_id = p_beach_fid
     and curated_by like 'auto:%';
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


-- ────────────────────────────────────────────────────────────────────
-- clear_auto_curated_beach(fid, curator, comment) → int (n_photos)
-- ────────────────────────────────────────────────────────────────────
-- Sets curated_by=NULL, curated_at=NULL on every auto-curated photo
-- for the beach so a future auto_curate.py run re-picks. No tombstone
-- (see spec — future runs may use larger n).

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
     and curated_by like 'auto:%';
  update public.beach_photos
     set curated_by = null,
         curated_at = null
   where arena_group_id = p_beach_fid
     and curated_by like 'auto:%';
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


-- ────────────────────────────────────────────────────────────────────
-- approve_auto_curated_state(state, curator, comment, expected_fid_count)
--   → int (total photos updated across all approved beaches)
-- ────────────────────────────────────────────────────────────────────
-- Bulk: Approve every auto-curated beach in <state>. Caller passes
-- expected_fid_count from a fresh get_auto_curated_beach_fids preview;
-- RPC refuses if actual count != expected (avoids race surprises if
-- another curator approved one between preview and click).

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
       and curated_by like 'auto:%';
    update public.beach_photos
       set curated_by = p_curator
     where arena_group_id = v_fid
       and curated_by like 'auto:%';
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
