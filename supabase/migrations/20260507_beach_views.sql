-- 20260507_beach_views.sql
--
-- Anonymous page-view tracking. Feeds the long-tail tier in the daily-
-- refresh strategy: high-traffic beaches get full nightly refresh,
-- never-viewed beaches get on-demand refresh.
--
-- Privacy: no PII. client_id is a browser-generated random token
-- (localStorage), expires/rotates ~yearly. Source distinguishes index /
-- detail / find / direct.
--
-- Storage: raw events in beach_views; archived/aggregated by a nightly
-- job into beach_view_summary (separate migration when needed).

begin;

create table if not exists public.beach_views (
  id            bigserial primary key,
  arena_group_id bigint not null
    references public.beaches_gold(fid) on delete cascade,
  viewed_at     timestamptz not null default now(),
  client_id     text,                                  -- nullable; not all clients pass one
  source        text                                   -- 'index' | 'detail' | 'find' | 'api'
    check (source is null or source in ('index','detail','find','api','admin'))
);

create index if not exists beach_views_fid_time_idx
  on public.beach_views (arena_group_id, viewed_at desc);
create index if not exists beach_views_time_idx
  on public.beach_views (viewed_at desc);

-- Rolling per-beach summary view — backed by a query, not a materialized
-- view, so it's always current. Cheap because the index above is
-- (arena_group_id, viewed_at desc).
create or replace view public.beach_view_summary as
  select
    bg.fid                                            as arena_group_id,
    bg.location_id,
    bg.name,
    coalesce(views.last_viewed_at, '1970-01-01'::timestamptz) as last_viewed_at,
    coalesce(views.views_24h,  0)                    as views_24h,
    coalesce(views.views_7d,   0)                    as views_7d,
    coalesce(views.views_30d,  0)                    as views_30d
  from public.beaches_gold bg
  left join lateral (
    select
      max(viewed_at) as last_viewed_at,
      count(*) filter (where viewed_at >= now() - interval '24 hours') as views_24h,
      count(*) filter (where viewed_at >= now() - interval '7 days')   as views_7d,
      count(*) filter (where viewed_at >= now() - interval '30 days')  as views_30d
    from public.beach_views bv
    where bv.arena_group_id = bg.fid
  ) views on true
  where bg.is_active;

-- RLS: anon can INSERT (writes from the browser via the log-beach-view
-- edge function which uses service role, but enable RLS for safety).
-- SELECT on raw rows is service-role-only; SELECT on the summary view is
-- safe to expose.
alter table public.beach_views enable row level security;

-- No anon policies on the raw table — only service role writes from
-- log-beach-view. The summary view inherits the underlying table's RLS
-- when called by anon, so anon SELECT on the view returns 0 rows.
-- Admin tools using the service key see everything.

grant select on public.beach_view_summary to anon, authenticated;

commit;
