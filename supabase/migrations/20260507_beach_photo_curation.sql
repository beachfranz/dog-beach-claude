-- 20260507_beach_photo_curation.sql
--
-- Curation columns on beach_photos + a per-beach status row tracking
-- when curation was completed. Used by curate-photos.html (admin tool
-- for human review of auto-loaded photos).

begin;

-- Match quality: human's verdict on how well the photo's source
-- (e.g., CCC access point name) matches the actual beach.
alter table public.beach_photos
  add column if not exists match_quality text
    check (match_quality is null or match_quality in
      ('exact','very_likely','somewhat_likely','unlikely','wtf')),
  add column if not exists curated_at timestamptz,
  add column if not exists curated_by text;

create index if not exists beach_photos_curated_idx
  on public.beach_photos(arena_group_id, curated_at);

-- Per-beach completion tracker
create table if not exists public.beach_curation_status (
  arena_group_id  bigint primary key references public.beaches_gold(fid) on delete cascade,
  curated_at      timestamptz default now(),
  curated_by      text,
  notes           text,
  match_quality   text
                  check (match_quality is null or match_quality in
                    ('exact','very_likely','somewhat_likely','unlikely','wtf'))
);

alter table public.beach_curation_status enable row level security;
-- Anon CAN read (so a public summary page could show what's curated);
-- writes go through service-role-keyed edge functions.
create policy beach_curation_status_anon_read on public.beach_curation_status
  for select to anon, authenticated using (true);

-- Progress RPC: returns county-level + beach-level state for the curator UI.
create or replace function public.get_curation_progress()
returns jsonb
language sql
stable
security definer
set search_path to 'public'
as $$
  with beaches as (
    select g.fid, coalesce(g.display_name_override, g.name) as name,
           g.county_name, g.state,
           (select count(*) from public.beach_photos bp
              where bp.arena_group_id = g.fid) as photo_count,
           (s.arena_group_id is not null) as is_curated
      from public.beaches_gold g
      left join public.beach_curation_status s on s.arena_group_id = g.fid
     where g.is_active and g.state = 'CA'
  ),
  -- One row per county with totals + per-beach details
  per_county as (
    select county_name,
           count(*) as total,
           count(*) filter (where photo_count > 0) as with_photos,
           count(*) filter (where is_curated)      as curated,
           jsonb_agg(jsonb_build_object(
             'fid',         fid,
             'name',        name,
             'photo_count', photo_count,
             'is_curated',  is_curated
           ) order by name) as beaches
      from beaches
     group by county_name
  )
  select jsonb_agg(jsonb_build_object(
    'county',      county_name,
    'total',       total,
    'with_photos', with_photos,
    'curated',     curated,
    'is_done',     curated >= with_photos and with_photos > 0,
    'beaches',     beaches
  ) order by county_name)
  from per_county
  where county_name is not null;
$$;

grant execute on function public.get_curation_progress() to anon, authenticated, service_role;

commit;
