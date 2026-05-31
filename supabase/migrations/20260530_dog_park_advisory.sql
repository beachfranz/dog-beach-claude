-- dog_park_advisory — parallel to beach_advisory. Used by
-- compute_dog_park_advisories.py to surface forecast-driven safety
-- pills on dog-park.html. Mirrors beach_advisory's shape so the
-- consumer-side renderer can be the same code path (just keyed off
-- dog_park_fid instead of beach_fid).
--
-- Initial use: car_heat_status + car_cold_status (ASPCA-aligned).
-- Future: UV, hot asphalt (parking lot), feels_like, etc.

begin;

create table if not exists public.dog_park_advisory (
  id              bigserial primary key,
  dog_park_fid    bigint not null references public.dog_parks_gold(fid) on delete cascade,
  advisory_key    text   not null,
  source          text   not null,
  event_type      text   not null,
  severity        text   not null,
  valid_from      timestamptz not null,
  valid_to        timestamptz not null,
  dog_impact_class text,
  dog_impact_text text,
  translation_source text,
  translation_confidence numeric,
  label           text,
  value           text,
  icon            text,
  raw_data        jsonb,
  fetched_at      timestamptz not null default now(),
  created_at      timestamptz not null default now(),
  constraint dog_park_advisory_key_uq unique (dog_park_fid, advisory_key)
);

create index if not exists dog_park_advisory_fid_idx
  on public.dog_park_advisory (dog_park_fid);
create index if not exists dog_park_advisory_valid_idx
  on public.dog_park_advisory (valid_to);

-- RLS: anon SELECT (matches beach_advisory pattern)
alter table public.dog_park_advisory enable row level security;

drop policy if exists dog_park_advisory_anon_select on public.dog_park_advisory;
create policy dog_park_advisory_anon_select
  on public.dog_park_advisory for select to anon using (true);

drop policy if exists dog_park_advisory_authed_select on public.dog_park_advisory;
create policy dog_park_advisory_authed_select
  on public.dog_park_advisory for select to authenticated using (true);

comment on table public.dog_park_advisory is
  'Forecast-driven safety advisories per dog park. Mirrors beach_advisory.';

commit;
