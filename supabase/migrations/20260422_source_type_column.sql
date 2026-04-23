-- Add source_type badge column to beaches and beaches_staging_new.
--
-- Classification is derived from dogs_policy_source_url via a generated
-- STORED column — so it stays in sync automatically as the URL changes,
-- with no backfill process or trigger needed. Adding the column computes
-- the value for every existing row in one ALTER TABLE.
--
-- Buckets:
--   official   — .gov, .mil, gov.xx (international gov)
--   nonprofit  — .org (excluding the community sites listed below)
--   community  — user-generated platforms (yelp, wikipedia, reddit, etc.)
--   commercial — everything else (.com, blogs, businesses)
--   null       — no dogs_policy_source_url set
--
-- Match order matters: .gov is checked before .org (for .gov.org edge
-- cases that shouldn't exist), and community patterns are checked before
-- the generic .org fallback so nonprofit-platform domains don't get
-- mis-tagged as nonprofit.

create or replace function public._classify_source_url(url text)
returns text
language sql
immutable
as $$
  select case
    when url is null or url = '' then null
    -- Government (.gov, .mil, and international gov.xx like gov.uk, gov.ca)
    when url ~* '://[^/]*\.(gov|mil)(/|:|$)'       then 'official'
    when url ~* '://[^/]*\.gov\.[a-z]{2,3}(/|:|$)' then 'official'
    -- User-generated / community platforms (checked before .org so
    -- e.g. wikipedia.org lands here, not in nonprofit)
    when url ~* '://[^/]*(yelp|tripadvisor|facebook|reddit|wikipedia|wikimedia|instagram|twitter|x\.com|tiktok|pinterest|alltrails|wikiloc|quora|medium)\.' then 'community'
    -- Nonprofit / educational
    when url ~* '://[^/]*\.(org|edu)(/|:|$)'       then 'nonprofit'
    -- Everything else (commercial .com, blogs, misc)
    else 'commercial'
  end;
$$;

alter table public.beaches
  add column if not exists source_type text
  generated always as (public._classify_source_url(dogs_policy_source_url)) stored;

alter table public.beaches_staging_new
  add column if not exists source_type text
  generated always as (public._classify_source_url(dogs_policy_source_url)) stored;
