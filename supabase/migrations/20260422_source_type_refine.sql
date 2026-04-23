-- Refine _classify_source_url to catch the .xx.us municipal-gov pattern
-- (e.g., ci.cannon-beach.or.us, www.coronado.ca.us, www.delmar.ca.us).
-- The original regex only matched .gov / .mil / gov.xx and missed this,
-- dropping ~50+ clearly municipal records into the 'commercial' bucket.
--
-- Domains still falling through to 'commercial' after this refinement
-- are legitimately ambiguous: tourism boards (visitnewportbeach.com),
-- municipal code publishers (ecode360.com, codepublishing.com), or
-- city sites on .net (lagunabeachcity.net). Per-domain overrides can
-- be added later if the badges end up driving re-extract ranking.
--
-- Generated STORED column needs to be dropped + re-added to recompute;
-- CREATE OR REPLACE FUNCTION alone doesn't re-evaluate existing rows.

create or replace function public._classify_source_url(url text)
returns text
language sql
immutable
as $$
  select case
    when url is null or url = '' then null
    -- Government: .gov, .mil, international gov.xx, US local gov .xx.us
    when url ~* '://[^/]*\.(gov|mil)(/|:|$)'       then 'official'
    when url ~* '://[^/]*\.gov\.[a-z]{2,3}(/|:|$)' then 'official'
    when url ~* '://[^/]*\.[a-z]{2}\.us(/|:|$)'    then 'official'
    -- User-generated / community platforms
    when url ~* '://[^/]*(yelp|tripadvisor|facebook|reddit|wikipedia|wikimedia|instagram|twitter|x\.com|tiktok|pinterest|alltrails|wikiloc|quora|medium|bringfido|rover|huskymutty)\.' then 'community'
    -- Nonprofit / educational
    when url ~* '://[^/]*\.(org|edu)(/|:|$)'       then 'nonprofit'
    else 'commercial'
  end;
$$;

-- Re-evaluate by dropping and re-adding the generated column.
alter table public.beaches               drop column if exists source_type;
alter table public.beaches_staging_new   drop column if exists source_type;

alter table public.beaches
  add column source_type text
  generated always as (public._classify_source_url(dogs_policy_source_url)) stored;

alter table public.beaches_staging_new
  add column source_type text
  generated always as (public._classify_source_url(dogs_policy_source_url)) stored;
