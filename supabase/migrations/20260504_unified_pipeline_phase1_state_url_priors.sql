-- 20260504_unified_pipeline_phase1_state_url_priors.sql
--
-- Per-state authoritative-domain priors (URL discovery remediation #2,
-- per project_url_discovery_scope.md). Today's AUTH_DOMAINS dict in
-- discover_urls.py + extract_for_beach.py is hardcoded with CA domains.
-- For OR/WA, oregonstateparks.org and parks.state.wa.us would score 1
-- (default) when they should be 4 (state authority).
--
-- This table holds per-(state, domain) authority scores. Lookup is
-- suffix match (mirrors current behavior).

begin;

create table if not exists public.state_url_priors (
  state_code      text not null,
  domain          text not null,        -- suffix match (e.g. 'parks.ca.gov')
  authority_score integer not null check (authority_score between 0 and 4),
  notes           text,
  created_at      timestamptz default now(),
  primary key (state_code, domain)
);

comment on table public.state_url_priors is
  'Per-state authoritative domain priors for URL discovery + extraction. '
  'Suffix match on hostname (e.g. domain="parks.ca.gov" matches '
  '"www.parks.ca.gov"). Used by discover_urls.py and extract_for_beach.py '
  'to score URLs 0-4 based on state-specific authority. Universal fallbacks '
  '(.gov=3, .us=3, default=1) live in code, not this table.';

-- Universal entry: '*' (any state) — domains that always score the same
-- regardless of state. Used as the lookup fallback before code defaults.
insert into public.state_url_priors (state_code, domain, authority_score, notes) values
  ('*', 'nps.gov',           4, 'National Park Service — federal authority'),
  ('*', 'noaa.gov',           4, 'National Oceanic and Atmospheric Administration'),
  ('*', 'wikipedia.org',      2, 'Wikipedia — generally good but not primary'),
  ('*', 'nature.org',          3, 'Nature Conservancy'),
  ('*', 'savetheredwoods.org', 3, 'Save the Redwoods League'),
  ('*', 'bringfido.com',      1, 'Aggregator'),
  ('*', 'dogtrekker.com',     1, 'Aggregator'),
  ('*', 'alltrails.com',      1, 'Aggregator'),
  ('*', 'yelp.com',           0, 'User reviews — unreliable for policy'),
  ('*', 'tripadvisor.com',    0, 'User reviews — unreliable for policy')
on conflict (state_code, domain) do nothing;

-- CA priors (mirror current hardcoded AUTH_DOMAINS)
insert into public.state_url_priors (state_code, domain, authority_score, notes) values
  ('CA', 'parks.ca.gov',          4, 'California Department of Parks and Recreation'),
  ('CA', 'ca.gov',                3, 'California state government (generic)'),
  ('CA', 'wildlife.ca.gov',       3, 'California Department of Fish and Wildlife'),
  ('CA', 'sandiego.gov',          4, 'City of San Diego'),
  ('CA', 'longbeach.gov',         4, 'City of Long Beach'),
  ('CA', 'newportbeachca.gov',    4, 'City of Newport Beach'),
  ('CA', 'smgov.net',             4, 'City of Santa Monica'),
  ('CA', 'hermosabeach.gov',      4, 'City of Hermosa Beach'),
  ('CA', 'ventura.org',           4, 'City of Ventura'),
  ('CA', 'lacounty.gov',          3, 'Los Angeles County'),
  ('CA', 'ocparks.com',           3, 'OC Parks (Orange County)'),
  ('CA', 'danapoint.org',         3, 'City of Dana Point'),
  ('CA', 'delmar.ca.us',          4, 'City of Del Mar'),
  ('CA', 'coronado.ca.us',        4, 'City of Coronado'),
  ('CA', 'californiabeaches.com', 2, 'CA-specific aggregator (better than generic)'),
  ('CA', 'movingtolagunabeach.com', 1, 'Niche aggregator')
on conflict (state_code, domain) do nothing;

-- OR priors (state authority + key cities + agencies)
insert into public.state_url_priors (state_code, domain, authority_score, notes) values
  ('OR', 'oregonstateparks.org',  4, 'Oregon State Parks'),
  ('OR', 'oregon.gov',            3, 'Oregon state government'),
  ('OR', 'parks.oregon.gov',      4, 'Oregon Parks subdomain'),
  ('OR', 'stateparks.oregon.gov', 4, 'Oregon State Parks alt'),
  ('OR', 'oregonmetro.gov',       3, 'Portland Metro'),
  ('OR', 'portlandoregon.gov',    4, 'City of Portland'),
  ('OR', 'cannonbeach.org',       4, 'City of Cannon Beach'),
  ('OR', 'cityofcannonbeach.org', 4, 'City of Cannon Beach alt'),
  ('OR', 'visittheoregoncoast.com', 2, 'Tourism / aggregator'),
  ('OR', 'travelportland.com',    2, 'Tourism / aggregator'),
  ('OR', 'oregonlive.com',        1, 'News'),
  ('OR', 'oprd.oregon.gov',       4, 'Oregon Parks and Recreation Department')
on conflict (state_code, domain) do nothing;

-- WA priors (placeholder; expand when WA expansion happens)
insert into public.state_url_priors (state_code, domain, authority_score, notes) values
  ('WA', 'parks.state.wa.us',     4, 'Washington State Parks'),
  ('WA', 'wa.gov',                3, 'Washington state government'),
  ('WA', 'dnr.wa.gov',            4, 'Washington Department of Natural Resources'),
  ('WA', 'parks.wa.gov',          4, 'Washington Parks alt'),
  ('WA', 'visitseattle.org',      2, 'Tourism'),
  ('WA', 'seattle.gov',           4, 'City of Seattle')
on conflict (state_code, domain) do nothing;

-- DB lookup function (suffix match, prefer most-specific = longest match;
-- prefer state-specific over universal '*'). Falls back to .gov=3, .us=3,
-- else 1.
create or replace function public._state_authority_score(p_state text, p_url text)
returns integer language plpgsql immutable as $$
declare
  host text;
  result int;
begin
  if p_url is null then return 1; end if;
  host := lower(coalesce(
    (regexp_match(p_url, '^[a-z]+://(?:www\.)?([^/?#]+)'))[1],
    p_url
  ));
  if host is null then return 1; end if;

  -- Prefer state-specific match, then '*', longest domain wins
  select authority_score into result
    from public.state_url_priors
   where state_code in (p_state, '*')
     and host like '%' || domain
   order by case when state_code = p_state then 0 else 1 end,
            length(domain) desc
   limit 1;
  if result is not null then return result; end if;

  -- Universal fallbacks
  if host like '%.gov'   then return 3; end if;
  if host like '%.ca.us' then return 3; end if;
  return 1;
end;
$$;

comment on function public._state_authority_score(text, text) is
  'Per-state URL authority score (0-4). Suffix match on host; prefers '
  'state-specific entries over universal (state_code=''*'') ones; longest '
  'domain match wins. Fallback: .gov/.ca.us=3, default=1. Used by '
  'discover_urls.py and extract_for_beach.py.';

commit;
