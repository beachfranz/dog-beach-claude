-- 20260510_beach_wikipedia.sql
--
-- Sidecar table for Wikipedia / Wikidata content per beach. Sourced
-- via the wikidata QID on the linked osm_landing row (osm tags carry
-- ~10% coverage across scoreable beaches; RI is an outlier at ~72%).
-- For each beach with a QID, we resolve the en.wikipedia sitelink and
-- pull the page summary from the Wikimedia REST API.
--
-- Used by: (a) future RAG pass in generate_beach_descriptions.py
-- (Wikipedia content as grounding context for the LLM); (b)
-- consumer surface "About this beach" panel if we want to expose
-- Wikipedia prose directly with attribution.

begin;

create table if not exists public.beach_wikipedia (
  arena_group_id  bigint primary key references public.beaches_gold(fid),
  qid             text not null,             -- e.g. 'Q12345' from Wikidata
  title           text,                      -- e.g. 'Doheny State Beach'
  url             text,                      -- canonical desktop URL
  summary         text,                      -- first paragraph (extract field)
  description     text,                      -- short Wikidata description
  thumbnail_url   text,                      -- lead image thumbnail if any
  thumbnail_w     int,
  thumbnail_h     int,
  coordinates     point,                     -- (lon, lat) per Wikidata
  match_method    text not null,             -- 'osm_wikidata' | 'osm_wikipedia' | 'name_search'
  match_dist_m    int,                       -- meters between beach and source feature
  raw             jsonb,                     -- full REST response for future use
  fetched_at      timestamptz not null default now(),
  refreshed_at    timestamptz
);

create index if not exists beach_wikipedia_qid_idx on public.beach_wikipedia (qid);

grant select on public.beach_wikipedia to anon, authenticated, service_role;

comment on table public.beach_wikipedia is
  'Wikipedia + Wikidata content per beach, sourced via OSM tag bridges. '
  'CC-BY-SA — attribution required on any consumer-surface display.';

commit;
