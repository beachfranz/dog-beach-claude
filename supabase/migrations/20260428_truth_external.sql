-- truth_external: independent third-party dog-beach data scraped from
-- public directories for COMPARISON ONLY. Never merged into the model.
-- Used to identify rows where our model disagrees with 2+ external
-- sources (likely error) vs rows where externals also disagree
-- (genuinely ambiguous in the wild).

create table if not exists public.truth_external (
  source         text   not null check (source in ('bringfido','californiabeaches','dogtrekker','gopetfriendly')),
  source_id      text   not null,    -- natural ID from the source (e.g., '206' for BringFido)
  source_url     text   not null,
  name           text,
  city           text,
  state          text   default 'CA',

  lat            numeric,
  lng            numeric,
  geom           geometry(Point, 4326),

  -- Dog policy as stated by the source. The source's own classification.
  dogs_rule      text   check (dogs_rule in ('yes','no','leash','off_leash','mixed','unknown')),
  raw_dog_text   text,                -- the source text the rule was extracted from

  address        text,
  hours_text     text,
  description    text,                -- prose body of the listing

  -- Match to our internal beach inventory, populated later by matcher
  matched_origin_key text,            -- e.g., 'ccc/1234' or 'osm/way/567'
  match_method       text,            -- 'name+proximity' / 'manual' / null
  match_score        numeric,

  scraped_at     timestamptz not null default now(),
  primary key (source, source_id)
);

create index if not exists truth_external_geom_gix on public.truth_external using gist (geom);
create index if not exists truth_external_name_trgm on public.truth_external using gin (name gin_trgm_ops);
create index if not exists truth_external_match_idx on public.truth_external (matched_origin_key);
