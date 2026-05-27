-- 20260527_beach_discovery_queue.sql
--
-- Stage table for beaches discovered via state-park-system walkers.
-- Analog of public.dog_park_discovery_queue (2026-05-25).
--
-- Populated by beach_walk_state_parks (per-state handlers parse state-park
-- listing pages + per-park unit pages, extract named beach/swim areas).
-- Promoted by beach_ingest_queue to public.arena (and from there to
-- beaches_gold via existing promote_to_gold path).
--
-- Default scoring posture: rows promoted from this queue land with
-- scoring_tier='none' and is_scoreable=false. Manual is_scoreable=true
-- flip is required to start scoring — same pattern as bulk_promote_socal.py.

begin;

create table if not exists public.beach_discovery_queue (
  id                     bigserial primary key,
  state                  text not null,
  parent_park_name       text not null,    -- e.g., 'Bear Lake'
  parent_pad_us_unit_id  text,             -- PAD-US unit_id if matched via PIP
  parent_park_url        text,             -- e.g., 'https://stateparks.utah.gov/parks/bear-lake'
  beach_name             text not null,    -- e.g., 'Rendezvous Beach'
  verbatim_quote         text,             -- supporting cite from page
  source_url             text not null,    -- the page the quote came from
  source_handler         text not null,    -- e.g., 'ut_stateparks_v1'
  lat                    double precision, -- geocoded or PIP centroid
  lng                    double precision,
  geocode_method         text,             -- 'google_places' | 'pad_us_centroid' | 'manual'
  best_similarity        numeric,          -- vs existing arena entry
  best_match_fid         bigint,           -- arena.fid of closest existing match
  best_match_name        text,
  status                 text default 'pending'
    check (status in ('pending','ingested','duplicate','needs_review','rejected')),
  review_notes           text,
  queue_meta             jsonb default '{}'::jsonb,
  discovered_at          timestamptz default now(),
  updated_at             timestamptz default now()
);

create index if not exists beach_discovery_queue_status_idx on public.beach_discovery_queue (status);
create index if not exists beach_discovery_queue_state_idx  on public.beach_discovery_queue (state);
create unique index if not exists beach_discovery_queue_uniq
  on public.beach_discovery_queue (state, lower(parent_park_name), lower(beach_name), source_handler);

comment on table public.beach_discovery_queue is
  'Stage area for beaches discovered via state-park-system walkers. '
  'Analog of dog_park_discovery_queue. Promoted to arena+beaches_gold by '
  'beach_ingest_queue with scoring_tier=none + is_scoreable=false default.';

commit;
