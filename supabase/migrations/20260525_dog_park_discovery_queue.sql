-- 20260525_dog_park_discovery_queue.sql
--
-- Phase DP-W4: capture catalog-walker discoveries that DON'T match any
-- existing dog_parks_gold row, so they feed an inventory-ingest pipeline.
--
-- The catalog-walker (walk_dog_park_operator_catalog.py) found that SF's
-- operator catalog lists ~31 dog parks but our gold table has only 16 SF
-- entries. The 15+ unmatched entries are real famous SF dog parks (Alamo
-- Square, Alta Plaza, Balboa, Bernal Heights, Lafayette, Mission Dolores,
-- etc.) that OSM ingest missed.
--
-- This queue captures every unmatched (and low-confidence-matched) catalog
-- entry. A future inventory pipeline consumes pending rows to create new
-- dog_parks_gold rows (after human review or automated triangulation).

begin;

create table if not exists public.dog_park_discovery_queue (
  id                    bigserial primary key,
  operator_id           bigint references public.operator(id),
  operator_name         text,                              -- snapshot for human review
  catalog_park_name     text not null,
  catalog_park_url      text,
  blurb                 text,
  catalog_source_url    text not null,                     -- the catalog page we walked
  best_similarity       numeric,                           -- 0.0-1.0 best fuzzy match to existing gold
  best_match_fid        bigint references public.dog_parks_gold(fid),
  best_match_name       text,                              -- snapshot of best match name
  status                text not null default 'pending'
                        check (status in ('pending','ingested','duplicate','rejected','needs_review')),
  review_notes          text,
  discovered_at         timestamptz not null default now(),
  updated_at            timestamptz not null default now(),
  unique (operator_id, catalog_park_url) deferrable initially deferred
);

create index if not exists dpdq_operator_id_idx     on public.dog_park_discovery_queue (operator_id);
create index if not exists dpdq_status_idx          on public.dog_park_discovery_queue (status);
create index if not exists dpdq_best_similarity_idx on public.dog_park_discovery_queue (best_similarity)
  where status = 'pending';

comment on table public.dog_park_discovery_queue is
  'Catalog-walker discoveries that did NOT match an existing dog_parks_gold row. Feeds an inventory-ingest pipeline (future) so missing dog parks (e.g., SF Alamo Square, Lafayette, Mission Dolores) can be added to the gold catalog. Walker writes pending rows; human review or automated triangulation flips status to ingested/duplicate/rejected.';

comment on column public.dog_park_discovery_queue.best_similarity is
  'Best fuzzy-match similarity score (0-1) against existing dog_parks_gold rows for the operator. <0.6 = no close match (true new park). 0.6-0.8 = possible name variation, needs review. >=0.8 should have matched and been extracted directly.';

alter table public.dog_park_discovery_queue enable row level security;
create policy "dpdq_service_role_all"
  on public.dog_park_discovery_queue
  for all using (auth.role() = 'service_role');

commit;
