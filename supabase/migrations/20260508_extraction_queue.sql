-- 20260508_extraction_queue.sql
--
-- Surfaced by tonight's OR simulation: Cape Kiwanda has only 1 BEP row
-- (counties spatial) — never had LLM extraction run. The orchestrator
-- doesn't currently kick off extraction for newly-promoted beaches.
--
-- Adds:
--   1. beach_extraction_queue table (fid, queued_at, kind, attempts, last_error)
--   2. enqueue_missing_extractions() function — finds gold beaches without
--      dogs BEP (or stale extractions) and queues them
--   3. Wires into run_full_pipeline_maintenance as Step 5
--
-- The actual extraction worker is external Python (scripts/extract_for_beach.py)
-- which drains the queue. SQL side just enqueues; Python side processes.

begin;

create table if not exists public.beach_extraction_queue (
  id          bigserial primary key,
  fid         bigint not null references public.beaches_gold(fid) on delete cascade,
  kind        text   not null check (kind in ('dogs_extraction','park_url_discovery','full_refresh')),
  queued_at   timestamptz default now(),
  picked_at   timestamptz,
  completed_at timestamptz,
  attempts    int default 0,
  last_error  text,
  unique (fid, kind)
);

create index if not exists beach_extraction_queue_pending_idx
  on public.beach_extraction_queue(queued_at)
  where completed_at is null;

-- Enqueue any active gold beach that's missing extraction signal.
-- Called from run_full_pipeline_maintenance.
create or replace function public.enqueue_missing_extractions()
returns table(dogs_enqueued bigint, park_url_enqueued bigint, queue_total bigint)
language plpgsql
security definer
as $function$
declare
  v_dogs bigint := 0;
  v_purl bigint := 0;
  v_total bigint := 0;
begin
  -- Beaches with no dogs BEP at all (need LLM extraction)
  with applied as (
    insert into public.beach_extraction_queue (fid, kind)
    select g.fid, 'dogs_extraction'
      from public.beaches_gold g
     where g.is_active and g.is_scoreable
       and not exists (select 1 from public.beach_enrichment_provenance b
                        where b.gold_fid = g.fid and b.field_group = 'dogs')
       and not exists (select 1 from public.beach_extraction_queue q
                        where q.fid = g.fid and q.kind = 'dogs_extraction'
                          and q.completed_at is null)
    on conflict (fid, kind) do nothing
    returning 1
  )
  select count(*) into v_dogs from applied;

  -- Beaches with no park_url BEP and no manual park_url (URL discovery candidates)
  with applied as (
    insert into public.beach_extraction_queue (fid, kind)
    select g.fid, 'park_url_discovery'
      from public.beaches_gold g
     where g.is_active and g.is_scoreable
       and not exists (select 1 from public.beach_enrichment_provenance b
                        where b.gold_fid = g.fid
                          and b.source in ('park_url','park_url_buffer_attribution'))
       and not exists (select 1 from public.beach_extraction_queue q
                        where q.fid = g.fid and q.kind = 'park_url_discovery'
                          and q.completed_at is null)
    on conflict (fid, kind) do nothing
    returning 1
  )
  select count(*) into v_purl from applied;

  select count(*) into v_total
    from public.beach_extraction_queue
   where completed_at is null;

  return query select v_dogs, v_purl, v_total;
end;
$function$;

grant execute on function public.enqueue_missing_extractions() to anon, authenticated, service_role;

-- Update run_full_pipeline_maintenance to include extraction queueing
create or replace function public.run_full_pipeline_maintenance(p_state text default 'CA')
returns table(
  poi_promoted_to_arena bigint,
  osm_promoted_to_arena bigint,
  names_refreshed bigint,
  arena_grouped bigint,
  arena_extras_grouped bigint,
  promoted_to_gold bigint,
  dedup_kills bigint,
  bep_rows_migrated bigint,
  queue_processed bigint,
  bep_missing_refired bigint,
  extractions_dogs_enqueued bigint,
  extractions_park_url_enqueued bigint,
  extraction_queue_pending bigint,
  ran_at timestamptz
)
language plpgsql
security definer
as $function$
declare
  v_poi_to_arena bigint := 0;
  v_osm_to_arena bigint := 0;
  v_names_ref    bigint := 0;
  v_missing_bep  bigint := 0;
  v_dogs_eq bigint := 0;
  v_purl_eq bigint := 0;
  v_q_total bigint := 0;
  v_pf_rec record;
  v_pi_rec record;
  v_po_rec record;
  v_nr_rec record;
  v_mb_rec record;
  v_eq_rec record;
begin
  select * into v_pi_rec from public.promote_poi_landing_to_arena();
  v_poi_to_arena := coalesce(v_pi_rec.promoted, 0);

  select * into v_po_rec from public.promote_osm_landing_to_arena();
  v_osm_to_arena := coalesce(v_po_rec.promoted, 0);

  select * into v_nr_rec from public.refresh_arena_names_from_osm_landing();
  v_names_ref := coalesce(v_nr_rec.arena_rows_updated, 0);

  select * into v_mb_rec from public.refire_missing_bep(150);
  v_missing_bep := coalesce(v_mb_rec.fids_refired, 0);

  select * into v_pf_rec from public.run_pipeline_for_state(p_state);

  -- ★ NEW: enqueue extraction work for any beach missing it
  select * into v_eq_rec from public.enqueue_missing_extractions();
  v_dogs_eq := coalesce(v_eq_rec.dogs_enqueued, 0);
  v_purl_eq := coalesce(v_eq_rec.park_url_enqueued, 0);
  v_q_total := coalesce(v_eq_rec.queue_total, 0);

  return query select
    v_poi_to_arena, v_osm_to_arena, v_names_ref,
    coalesce(v_pf_rec.arena_grouped, 0),
    coalesce(v_pf_rec.arena_extras_grouped, 0),
    coalesce(v_pf_rec.promoted, 0),
    coalesce(v_pf_rec.dedup_kills, 0),
    coalesce(v_pf_rec.bep_rows_migrated, 0),
    coalesce(v_pf_rec.queue_processed, 0),
    v_missing_bep,
    v_dogs_eq, v_purl_eq, v_q_total,
    now();
end;
$function$;

commit;
