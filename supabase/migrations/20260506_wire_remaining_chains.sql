-- 20260506_wire_remaining_chains.sql
--
-- Closes three more manual seams in the landing→consumer chain:
--
-- 1. Generic field-group promote chain trigger
--    Today's tg_after_change_promote_dogs_chain fires only for
--    field_group='dogs'. When BEP rows land for any OTHER field
--    group ('practical', 'amenities', etc), nothing auto-fires
--    consensus + canonical promote. This trigger catches them.
--
--    Calls compute_beach_field_consensus(fid) +
--    promote_canonical_to_consumer_tables(fid, 0.5) — the latter
--    is the pre-existing combined orchestrator that walks every
--    canonical field group and writes to its consumer table
--    (beach_dog_policy / beach_amenities / etc).
--
-- 3. Verdict cascade nightly
--    recompute_all_dogs_verdicts_by_origin() exists but only fires
--    by hand. Per CLAUDE.md it's a parity/reference pipeline — but
--    if it's load-bearing for any consumer surface, daily refresh
--    keeps it current with BEP changes that landed during the day.
--    pg_cron 03:00 UTC (after daily-beach-refresh, before users
--    start hitting the API at 06:00 PT).
--
-- 4. Polygon resolver weekly
--    refresh_beach_polygons() rebuilds the public.beach_polygons
--    canonical resolver output. OSM and CPAD polygons rarely move
--    so weekly is plenty (Sundays 04:00 UTC). Not on the daily
--    critical path.

begin;

-- ----------------------------------------------------------------------
-- 1. Generic field-group promote chain (non-dogs)
-- ----------------------------------------------------------------------
create or replace function public.tg_promote_other_fields_chain()
returns trigger
language plpgsql
as $$
begin
  if NEW.gold_fid is null then return NEW; end if;
  -- Anti-recursion: skip metadata-only updates (is_canonical flips, etc.)
  if TG_OP = 'UPDATE' and NEW.claimed_values is not distinct from OLD.claimed_values then
    return NEW;
  end if;

  -- Recompute consensus across ALL fields for this fid (compute is field-agnostic)
  perform public.compute_beach_field_consensus(NEW.gold_fid);
  -- Promote canonical to consumer tables (combined orchestrator handles
  -- every field group: dogs, practical, amenities, etc.)
  perform public.promote_canonical_to_consumer_tables(NEW.gold_fid, 0.5);

  return NEW;
end $$;

drop trigger if exists tg_after_change_promote_other_chain
  on public.beach_enrichment_provenance;

create trigger tg_after_change_promote_other_chain
  after insert or update on public.beach_enrichment_provenance
  for each row
  when (NEW.field_group <> 'dogs' and NEW.gold_fid is not null)
  execute function public.tg_promote_other_fields_chain();

-- ----------------------------------------------------------------------
-- 3. Verdict cascade nightly (03:00 UTC)
-- ----------------------------------------------------------------------
-- Idempotent: drops first, then schedules.
do $cron$
begin
  if exists (select 1 from cron.job where jobname = 'verdict_cascade_nightly') then
    perform cron.unschedule('verdict_cascade_nightly');
  end if;
  perform cron.schedule(
    'verdict_cascade_nightly',
    '0 3 * * *',
    $$ select public.recompute_all_dogs_verdicts_by_origin(); $$
  );
end $cron$;

-- ----------------------------------------------------------------------
-- 4. Polygon resolver weekly (Sunday 04:00 UTC)
-- ----------------------------------------------------------------------
do $cron$
begin
  if exists (select 1 from cron.job where jobname = 'beach_polygons_weekly') then
    perform cron.unschedule('beach_polygons_weekly');
  end if;
  perform cron.schedule(
    'beach_polygons_weekly',
    '0 4 * * 0',
    $$ select public.refresh_beach_polygons(); $$
  );
end $cron$;

commit;
