-- 20260508_backfill_stranded_loser_bep.sql
--
-- Bug surfaced by the 50-row simulation: dedup losers in earlier tonight's
-- runs had BEP enrichment that NEVER got migrated to winners, because
-- run_late_stage_dedup didn't have BEP migration logic until later in
-- the session.
--
-- Sample finding: Muir Beach winner (POI 6268) had 0 dogs BEP, while
-- the loser (OSM 8539) had 3 dogs BEP rows (research, text_repass_v1,
-- zone_rules_derived_v1). All stranded.
--
-- This migration:
--   1. Backfills BEP from all dupe-killed losers to their winners
--      (ON CONFLICT DO NOTHING — preserves winner's existing rows)
--   2. Re-fires consensus + promote_canonical for affected winners so
--      the migrated evidence flows to beach_dog_policy / etc.
--
-- One-shot. Idempotent (re-runs would migrate nothing new).

begin;

-- 1. Migrate stranded BEP from losers to winners
with pairs as (
  select cast(substring(g.inactive_reason from 'dupe_of_(.*)') as bigint) as winner_fid,
         g.fid as loser_fid
    from public.beaches_gold g
   where g.inactive_reason like 'dupe_of_%'
)
insert into public.beach_enrichment_provenance
  (gold_fid, field_group, source, confidence, claimed_values, source_url,
   cpad_unit_name, extraction_type, cpad_role)
select p.winner_fid, bep.field_group, bep.source, bep.confidence,
       bep.claimed_values, bep.source_url,
       bep.cpad_unit_name, bep.extraction_type, bep.cpad_role
  from public.beach_enrichment_provenance bep
  join pairs p on p.loser_fid = bep.gold_fid
 where exists (select 1 from public.beaches_gold w
                where w.fid = p.winner_fid and w.is_active)
 on conflict (gold_fid, field_group, source) where gold_fid is not null
 do nothing;

-- 2. Re-fire consensus + promote for affected winners
do $$
declare f bigint;
begin
  for f in (
    select distinct cast(substring(g.inactive_reason from 'dupe_of_(.*)') as bigint)
      from public.beaches_gold g
     where g.inactive_reason like 'dupe_of_%'
       and exists (select 1 from public.beaches_gold w
                    where w.fid = cast(substring(g.inactive_reason from 'dupe_of_(.*)') as bigint)
                      and w.is_active)
  ) loop
    perform public.compute_beach_field_consensus(f);
    perform public.promote_canonical_to_consumer_tables(f);
  end loop;
end $$;

commit;
