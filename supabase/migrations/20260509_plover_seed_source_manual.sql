-- 20260509_plover_seed_source_manual.sql
--
-- Fix the plover-season seed so it survives the consensus resolver.
-- The 20260508_seed_plover_closures.sql migration set
-- dogs_prohibited_start/end on matched beaches but did NOT update
-- beach_dog_policy.source. As a result, promote_canonical_to_consumer_tables
-- saw the row's source='auto_promoted_from_consensus' and rewrote the
-- season fields back to NULL during the next refire_bep_cascade.
--
-- Setting source='manual' tells the resolver to leave the row alone.
-- This re-applies the season for the matched named plover beaches and
-- pins the source so it persists.

begin;

-- Re-apply season + pin source to manual on the canonical-named plover beaches.
-- We re-derive the matched fids from name/state without the temp table, so this
-- is idempotent and re-runnable.
with named as (
  select g.fid
    from public.beaches_gold g
   where g.is_active
     and (
       (g.state = 'OR' and (
            g.name ilike '%Sutton%' or g.name ilike '%Siltcoos%' or g.name ilike '%Tahkenitch%'
         or g.name ilike '%Tenmile%' or g.name ilike '%New River%' or g.name ilike '%Floras%'
       ))
       or
       (g.state = 'WA' and (
            g.name ilike '%Damon Point%' or g.name ilike '%Copalis Spit%' or g.name ilike '%Midway Beach%'
         or g.name ilike '%Leadbetter%' or g.name ilike '%Graveyard Spit%'
         or g.name ilike '%Long Beach%'
       ))
     )
)
update public.beach_dog_policy bdp
   set dogs_prohibited_start = '03-15',
       dogs_prohibited_end   = '09-15',
       source                = 'manual',
       notes                 = coalesce(bdp.notes || E'\n', '') ||
         'Snowy Plover protection (Mar 15 – Sep 15). Source: USFWS Pacific Coast recovery plan; OPRD/WDFW management areas. Authoritative polygon pinned for shapefile load. Marked source=manual 2026-05-09 to survive resolver overwrites.'
  from named
 where bdp.arena_group_id = named.fid;

commit;
