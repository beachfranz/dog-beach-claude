-- 20260503_harmony_phase7_3b_arena_backfill.sql
--
-- Phase 7.3 part 1b: arena-based backfill follow-up.
--
-- Phase 7.3 part 1 backfilled gold_fid only against beaches_gold (50m).
-- Mapped 1,087 / 5,532 legacy evidence rows. Stranded the rest because
-- 67% of the "orphans" are actually in arena (the master inventory)
-- but not yet promoted to beaches_gold.
--
-- This migration extends the backfill: if no beaches_gold beach within
-- 50m, fall back to the nearest active arena beach within 200m. Since
-- arena.fid equals beaches_gold.fid by path-3 inheritance, setting
-- gold_fid = arena.fid points the evidence at the right id even when
-- arena hasn't been promoted yet — when promotion eventually happens,
-- the evidence is already wired correctly.
--
-- Same conflict-skip semantics as part 1a: skip rows that would
-- duplicate an existing (gold_fid, field_group, source) tuple. Dedup
-- within batch via row_number on (gold_fid, field_group, source).

begin;

with legacy_rows as (
  select e.id, e.fid as legacy_fid, e.field_group, e.source, ls.geom
    from public.beach_enrichment_provenance e
    join public.locations_stage ls on ls.fid = e.fid
   where e.gold_fid is null
     and ls.geom is not null
),
mapped as (
  select lr.id, lr.field_group, lr.source,
         (select a.fid from public.arena a
           where a.is_active
             and st_dwithin(a.geom::geography, lr.geom, 200)
           order by st_distance(a.geom::geography, lr.geom) asc
           limit 1) as gold_fid_candidate
    from legacy_rows lr
),
non_conflicting as (
  select m.id, m.gold_fid_candidate, m.field_group, m.source
    from mapped m
   where m.gold_fid_candidate is not null
     and not exists (
       select 1 from public.beach_enrichment_provenance e2
        where e2.gold_fid = m.gold_fid_candidate
          and e2.field_group = m.field_group
          and e2.source = m.source
     )
),
deduped as (
  select id, gold_fid_candidate
    from (
      select nc.id, nc.gold_fid_candidate,
             row_number() over (
               partition by nc.gold_fid_candidate, nc.field_group, nc.source
               order by nc.id desc
             ) as rnk
        from non_conflicting nc
    ) x
   where rnk = 1
)
update public.beach_enrichment_provenance e
   set gold_fid = d.gold_fid_candidate
  from deduped d
 where d.id = e.id;

commit;
