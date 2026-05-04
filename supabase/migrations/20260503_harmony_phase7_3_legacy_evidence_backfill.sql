-- 20260503_harmony_phase7_3_legacy_evidence_backfill.sql
--
-- Phase 7.3 part 1: spatial backfill of gold_fid for legacy evidence rows.
--
-- Context: 5,532 evidence rows in beach_enrichment_provenance have legacy
-- fid set but gold_fid null (from before the harmony migration). 846
-- distinct legacy fids; all are present in locations_stage with geom.
--
-- Strategy: for each legacy evidence row, find the nearest beaches_gold
-- beach within 50m of the locations_stage geom. Skip when:
--   * No beaches_gold beach is within 50m (legacy row is for a beach
--     that doesn't have a gold-spine equivalent — likely defunct or
--     not promoted)
--   * A gold-side row already exists with same (gold_fid, field_group,
--     source) — gold-side wins (it's typically fresher; the harmony
--     populators emitted from current production data)
--
-- After this lands, we'll know how many legacy evidence rows have
-- gold_fid mapped vs. how many remain stranded. Stranded rows can
-- eventually be dropped when 7.3 part 2 (DROP COLUMN fid) runs.

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
         (select g.fid from public.beaches_gold g
           where st_dwithin(g.geom::geography, lr.geom, 50)
           order by st_distance(g.geom::geography, lr.geom) asc
           limit 1) as gold_fid_candidate
    from legacy_rows lr
),
non_conflicting as (
  select m.id, m.gold_fid_candidate, m.field_group, m.source
    from mapped m
   where m.gold_fid_candidate is not null
     and not exists (
       -- Don't conflict with existing gold-side rows
       select 1 from public.beach_enrichment_provenance e2
        where e2.gold_fid = m.gold_fid_candidate
          and e2.field_group = m.field_group
          and e2.source = m.source
     )
),
deduped as (
  -- Within this batch, multiple legacy rows can map to the same
  -- (gold_fid, field_group, source) tuple — partial unique constraint
  -- allows only one. Keep the newest legacy row per tuple (highest id).
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
