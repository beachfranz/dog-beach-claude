-- 20260507_pin16_dedup_carveouts.sql
--
-- Pin #16: deactivate duplicate carve-out beaches in beaches_gold.
--
-- Two pairs of records pointing at the same physical beach:
--
--   HB Dog Beach pair (fid 6212 + 9717):
--     - 6212: source=poi, name="Dog Beach", own arena_group (6212)
--     - 9717: source=manual, name="Huntington Beach Dog Beach", own arena_group (9717)
--     - SEPARATE arena clusters — arena dedup never ran on this pair.
--     - Manual seed (9717) is canonical per project_dedupe_canonical_rule.md
--       (most credible-source metadata wins).
--
--   OB Dog Beach pair (fid 6238 + 8358):
--     - 6238: source=poi, name="Ocean Beach Dog Beach", arena_group=8358
--     - 8358: source=osm, name="Dog Beach", arena_group=8358 (head)
--     - SAME arena cluster — arena dedup is correct. But promote_to_gold
--       didn't filter to heads-only, so 6238 got its own beaches_gold row
--       despite being a non-head member.
--     - 8358 is the arena head and has the OSM relation data.
--
-- Action: deactivate the dupes (is_active=false, is_scoreable=false, with
-- inactive_reason note). Doesn't touch arena — that pipeline can run a
-- proper merge later if needed. Detail pages for the deactivated fids
-- still load by URL (graceful degrade); they just stop appearing in
-- find.html and stop getting scored.
--
-- Idempotent via WHERE on source_id (stable identifier).

begin;

update public.beaches_gold
   set is_active = false,
       is_scoreable = false,
       inactive_reason = 'duplicate of fid 9717 (manual canonical) — pin #16 dedup 2026-05-07'
 where source_id = 'poi/12000191'
   and is_active = true;

update public.beaches_gold
   set is_active = false,
       is_scoreable = false,
       inactive_reason = 'duplicate of fid 8358 (osm head; same arena_group_id) — pin #16 dedup 2026-05-07'
 where source_id = 'poi/12605142'
   and is_active = true;

commit;
