-- Re-key beach_policy_extractions and beach_policy_gold_set so that
-- their `fid` column holds arena.fid (the future beaches_gold.fid)
-- instead of us_beach_points.fid (Geoapify).
--
-- Today the two columns are in disjoint keyspaces:
--   beach_policy_extractions.fid           = us_beach_points.fid (Geoapify)
--   beach_policy_extractions.arena_group_id = arena.fid           (canonical)
-- After this migration, both columns equal arena.fid.
--
-- Why now: we're retargeting the FK on `fid` from us_beach_points to
-- beaches_gold (=arena.fid). Without this rewrite, every extraction
-- row violates the new FK.
--
-- Includes a dedupe re-key for the 3 beaches arena marked inactive:
--   4671 Shaw's Cove   → 7729 Shaw's Cove Beach   (likely_dup_of/7729)
--   6978 Crescent Bay  →  218                     (likely_dup_of/218)
--   8758 Fort Funston  → 8226 Seal Rocks (GGNRA)  (subsegment_of/8226)
--
-- Pre-flight (verified 2026-05-01, all returned 0):
--   - gold_set unique(fid,field_name) would not collide after rewrite
--   - 0 rows with NULL arena_group_id in either table
--
-- DRY-RUN — apply via Supabase dashboard SQL editor when ready.
-- Wrapped in BEGIN/COMMIT so any failure rolls back cleanly.
--
-- Reversibility: the original Geoapify fid is preserved in
-- legacy_geoapify_fid (added in step 1 below). To revert:
--   UPDATE public.beach_policy_extractions SET fid = legacy_geoapify_fid;
--   UPDATE public.beach_policy_gold_set    SET fid = legacy_geoapify_fid;
-- Drop legacy_geoapify_fid only after the new model has been stable for
-- a few weeks (no separate migration needed; just an ALTER TABLE).

begin;

-- ── 0. Drop the old FK to us_beach_points ───────────────────────────
-- The rewrite in step 4 changes fid values from Geoapify -> arena
-- keyspace, which the old FK would reject. The new FK is added by the
-- separate fk_retarget migration AFTER beaches_gold is fully populated
-- and verified.
alter table public.beach_policy_extractions
  drop constraint if exists beach_policy_extractions_fid_fkey;

alter table public.beach_policy_gold_set
  drop constraint if exists beach_policy_gold_set_fid_fkey;

-- ── 1. Backup the original fid before we overwrite it ───────────────
alter table public.beach_policy_extractions
  add column if not exists legacy_geoapify_fid bigint;

alter table public.beach_policy_gold_set
  add column if not exists legacy_geoapify_fid bigint;

-- Copy current fid into the backup column for any row not yet backed up.
-- Idempotent: safe to re-run; only fills nulls.
update public.beach_policy_extractions
   set legacy_geoapify_fid = fid
 where legacy_geoapify_fid is null;

update public.beach_policy_gold_set
   set legacy_geoapify_fid = fid
 where legacy_geoapify_fid is null;

comment on column public.beach_policy_extractions.legacy_geoapify_fid is
  'Original us_beach_points.fid (Geoapify keyspace) before the 2026-05-01 re-key to arena.fid keyspace. Drop after a few weeks of stability.';
comment on column public.beach_policy_gold_set.legacy_geoapify_fid is
  'Original us_beach_points.fid (Geoapify keyspace) before the 2026-05-01 re-key to arena.fid keyspace. Drop after a few weeks of stability.';

-- ── 2. Pre-flight inside the txn (defensive) ────────────────────────
do $do$
declare
  null_extractions int;
  null_gold        int;
  collision_gold   int;
begin
  select count(*) into null_extractions
    from public.beach_policy_extractions where arena_group_id is null;
  if null_extractions > 0 then
    raise exception 'ABORT: % extractions rows with NULL arena_group_id; rewrite would lose them', null_extractions;
  end if;

  select count(*) into null_gold
    from public.beach_policy_gold_set where arena_group_id is null;
  if null_gold > 0 then
    raise exception 'ABORT: % gold_set rows with NULL arena_group_id', null_gold;
  end if;

  -- Check for collisions AFTER the redundant-row delete in step 3 below.
  -- Pattern: a deduped-side row (4671/6978/8758) survives the delete only
  -- if its field_name doesn't already exist at the canonical fid. If a
  -- non-redundant collision still exists, it's a real curation conflict
  -- and we should abort.
  select count(*) into collision_gold from (
    select 1
      from public.beach_policy_gold_set src
     where src.arena_group_id in (4671, 6978, 8758)
       and exists (
         select 1 from public.beach_policy_gold_set tgt
          where tgt.arena_group_id = case src.arena_group_id
                                       when 4671 then 7729
                                       when 6978 then 218
                                       when 8758 then 8226
                                     end
            and tgt.field_name = src.field_name
            and (tgt.verified_value is distinct from src.verified_value
                 or tgt.source_url     is distinct from src.source_url)
       )
  ) x;
  if collision_gold > 0 then
    raise exception 'ABORT: % gold_set rows have a real (non-trivial) curation conflict between deduped fid and canonical fid; resolve manually', collision_gold;
  end if;
end
$do$;

-- ── 3. Re-key the 3 deduped beaches ─────────────────────────────────
-- Some gold_set rows on the deduped side (4671/6978/8758) might
-- collide with rows on the canonical side (7729/218/8226) due to the
-- UNIQUE(fid, field_name) constraint. Drop any deduped-side gold_set
-- row whose field_name already exists at the canonical fid.
-- Verified 2026-05-01: 1 such row (8758 dogs_allowed -> 8226 already has it).
delete from public.beach_policy_gold_set src
 where src.arena_group_id in (4671, 6978, 8758)
   and exists (
     select 1 from public.beach_policy_gold_set tgt
      where tgt.arena_group_id = case src.arena_group_id
                                   when 4671 then 7729
                                   when 6978 then 218
                                   when 8758 then 8226
                                 end
        and tgt.field_name = src.field_name
   );

update public.beach_policy_extractions
   set arena_group_id = case arena_group_id
                          when 4671 then 7729
                          when 6978 then 218
                          when 8758 then 8226
                        end
 where arena_group_id in (4671, 6978, 8758);

update public.beach_policy_gold_set
   set arena_group_id = case arena_group_id
                          when 4671 then 7729
                          when 6978 then 218
                          when 8758 then 8226
                        end
 where arena_group_id in (4671, 6978, 8758);

-- ── 4. Rewrite fid from Geoapify to arena keyspace ──────────────────
-- After this UPDATE, fid = arena_group_id for every row (no more
-- two-keyspace shenanigans).
update public.beach_policy_extractions
   set fid = arena_group_id
 where fid <> arena_group_id;

update public.beach_policy_gold_set
   set fid = arena_group_id
 where fid <> arena_group_id;

-- ── 5. Post-flight: every row should now have fid in beaches_gold ──
-- (Run this AFTER beaches_gold is populated. This block doesn't fail
-- the txn — it just reports what would block the FK retarget.)
do $do$
declare
  unmatched_extractions int;
  unmatched_gold        int;
begin
  -- beaches_gold may not exist yet; this block is best-effort.
  if exists (select 1 from information_schema.tables
              where table_schema='public' and table_name='beaches_gold') then
    select count(*) into unmatched_extractions
      from public.beach_policy_extractions e
      left join public.beaches_gold g on g.fid = e.fid
     where g.fid is null;
    select count(*) into unmatched_gold
      from public.beach_policy_gold_set s
      left join public.beaches_gold g on g.fid = s.fid
     where g.fid is null;
    raise notice 'extractions rows with no matching beaches_gold row: %', unmatched_extractions;
    raise notice 'gold_set    rows with no matching beaches_gold row: %', unmatched_gold;
    if unmatched_extractions > 0 or unmatched_gold > 0 then
      raise notice 'FK retarget would FAIL until those rows are addressed.';
    else
      raise notice 'OK to apply 20260501_beaches_gold_fk_retarget.sql.';
    end if;
  else
    raise notice 'beaches_gold not yet created; run schema migration + populator first.';
  end if;
end
$do$;

commit;
