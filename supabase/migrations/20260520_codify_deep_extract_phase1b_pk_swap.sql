-- 20260520_codify_deep_extract_phase1b_pk_swap.sql
--
-- Follow-up to phase1 (8cff724): swap beach_policy_source primary key
-- from composite (beach_fid, policy_source_id, section) to surrogate id,
-- and replace the composite uniqueness with a richer dedupe index that
-- includes region_name (COALESCE'd to handle NULLs) + rule.
--
-- Per Franz 2026-05-20 — Option G:
--   - Prevents dupes (UNIQUE on the richer tuple catches accidental
--     re-inserts at the exact (beach, source, section, region, rule)).
--   - Lets Del Mar emit ~8 distinct rows: 4 regions × multiple sections
--     × multiple rules, plus the default-region baseline.
--   - Lets Avila Beach split into 2 rule-rows under one region
--     (off_leash + not_allowed) with temporal layers in bps_temporal.
--
-- Audit performed: only 3 writer scripts use the old composite ON
-- CONFLICT target. They'll be updated in this same task to ON CONFLICT
-- against the new index.

begin;

-- ── Step 0: Refactor beach_policy_source_temporal FK to use bps.id ──
-- Currently temporal references the composite (beach_fid, policy_source_id,
-- section). After deep-extract, that tuple is no longer unique. Switch the
-- FK to point at the surrogate bps.id (added in phase 1).
alter table public.beach_policy_source_temporal
  add column if not exists bps_id bigint;

-- Backfill bps_id from the existing composite. Pre-deep-extract data has
-- 1:1 mapping so this resolves deterministically.
update public.beach_policy_source_temporal t
   set bps_id = bps.id
  from public.beach_policy_source bps
 where t.beach_fid = bps.beach_fid
   and t.policy_source_id = bps.policy_source_id
   and t.section = bps.section
   and t.bps_id is null;

-- Drop the old composite FK and add the new surrogate FK.
alter table public.beach_policy_source_temporal
  drop constraint if exists beach_policy_source_temporal_beach_fid_policy_source_id_se_fkey;

alter table public.beach_policy_source_temporal
  add constraint beach_policy_source_temporal_bps_id_fkey
    foreign key (bps_id) references public.beach_policy_source(id) on delete cascade;

create index if not exists beach_policy_source_temporal_bps_id_idx
  on public.beach_policy_source_temporal (bps_id);

-- ── Step 1: Swap bps PK from composite to surrogate id ────────────
-- Approach: drop the 3 dependent FKs (parent_bps_id, vocab_queue.bps_id,
-- temporal.bps_id), drop the existing unique-on-id constraint, drop the
-- composite PK, add id as PK, then re-add the 3 FKs against the new PK.

-- 1a. Detach FKs that reference the id-unique constraint
alter table public.beach_policy_source
  drop constraint if exists beach_policy_source_parent_bps_id_fkey;
alter table public.vocab_review_queue
  drop constraint if exists vocab_review_queue_bps_id_fkey;
alter table public.beach_policy_source_temporal
  drop constraint if exists beach_policy_source_temporal_bps_id_fkey;

-- 1b. Drop the now-unreferenced unique-on-id AND the composite PK
alter table public.beach_policy_source
  drop constraint if exists beach_policy_source_id_key;
alter table public.beach_policy_source
  drop constraint if exists beach_policy_source_pkey;

-- 1c. Promote id to PK
alter table public.beach_policy_source
  add constraint beach_policy_source_pkey primary key (id);

-- 1d. Reattach the 3 FKs against the new PK
alter table public.beach_policy_source
  add constraint beach_policy_source_parent_bps_id_fkey
    foreign key (parent_bps_id) references public.beach_policy_source(id) on delete cascade;
alter table public.vocab_review_queue
  add constraint vocab_review_queue_bps_id_fkey
    foreign key (bps_id) references public.beach_policy_source(id) on delete cascade;
alter table public.beach_policy_source_temporal
  add constraint beach_policy_source_temporal_bps_id_fkey
    foreign key (bps_id) references public.beach_policy_source(id) on delete cascade;

-- New dedupe key: (beach, source, section, region, rule). COALESCE on
-- region_name handles legacy NULL rows uniformly with deep-extract rows
-- that name a region.
create unique index if not exists beach_policy_source_dedupe_idx
  on public.beach_policy_source
    (beach_fid, policy_source_id, section,
     (coalesce(region_name, '__default__')),
     rule);

comment on index public.beach_policy_source_dedupe_idx is
  'Deep-extract dedupe key. Writers use this as ON CONFLICT target with '
  '(beach_fid, policy_source_id, section, COALESCE(region_name,''__default__''), rule). '
  'Old code (region_name=NULL) and new deep-extract code (region_name=set) '
  'both honored.';

-- Keep the old composite available as a non-unique lookup index for
-- joins/groups that don't care about region/rule slicing.
create index if not exists beach_policy_source_legacy_lookup_idx
  on public.beach_policy_source (beach_fid, policy_source_id, section);

commit;
