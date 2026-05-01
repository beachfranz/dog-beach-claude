-- Re-key extraction/consensus metadata on arena.group_id.
--
-- Pre-existing extractions keyed on us_beach_points.fid (== poi_landing.fid;
-- the two share the same identity from US_beaches.csv). The arena pipeline
-- introduced its own fid space; future extractions need to anchor on the
-- canonical "beach" identity = arena.group_id.
--
-- Strategy: add nullable `arena_group_id` to each fid-keyed metadata table,
-- backfill from the existing fid via `arena.source_id = 'poi/<fid>'` lookup.
-- Keep legacy `fid` column for back-tracking. Future writes set both.

-- ── 1. Schema additions ─────────────────────────────────────────────

-- beach_policy_consensus is a VIEW (computed from beach_policy_extractions);
-- once the underlying table has arena_group_id, we'll update the view
-- separately to expose it.

alter table public.beach_policy_extractions
  add column if not exists arena_group_id bigint;
alter table public.park_url_extractions
  add column if not exists arena_group_id bigint;
alter table public.policy_research_extractions
  add column if not exists arena_group_id bigint;
alter table public.beach_policy_gold_set
  add column if not exists arena_group_id bigint;

create index if not exists beach_policy_extractions_arena_idx
  on public.beach_policy_extractions (arena_group_id);
create index if not exists park_url_extractions_arena_idx
  on public.park_url_extractions (arena_group_id);
create index if not exists policy_research_extractions_arena_idx
  on public.policy_research_extractions (arena_group_id);
create index if not exists beach_policy_gold_set_arena_idx
  on public.beach_policy_gold_set (arena_group_id);


-- ── 2. Backfill ─────────────────────────────────────────────────────
-- For each row, look up the arena row via source_id = 'poi/<fid>' and
-- copy its group_id. group_id is the canonical "beach" identifier (the
-- polygon's arena.fid for matched POIs, or the singleton's own fid).

update public.beach_policy_extractions e
   set arena_group_id = a.group_id
  from public.arena a
 where a.source_code = 'poi'
   and a.source_id = 'poi/' || e.fid::text
   and e.arena_group_id is null;

update public.park_url_extractions p
   set arena_group_id = a.group_id
  from public.arena a
 where a.source_code = 'poi'
   and a.source_id = 'poi/' || p.fid::text
   and p.arena_group_id is null;

update public.policy_research_extractions r
   set arena_group_id = a.group_id
  from public.arena a
 where a.source_code = 'poi'
   and a.source_id = 'poi/' || r.fid::text
   and r.arena_group_id is null;

update public.beach_policy_gold_set g
   set arena_group_id = a.group_id
  from public.arena a
 where a.source_code = 'poi'
   and a.source_id = 'poi/' || g.fid::text
   and g.arena_group_id is null;
