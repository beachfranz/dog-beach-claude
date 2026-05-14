-- 20260514_operator_aliases_stopgap.sql
--
-- Option A from the OPRD investigation: stopgap aliases table + immediate
-- backfill of parent_operator_id for known agency-name variants.
--
-- Pass 12 (fuzzy consolidation) has ~12% recall on agency-name dedup —
-- caught 1 of 8 OPRD variants. The structural fix is the agencies/operators/
-- units split (see project_agencies_table_refactor.md). This migration is
-- the immediate patch to consolidate known dupes so OR's existing data
-- isn't broken while we plan the larger refactor.
--
-- What this does:
--  1. Create operator_aliases table (canonical_operator_id, alias_text,
--     pad_us_mng_name_variant, notes, added_by) for curator-maintained
--     synonyms. Pass 12 will read from this in a follow-up.
--  2. Seed it with the known OPRD / ODFW / ODOT variants found in OR.
--  3. Backfill parent_operator_id for those variants immediately so the
--     resolver follows the parent chain to the canonical.
--  4. Migrate any BEP / operator_dogs_policy / operator_policy_extractions
--     refs from variant ids to canonical ids (same pattern as the Pass-12
--     fix migration on 2026-05-13).

begin;

-- ────────────────────────────────────────────────────────────────────────
-- 1. operator_aliases table
-- ────────────────────────────────────────────────────────────────────────

create table if not exists public.operator_aliases (
  id                       bigserial primary key,
  canonical_operator_id    bigint not null references public.operators(id) on delete cascade,
  alias_text               text not null,
  pad_us_mng_name_variant  text,   -- the exact PAD-US loc_mang or mng_name string
  notes                    text,
  added_by                 text default 'system',
  created_at               timestamptz not null default now(),
  unique (canonical_operator_id, alias_text)
);

create index if not exists operator_aliases_text_idx
  on public.operator_aliases (alias_text);
create index if not exists operator_aliases_pad_us_idx
  on public.operator_aliases (pad_us_mng_name_variant)
  where pad_us_mng_name_variant is not null;

comment on table public.operator_aliases is
  'Curator-maintained synonyms for canonical operators. Pass 12 reads this BEFORE running fuzzy matching. Stopgap for project_agencies_table_refactor.';

-- ────────────────────────────────────────────────────────────────────────
-- 2. Seed OPRD aliases
-- ────────────────────────────────────────────────────────────────────────
-- Canonical: id=1742 "Oregon Parks and Recreation Department"
-- Found 6 unparented duplicates: 42506, 42507, 42508, 42514, 42515, 42516, 42518

insert into public.operator_aliases (canonical_operator_id, alias_text, pad_us_mng_name_variant, notes, added_by)
values
  (1742, 'Oregon State Park and Recreation Department',     'Oregon State Park and Recreation Department',     'Pass-12 false negative; seeded 2026-05-14', 'OPRD-stopgap-migration'),
  (1742, 'Oregon State Parks and Recreation Department',    'Oregon State Parks and Recreation Department',    'Pass-12 false negative; seeded 2026-05-14', 'OPRD-stopgap-migration'),
  (1742, 'Oregon State Parks Dept',                          'Oregon State Parks Dept',                          'Pass-12 false negative; seeded 2026-05-14', 'OPRD-stopgap-migration'),
  (1742, 'State of Oregon Parks and Recreation',             'State of Oregon Parks and Recreation',             'Pass-12 false negative; seeded 2026-05-14', 'OPRD-stopgap-migration'),
  (1742, 'State of Oregon Parks & Rec',                      'State of Oregon Parks & Rec',                      'Pass-12 false negative; seeded 2026-05-14', 'OPRD-stopgap-migration'),
  (1742, 'State of Oregon Parks & Recreation Department',    'State of Oregon Parks & Recreation Department',    'Pass-12 false negative; seeded 2026-05-14', 'OPRD-stopgap-migration'),
  (1742, 'State Park & Rec Dept',                            'State Park & Rec Dept',                            'Pass-12 false negative; seeded 2026-05-14', 'OPRD-stopgap-migration'),
  (1742, 'OPRD',                                             null,                                               'Common acronym',                            'OPRD-stopgap-migration')
on conflict (canonical_operator_id, alias_text) do nothing;

-- ────────────────────────────────────────────────────────────────────────
-- 3. Seed ODFW aliases
-- ────────────────────────────────────────────────────────────────────────
-- Canonical: id=1743 "Oregon Department of Fish and Wildlife"
-- Already correctly parented: 42495, 42496
-- Unparented duplicates: 42494, 42498

insert into public.operator_aliases (canonical_operator_id, alias_text, pad_us_mng_name_variant, notes, added_by)
values
  (1743, 'Oregon Dep of Fish & Wildlife',   'Oregon Dep of Fish & Wildlife',   'Pass-12 false negative ("Dep" not "Dept"); seeded 2026-05-14', 'OPRD-stopgap-migration'),
  (1743, 'Oregon Dept of Fish & Wildlife',  'Oregon Dept of Fish & Wildlife',  'Pass-12 false negative ("Dept" abbrev); seeded 2026-05-14',     'OPRD-stopgap-migration'),
  (1743, 'ODFW',                             null,                              'Common acronym',                                                'OPRD-stopgap-migration')
on conflict (canonical_operator_id, alias_text) do nothing;

-- ────────────────────────────────────────────────────────────────────────
-- 4. Seed ODOT aliases
-- ────────────────────────────────────────────────────────────────────────
-- Canonical: id=1745 "Oregon Department of Transportation"
-- Unparented duplicates: 42500, 42501, 42505

insert into public.operator_aliases (canonical_operator_id, alias_text, pad_us_mng_name_variant, notes, added_by)
values
  (1745, 'Oregon Dept of Transportation',  'Oregon Dept of Transportation',  'Pass-12 false negative; seeded 2026-05-14', 'OPRD-stopgap-migration'),
  (1745, 'Oregon DOT',                      'Oregon DOT',                      'Pass-12 false negative; seeded 2026-05-14', 'OPRD-stopgap-migration'),
  (1745, 'OREGON STATE OF(HWY COMM',        'OREGON STATE OF(HWY COMM',        'PAD-US uppercase variant; seeded 2026-05-14', 'OPRD-stopgap-migration'),
  (1745, 'ODOT',                             null,                              'Common acronym',                                'OPRD-stopgap-migration')
on conflict (canonical_operator_id, alias_text) do nothing;

-- ────────────────────────────────────────────────────────────────────────
-- 5. Oregon Dept of State Lands aliases
-- ────────────────────────────────────────────────────────────────────────
-- Canonical: id=1744 "Oregon Department of State Lands"
-- Unparented duplicate: 42499

insert into public.operator_aliases (canonical_operator_id, alias_text, pad_us_mng_name_variant, notes, added_by)
values
  (1744, 'Oregon Dept of State Lands',  'Oregon Dept of State Lands',  'Pass-12 false negative; seeded 2026-05-14', 'OPRD-stopgap-migration'),
  (1744, 'State Land Board',             'State Land Board',             'Historical name; seeded 2026-05-14',          'OPRD-stopgap-migration')
on conflict (canonical_operator_id, alias_text) do nothing;

-- ────────────────────────────────────────────────────────────────────────
-- 6. Backfill parent_operator_id for the unparented duplicates
-- ────────────────────────────────────────────────────────────────────────

-- OPRD variants → 1742
update public.operators
   set parent_operator_id = 1742, is_active = false
 where id in (42506, 42507, 42508, 42514, 42515, 42516, 42518)
   and parent_operator_id is null;

-- ODFW variants → 1743
update public.operators
   set parent_operator_id = 1743, is_active = false
 where id in (42494, 42498)
   and parent_operator_id is null;

-- ODOT variants → 1745
update public.operators
   set parent_operator_id = 1745, is_active = false
 where id in (42500, 42501, 42505)
   and parent_operator_id is null;

-- ODSL variants → 1744
update public.operators
   set parent_operator_id = 1744, is_active = false
 where id in (42499)
   and parent_operator_id is null;

-- ────────────────────────────────────────────────────────────────────────
-- 7. Migrate downstream refs from variants to canonicals
-- (Same pattern as 20260513_pass12_migrate_refs.sql)
-- ────────────────────────────────────────────────────────────────────────

-- BEP governance + dogs evidence
update public.beach_enrichment_provenance bep
   set operator_id = o.parent_operator_id
  from public.operators o
 where bep.operator_id = o.id
   and o.parent_operator_id is not null
   and o.id in (42506, 42507, 42508, 42514, 42515, 42516, 42518,
                42494, 42498,
                42500, 42501, 42505,
                42499);

-- operator_dogs_policy
update public.operator_dogs_policy odp
   set operator_id = o.parent_operator_id
  from public.operators o
 where odp.operator_id = o.id
   and o.parent_operator_id is not null
   and o.id in (42506, 42507, 42508, 42514, 42515, 42516, 42518,
                42494, 42498,
                42500, 42501, 42505,
                42499);

-- operator_policy_extractions
update public.operator_policy_extractions ope
   set operator_id = o.parent_operator_id
  from public.operators o
 where ope.operator_id = o.id
   and o.parent_operator_id is not null
   and o.id in (42506, 42507, 42508, 42514, 42515, 42516, 42518,
                42494, 42498,
                42500, 42501, 42505,
                42499);

commit;
