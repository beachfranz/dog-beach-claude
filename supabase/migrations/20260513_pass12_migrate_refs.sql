-- 20260513_pass12_migrate_refs.sql
--
-- Pass 12 fuzzy consolidation correctly identifies dupes and points
-- `parent_operator_id` from the loser to the canonical winner. BUT it
-- doesn't migrate existing operator_id references in dependent tables.
--
-- Result observed 2026-05-13: OR has id=42502 (Oregon Parks & Rec,
-- subtype=state) with parent_operator_id=1742 (same name, subtype=
-- state-parks). BEP rows attribute to id=42502 (the loser); the
-- helper `state_operator_ids_for_scoreable_beaches` returns 42502;
-- Phase 26 extraction runs on the deactivated dupe and produces no
-- useful policy.
--
-- Fix:
--   1. One-shot backfill: for every BEP / operator_dogs_policy /
--      operator_policy_extractions row pointing to a deactivated
--      operator with a parent_operator_id, repoint to the parent.
--   2. Patch _op_pass12_consolidation to run the same migration as
--      part of every consolidation cycle (so future runs migrate
--      references inline).

begin;

-- ────────────────────────────────────────────────────────────────────────
-- 1. One-shot backfill: migrate refs from deactivated dupes → canonical
-- ────────────────────────────────────────────────────────────────────────

-- BEP rows: no unique constraint on operator_id, safe to UPDATE
update public.beach_enrichment_provenance bep
   set operator_id = o.parent_operator_id,
       updated_at  = now()
  from public.operators o
 where bep.operator_id = o.id
   and o.parent_operator_id is not null
   and not o.is_active;

-- operator_dogs_policy: PK on operator_id → can't UPDATE if canonical
-- already has a row. Strategy: DELETE the dupe row when canonical
-- already has one (canonical wins; we trust its extraction); else UPDATE.
delete from public.operator_dogs_policy odp_d
 using public.operators o
 where odp_d.operator_id = o.id
   and o.parent_operator_id is not null
   and not o.is_active
   and exists (
     select 1 from public.operator_dogs_policy odp_c
      where odp_c.operator_id = o.parent_operator_id
   );

update public.operator_dogs_policy odp
   set operator_id = o.parent_operator_id
  from public.operators o
 where odp.operator_id = o.id
   and o.parent_operator_id is not null
   and not o.is_active;

-- operator_policy_extractions: PK is (operator_id + source_url + extraction_run)
-- or similar — also could conflict. Same DELETE-then-UPDATE pattern.
delete from public.operator_policy_extractions ope_d
 using public.operators o
 where ope_d.operator_id = o.id
   and o.parent_operator_id is not null
   and not o.is_active
   and exists (
     select 1 from public.operator_policy_extractions ope_c
      where ope_c.operator_id = o.parent_operator_id
        and ope_c.source_url is not distinct from ope_d.source_url
   );

update public.operator_policy_extractions ope
   set operator_id = o.parent_operator_id
  from public.operators o
 where ope.operator_id = o.id
   and o.parent_operator_id is not null
   and not o.is_active;

-- ────────────────────────────────────────────────────────────────────────
-- 2. Patch _op_pass12_consolidation to migrate refs inline going forward
-- ────────────────────────────────────────────────────────────────────────

create or replace function public._op_pass12_consolidation(p_state text)
returns integer language plpgsql as $function$
declare
  v_total int := 0;
  v_level_count int := 0;
  v_level text;
begin
  for v_level in
    select distinct level
      from public.operators
     where state_code = p_state and is_active and parent_operator_id is null
  loop
    with pairs as (
      select a.id  as a_id, b.id  as b_id,
             a.aliases as a_aliases, b.aliases as b_aliases,
             a.pad_us_mng_name as a_pmn, b.pad_us_mng_name as b_pmn,
             coalesce(array_length(a.pad_us_mng_name, 1), 0) as a_pad,
             coalesce(array_length(b.pad_us_mng_name, 1), 0) as b_pad,
             coalesce(array_length(a.aliases, 1), 0)         as a_alias,
             coalesce(array_length(b.aliases, 1), 0)         as b_alias
        from public.operators a
        join public.operators b
          on a.state_code = b.state_code
         and a.level      = b.level
         and a.id        < b.id
         and a.is_active and b.is_active
         and a.parent_operator_id is null
         and b.parent_operator_id is null
       where a.state_code = p_state
         and a.level = v_level
         and similarity(a.canonical_name, b.canonical_name) > 0.6
         and exists (
           select 1
             from unnest(a.aliases) aa, unnest(b.aliases) ab
            where public._normalize_agency_text(aa) is not null
              and public._normalize_agency_text(aa) = public._normalize_agency_text(ab)
         )
    ),
    picked as (
      select
        case when (a_pad, a_alias, b_id) >= (b_pad, b_alias, a_id) then a_id else b_id end as canonical_id,
        case when (a_pad, a_alias, b_id) >= (b_pad, b_alias, a_id) then b_id else a_id end as dupe_id,
        case when (a_pad, a_alias, b_id) >= (b_pad, b_alias, a_id) then b_aliases else a_aliases end as dupe_aliases,
        case when (a_pad, a_alias, b_id) >= (b_pad, b_alias, a_id) then b_pmn else a_pmn end as dupe_pmn
        from pairs
    ),
    per_canonical as (
      select canonical_id,
             array_agg(distinct alias) filter (where alias is not null) as add_aliases,
             array_agg(distinct pmn)   filter (where pmn   is not null) as add_pmns,
             array_agg(distinct dupe_id) as dupe_ids
        from picked
        left join lateral unnest(dupe_aliases) alias on true
        left join lateral unnest(coalesce(dupe_pmn, '{}'::text[])) pmn on true
       group by canonical_id
    ),
    merge_canonical as (
      update public.operators op
         set aliases = (
               select array_agg(distinct x)
                 from unnest(op.aliases || coalesce(pc.add_aliases, '{}'::text[])) x
             ),
             pad_us_mng_name = (
               select array_agg(distinct x)
                 from unnest(coalesce(op.pad_us_mng_name, '{}'::text[])
                           || coalesce(pc.add_pmns, '{}'::text[])) x
             ),
             updated_at = now()
        from per_canonical pc
       where op.id = pc.canonical_id
      returning op.id
    ),
    -- ★ NEW: migrate BEP operator_id refs from dupes → canonical
    migrate_bep as (
      update public.beach_enrichment_provenance bep
         set operator_id = pc.canonical_id,
             updated_at  = now()
        from per_canonical pc
       where bep.operator_id = ANY(pc.dupe_ids)
      returning bep.id
    ),
    -- ★ NEW: migrate operator_dogs_policy refs
    migrate_odp as (
      update public.operator_dogs_policy odp
         set operator_id = pc.canonical_id
        from per_canonical pc
       where odp.operator_id = ANY(pc.dupe_ids)
      returning odp.operator_id
    ),
    -- ★ NEW: migrate operator_policy_extractions refs
    migrate_ope as (
      update public.operator_policy_extractions ope
         set operator_id = pc.canonical_id
        from per_canonical pc
       where ope.operator_id = ANY(pc.dupe_ids)
      returning ope.id
    ),
    deactivate_dupes as (
      update public.operators op
         set parent_operator_id = pc.canonical_id,
             is_active          = false,
             inactive_reason    = 'deduped_pass12',
             updated_at         = now()
        from per_canonical pc
       where op.id = ANY(pc.dupe_ids)
         and op.is_active
      returning op.id
    )
    select count(*) into v_level_count from deactivate_dupes;
    v_total := v_total + coalesce(v_level_count, 0);
    raise notice 'Pass 12 level=%: consolidated=%', v_level, v_level_count;
  end loop;
  return v_total;
end $function$;

commit;
