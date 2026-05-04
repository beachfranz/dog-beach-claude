-- 20260503_harmony_phase5_promote_containment.sql
--
-- Phase 5 of the harmony migration (see docs/harmony.md).
--
-- Closes the Emit → Resolve → Promote loop for beach-CPAD containment.
-- After this lands, raw `UPDATE beaches_gold SET cpad_unit_id = X` is dead;
-- assignment is fully evidence-driven and auditable.
--
-- Adds three functions:
--   1. _resolve_polygon_containment(p_fid)
--        Sets is_canonical=true on the winning evidence row per beach.
--        Today: trivial (single source = cpad). When manual_curator/llm
--        sources are added later, this is where source-priority resolution
--        lives (manual > cpad_spatial).
--
--   2. _promote_polygon_containment_to_gold(p_fid)
--        Reads is_canonical=true rows in field_group='polygon_containment'
--        and UPDATEs beaches_gold.cpad_unit_id with the polygon_id from
--        claimed_values. Only writes when value actually changes (IS DISTINCT
--        FROM) to keep updated_at meaningful and audit clean.
--
--   3. populate_polygon_containment_gold(p_fid)
--        Orchestrator wrapping emit + resolve + promote, mirroring the
--        legacy populate_from_<source> pattern.

begin;

-- 1. Resolver: pick canonical containment evidence per (gold_fid, field_group).
--    Today single-source — cpad always wins by default. Future sources will
--    extend this with priority logic.
create or replace function public._resolve_polygon_containment(p_fid bigint default null)
returns int
language plpgsql
as $$
declare touched int := 0;
begin
  -- First clear any stale canonical flags on rows we're about to re-resolve.
  update public.beach_enrichment_provenance e
     set is_canonical = false
   where e.field_group = 'polygon_containment'
     and e.gold_fid is not null
     and (p_fid is null or e.gold_fid = p_fid);

  -- Then mark winners. Single-source today: cpad wins; future:
  --   priority order = manual_curator > llm > cpad
  with ranked as (
    select e.id,
      row_number() over (
        partition by e.gold_fid
        order by case e.source
                   when 'manual' then 1
                   when 'llm'    then 2
                   when 'cpad'   then 3
                   else               9
                 end,
                 e.confidence desc nulls last,
                 e.id asc
      ) as rnk
    from public.beach_enrichment_provenance e
    where e.field_group = 'polygon_containment'
      and e.gold_fid is not null
      and (p_fid is null or e.gold_fid = p_fid)
  ),
  upd as (
    update public.beach_enrichment_provenance e
       set is_canonical = true
      from ranked r
     where r.id = e.id and r.rnk = 1
    returning 1
  )
  select count(*) into touched from upd;

  return touched;
end;
$$;

-- 2. Promoter: write cpad_unit_id from canonical containment evidence.
create or replace function public._promote_polygon_containment_to_gold(p_fid bigint default null)
returns int
language plpgsql
as $$
declare touched int := 0;
begin
  with canonical as (
    select gold_fid,
           (claimed_values->>'polygon_id')::bigint as cpad_unit_id
      from public.beach_enrichment_provenance
     where field_group = 'polygon_containment'
       and is_canonical = true
       and gold_fid is not null
       and claimed_values->>'polygon_kind' = 'cpad_unit'
       and (p_fid is null or gold_fid = p_fid)
  ),
  upd as (
    update public.beaches_gold g
       set cpad_unit_id = c.cpad_unit_id
      from canonical c
     where g.fid = c.gold_fid
       and (g.cpad_unit_id is distinct from c.cpad_unit_id)
    returning 1
  )
  select count(*) into touched from upd;

  return touched;
end;
$$;

-- 3. Orchestrator: emit + resolve + promote.
create or replace function public.populate_polygon_containment_gold(p_fid bigint default null)
returns table (emitted int, resolved int, promoted int)
language plpgsql
as $$
declare e_count int; r_count int; p_count int;
begin
  e_count := public.populate_cpad_containment_gold(p_fid);
  r_count := public._resolve_polygon_containment(p_fid);
  p_count := public._promote_polygon_containment_to_gold(p_fid);
  return query select e_count, r_count, p_count;
end;
$$;

comment on function public._resolve_polygon_containment(bigint) is
  'Phase 5 of harmony migration. Sets is_canonical=true on winning containment evidence per beach. Source priority: manual > llm > cpad.';

comment on function public._promote_polygon_containment_to_gold(bigint) is
  'Phase 5 of harmony migration. Writes beaches_gold.cpad_unit_id from canonical containment evidence. Only updates when value changes.';

comment on function public.populate_polygon_containment_gold(bigint) is
  'Phase 5 of harmony migration. Orchestrator: emit (populate_cpad_containment_gold) + resolve (_resolve_polygon_containment) + promote (_promote_polygon_containment_to_gold). Replaces raw UPDATE pattern for cpad_unit_id assignment.';

commit;
