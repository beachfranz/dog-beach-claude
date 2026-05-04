-- 20260503_harmony_phase8_6_gold_resolvers.sql
--
-- Gold-side resolvers for governance / dogs / practical field groups.
-- Mirror _resolve_polygon_containment's shape (which exists for
-- field_group='polygon_containment').
--
-- Each resolver picks the single canonical evidence row per beach per
-- field_group via source priority. Today's source priority order
-- (highest → lowest):
--   manual         > llm        > park_url        > park_operators
--   research       > old_school_llm
--   cpad           > jurisdictions > counties > nps_places > military_bases
--                  > tribal_lands
-- Within ties, confidence desc then id asc.
--
-- These functions DON'T promote anywhere. They just set is_canonical=true
-- on the winning evidence rows so a downstream consumer (a future cascade
-- refactor or a calibration query) can read "the canonical answer for
-- field X on beach Y" without re-deriving consensus per-call.
--
-- Promoters that write canonical evidence to beach_dog_policy /
-- beach_amenities / etc. are deliberately NOT shipped in this migration
-- — they would be parallel to promote_production_to_beach_dog_policy.py
-- (the inline-consensus path that already works) and need a clear case
-- for replacement vs. coexistence. Park that decision until a consumer
-- actually wants evidence-resolved canonical values.

begin;

-- Source priority lookup function (DRY; reused across all 3 resolvers)
create or replace function public._gold_source_priority(p_source text)
returns int
language sql immutable
as $$
  select case p_source
    when 'manual'         then 1
    when 'llm'            then 2
    when 'park_url'       then 3
    when 'park_operators' then 3
    when 'research'       then 4
    when 'old_school_llm' then 5
    when 'cpad'           then 6
    when 'jurisdictions'  then 7
    when 'counties'       then 8
    when 'nps_places'     then 9
    when 'military_bases' then 10
    when 'tribal_lands'   then 10
    else                       99
  end
$$;

-- Generic factory: picks canonical per (gold_fid) within a single
-- field_group on the gold side. Used by the three field-group-specific
-- resolvers below.
create or replace function public._resolve_field_group_gold(
  p_field_group text,
  p_fid bigint default null
)
returns int
language plpgsql
as $$
declare touched int := 0;
begin
  update public.beach_enrichment_provenance e
     set is_canonical = false
   where e.field_group = p_field_group
     and e.gold_fid is not null
     and (p_fid is null or e.gold_fid = p_fid);

  with ranked as (
    select e.id,
      row_number() over (
        partition by e.gold_fid
        order by public._gold_source_priority(e.source) asc,
                 e.confidence desc nulls last,
                 e.id asc
      ) as rnk
    from public.beach_enrichment_provenance e
    where e.field_group = p_field_group
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

-- Three field-group-specific wrappers for ergonomic call sites + clear
-- audit trail in pg_stat_user_functions / EXPLAIN.
create or replace function public._resolve_governance_gold(p_fid bigint default null)
returns int language sql as $$
  select public._resolve_field_group_gold('governance', p_fid);
$$;

create or replace function public._resolve_dogs_gold(p_fid bigint default null)
returns int language sql as $$
  select public._resolve_field_group_gold('dogs', p_fid);
$$;

create or replace function public._resolve_practical_gold(p_fid bigint default null)
returns int language sql as $$
  select public._resolve_field_group_gold('practical', p_fid);
$$;

comment on function public._gold_source_priority(text) is
  'Source priority for gold-side canonical resolution. Lower number wins. manual=1, llm=2, park_url/park_operators=3, research=4, old_school_llm=5, then spatial sources cpad=6 down through tribal_lands=10.';

comment on function public._resolve_field_group_gold(text, bigint) is
  'Generic resolver factory. Sets is_canonical=true on the winning evidence row per beach within a field_group. Used by _resolve_governance_gold / _resolve_dogs_gold / _resolve_practical_gold.';

comment on function public._resolve_governance_gold(bigint) is
  'Phase 8 resolver. Picks canonical governance evidence per beach. Source priority (high→low): manual > llm > park_url > park_operators > research > old_school_llm > cpad > jurisdictions > counties > federal/military/tribal.';

commit;
