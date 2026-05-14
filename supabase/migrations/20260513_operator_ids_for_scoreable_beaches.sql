-- 20260513_operator_ids_for_scoreable_beaches.sql
--
-- Adds the strict-attribution helper for Phase 26 cost gate.
--
-- Background: state_operator_ids_with_beaches (the older helper) returns
-- both BEP-attributed operators AND any operator with geom within 1km of
-- ANY active beach in the state (regardless of scoring_tier). For OR
-- this returns 114 operators; raw operators table has 3,048.
--
-- Franz's Phase 26 cost gate (2026-05-13): "only process operators
-- definitively assigned to a beach that has daily or hourly refreshes."
--
-- Definitive assignment = a beach_enrichment_provenance row with
-- operator_id NOT NULL pointing to this operator, where the beach is
-- scoring_tier IN ('daily','hourly'). For OR this returns 7 operators.
--
-- Cost delta: 3048 ops × $0.30 = $914  →  7 ops × $0.30 = $2.10.

begin;

create or replace function public.state_operator_ids_for_scoreable_beaches(p_state text)
returns table(operator_id bigint)
language sql stable as $$
  select distinct bep.operator_id
    from public.beach_enrichment_provenance bep
    join public.beaches_gold g on g.fid = bep.gold_fid
   where g.state = p_state
     and g.is_active
     and g.scoring_tier in ('daily','hourly')
     and bep.operator_id is not null;
$$;

grant execute on function public.state_operator_ids_for_scoreable_beaches(text)
  to anon, authenticated, service_role;

commit;
