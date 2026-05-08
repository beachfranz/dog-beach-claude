-- 20260508_consensus_timeout_bump.sql
--
-- WA promote_to_gold ran into 8s statement_timeout in
-- compute_beach_field_consensus when called per-row by the
-- tg_after_insert_gold_promote_chain trigger. WA inserts 4-10x OR's
-- batch size (158 OSM + ~388 POI vs OR's 44+150) so each per-row
-- consensus call needs more headroom.

begin;

alter function public.compute_beach_field_consensus(bigint)
  set statement_timeout = '60s';

alter function public.promote_canonical_to_consumer_tables(bigint, numeric)
  set statement_timeout = '60s';

alter function public.tg_after_insert_gold_promote_chain()
  set statement_timeout = '120s';

alter function public.publish_canonical_for_fids(bigint[])
  set statement_timeout = '300s';

commit;
