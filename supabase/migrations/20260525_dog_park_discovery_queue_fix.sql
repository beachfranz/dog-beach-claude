-- 20260525_dog_park_discovery_queue_fix.sql
--
-- Fix: dog_park_discovery_queue's unique constraint was declared deferrable;
-- PostgreSQL rejects ON CONFLICT against deferrable constraints
-- ("ObjectNotInPrerequisiteState"). Drop + recreate as non-deferrable.

begin;

alter table public.dog_park_discovery_queue
  drop constraint if exists dog_park_discovery_queue_operator_id_catalog_park_url_key;

alter table public.dog_park_discovery_queue
  add constraint dog_park_discovery_queue_operator_id_catalog_park_url_key
  unique (operator_id, catalog_park_url);

commit;
