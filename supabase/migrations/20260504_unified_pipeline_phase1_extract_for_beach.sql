-- 20260504_unified_pipeline_phase1_extract_for_beach.sql
--
-- Schema fix to support scripts/extract_for_beach.py — the unified
-- extraction entry point (Layer D / move #3).
--
-- The legacy extractors required source_id NOT NULL because they keyed
-- evidence to a city_policy_sources row. The unified extractor pulls URLs
-- from many sources (park_url_extractions, operators, cpad, OSM tags,
-- etc.), most of which don't correspond to a city_policy_sources entry.
-- Source provenance is captured in source_type instead.

begin;

alter table public.beach_policy_extractions
  alter column source_id drop not null;

commit;
