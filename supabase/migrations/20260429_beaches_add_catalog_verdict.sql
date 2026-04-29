-- Bridge catalog cascade -> consumer beaches table.
--
-- Adds 3 nullable columns that mirror the cascade-computed dog verdict
-- alongside the existing curated `dogs_allowed` field. HTML still reads
-- `dogs_allowed`; these new columns are reference-only for surfacing
-- catalog-vs-consumer parity.
--
-- Populated by the Dagster `consumer_beaches_sync` asset, which joins
-- each consumer beach to the nearest beach_locations row within 500m
-- and copies the corresponding beach_verdicts row.
--
-- See plan: C:\Users\beach\.claude\plans\ethereal-herding-dusk.md
-- See mart: dbt_dbt.consumer_beach_with_verdict (view of the join)

alter table public.beaches
  add column if not exists dog_verdict_catalog text,
  add column if not exists dog_verdict_catalog_confidence numeric,
  add column if not exists dog_verdict_catalog_computed_at timestamptz;

comment on column public.beaches.dog_verdict_catalog
  is 'Cascade-computed dog verdict (yes/no/restricted/unknown) from beach_verdicts. Reference only; HTML reads dogs_allowed.';
comment on column public.beaches.dog_verdict_catalog_confidence
  is 'Cascade verdict confidence 0-1 from beach_verdicts.dogs_confidence.';
comment on column public.beaches.dog_verdict_catalog_computed_at
  is 'When the cascade verdict was last synced into this row by consumer_beaches_sync.';
