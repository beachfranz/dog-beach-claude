-- 20260525_dog_park_dog_policy_amenities.sql
--
-- B2 of dog-park sub-pipeline Phase B. Adds three amenity columns to
-- dog_park_dog_policy. These are populated by the operator-policy
-- amenity extractor (Phase D2). Nullable — unknown until extracted.
-- has_fence + has_drinking_water already exist on this table.
--
-- Per [[dog-park-subpipeline-scope]] R-section.

begin;

alter table public.dog_park_dog_policy
  add column if not exists double_gate    boolean,
  add column if not exists small_dog_area boolean,
  add column if not exists lighting       boolean;

comment on column public.dog_park_dog_policy.double_gate is
  'Two gates form an airlock entry, preventing escape during entry/exit. Set by amenity extractor (Phase D2).';
comment on column public.dog_park_dog_policy.small_dog_area is
  'Has a separately-fenced area for small dogs (typically <25 lbs). Set by amenity extractor.';
comment on column public.dog_park_dog_policy.lighting is
  'Has artificial lighting for dawn/dusk/evening use. Set by amenity extractor.';

commit;
