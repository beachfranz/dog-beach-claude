-- Add operator_id FK to all four beach source tables. Existing
-- managing_agency / managing_agency_level / managing_agency_source
-- columns stay (audit trail of the prior free-text cascade) but become
-- redundant once operator_id is populated. Drop them in a follow-up
-- migration once nothing reads from them.

alter table public.ccc_access_points  add column if not exists operator_id bigint references public.operators(id);
alter table public.us_beach_points    add column if not exists operator_id bigint references public.operators(id);
alter table public.osm_features       add column if not exists operator_id bigint references public.operators(id);
alter table public.locations_stage    add column if not exists operator_id bigint references public.operators(id);

create index if not exists ccc_access_points_operator_idx on public.ccc_access_points(operator_id);
create index if not exists us_beach_points_operator_idx   on public.us_beach_points(operator_id);
create index if not exists osm_features_operator_idx      on public.osm_features(operator_id);
create index if not exists locations_stage_operator_idx   on public.locations_stage(operator_id);
