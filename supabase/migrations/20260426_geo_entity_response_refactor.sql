-- Refactor cpad_unit_response into a polymorphic geo_entity_response
-- table that can hold answers about CPAD units, CCC access points,
-- off-leash dog beaches, and (future) BCDC access points using the
-- same shape. Discriminator: (entity_type, entity_id).
--
-- Migration steps:
--   1. Add entity_type + entity_id columns
--   2. Backfill from existing cpad_unit_id
--   3. Enforce NOT NULL + CHECK
--   4. Drop cpad_unit_id and its indexes
--   5. Rename table + indexes
--   6. Recreate unique key + indexes
--   7. Recreate the _current view, polymorphic
--   8. Update the cpad_units_near_active_beaches RPC

-- 1-3: add + backfill + constrain
alter table public.cpad_unit_response
  add column if not exists entity_type text,
  add column if not exists entity_id   bigint;

update public.cpad_unit_response
   set entity_type = 'cpad',
       entity_id   = cpad_unit_id
 where entity_type is null;

alter table public.cpad_unit_response
  alter column entity_type set not null,
  alter column entity_id   set not null;

alter table public.cpad_unit_response
  drop constraint if exists cpad_unit_response_entity_type_check;
alter table public.cpad_unit_response
  add constraint cpad_unit_response_entity_type_check
    check (entity_type in ('cpad','ccc','off_leash','bcdc'));

-- 4: drop old cpad_unit_id + dependent objects (view, indexes)
drop view if exists public.cpad_unit_response_current;
drop index if exists public.cpad_unit_response_unit_idx;
alter table public.cpad_unit_response drop column if exists cpad_unit_id;

-- 5: rename table + supporting indexes
alter table public.cpad_unit_response rename to geo_entity_response;
alter index if exists cpad_unit_response_status_idx rename to geo_entity_response_status_idx;
alter index if exists cpad_unit_response_url_idx    rename to geo_entity_response_url_idx;
alter index if exists cpad_unit_response_scope_idx  rename to geo_entity_response_scope_idx;
alter index if exists cpad_unit_response_jsonb_idx  rename to geo_entity_response_jsonb_idx;

-- 6: new uniqueness key + entity index
drop index if exists public.cpad_unit_response_unique;
create unique index if not exists geo_entity_response_unique
  on public.geo_entity_response
  (entity_type, entity_id, source_url, response_scope, scraped_at);
create index if not exists geo_entity_response_entity_idx
  on public.geo_entity_response (entity_type, entity_id);

-- 7: replace _current view (polymorphic)
drop view if exists public.cpad_unit_response_current;
create or replace view public.geo_entity_response_current as
  select distinct on (entity_type, entity_id, response_scope)
    entity_type, entity_id, response_scope, source_url,
    response_value, response_reason, response_confidence, extracted_at
  from public.geo_entity_response
  where fetch_status in ('success','manual','llm_knowledge') and response_value is not null
  order by entity_type, entity_id, response_scope,
           response_confidence desc nulls last, extracted_at desc;

-- 8: rebuild the CPAD map RPC to read from the polymorphic view
drop function if exists public.cpad_units_near_active_beaches(integer);
create or replace function public.cpad_units_near_active_beaches(
  p_meters integer default 200
)
returns table (
  unit_id           integer,
  unit_name         text,
  mng_agncy         text,
  mng_ag_lev        text,
  park_url          text,
  agncy_web         text,
  geom_geojson      jsonb,
  dogs_allowed      text,
  dogs_confidence   numeric
)
language sql stable security definer as $$
  select
    cc.unit_id, cc.unit_name, cc.mng_agncy, cc.mng_ag_lev,
    cc.park_url, cc.agncy_web, cc.geom_geojson,
    d.response_value      as dogs_allowed,
    d.response_confidence as dogs_confidence
  from public.cpad_units_coastal cc
  left join public.geo_entity_response_current d
    on d.entity_type    = 'cpad'
   and d.entity_id      = cc.unit_id
   and d.response_scope = 'dogs_allowed';
$$;
grant execute on function public.cpad_units_near_active_beaches(integer) to anon, authenticated;
