-- 20260527_promote_registry.sql
--
-- Pre-beach infra item 2/4. Make promote_canonical_*_policy data-driven.
--
-- promote_canonical_dog_park_policy() was rewritten 4 times today as
-- new amenity columns were added. Now: a registry table holds the
-- (jsonb_key → target_column, type) mappings, and the promote function
-- iterates the registry. Adding a new amenity = ALTER TABLE + one row
-- in registry, no function rewrite.

begin;

create table if not exists public.promote_registry (
  entity_type    text not null,    -- 'dog_park' | 'beach' | future
  field_group    text not null,    -- 'park_v1' | 'amenities' | etc. (in BEP table)
  jsonb_key      text not null,    -- key in claimed_values jsonb
  target_column  text not null,    -- column in target policy table
  data_type      text not null,    -- 'text' | 'boolean' | 'integer' | 'numeric'
  notes          text,
  updated_at     timestamptz not null default now(),
  primary key (entity_type, field_group, target_column),
  constraint promote_registry_dtype_check
    check (data_type in ('text','boolean','integer','numeric','date','timestamp'))
);

comment on table public.promote_registry is
  'Registry mapping (entity_type, field_group, jsonb_key) → (target_column, type) for '
  'promote_canonical_*_policy functions. Iterated by the generic promote routine to '
  'build a dynamic UPDATE. Adding a new amenity column = ALTER TABLE + one INSERT.';

-- ── Seed: current dog-park park_v1 mappings (15 fields)
insert into public.promote_registry (entity_type, field_group, jsonb_key, target_column, data_type) values
  ('dog_park', 'park_v1', 'hours_open_time',   'hours_open_time',    'text'),
  ('dog_park', 'park_v1', 'hours_close_time',  'hours_close_time',   'text'),
  ('dog_park', 'park_v1', 'hours_text',        'hours_text',         'text'),
  ('dog_park', 'park_v1', 'additional_rules',  'additional_rules',   'text'),
  ('dog_park', 'park_v1', 'has_fence',         'has_fence',          'boolean'),
  ('dog_park', 'park_v1', 'has_drinking_water','has_drinking_water', 'boolean'),
  ('dog_park', 'park_v1', 'double_gate',       'double_gate',        'boolean'),
  ('dog_park', 'park_v1', 'small_dog_area',    'small_dog_area',     'boolean'),
  ('dog_park', 'park_v1', 'large_dog_area',    'large_dog_area',     'boolean'),
  ('dog_park', 'park_v1', 'lighting',          'lighting',           'boolean'),
  ('dog_park', 'park_v1', 'has_shade',         'has_shade',          'boolean'),
  ('dog_park', 'park_v1', 'has_agility',       'has_agility',        'boolean'),
  ('dog_park', 'park_v1', 'has_water_play',    'has_water_play',     'boolean'),
  ('dog_park', 'park_v1', 'has_picnic_tables', 'has_picnic_tables',  'boolean'),
  ('dog_park', 'park_v1', 'surface',           'surface_overlay',    'text'),
  ('dog_park', 'park_v1', 'description',       'description_overlay','text')
on conflict (entity_type, field_group, target_column) do update
  set jsonb_key = excluded.jsonb_key,
      data_type = excluded.data_type,
      updated_at = now();

-- ── Generic promote function — registry-driven, single entity per call
--
-- Builds a dynamic UPDATE that sets each target_column = coalesce(
-- (claimed_values->>jsonb_key)::data_type, existing_value). Default
-- columns (leash defaults, source, source_url, operator_id, etc.) are
-- still hardcoded inline since they don't come from the extractor.
--
-- Param renamed p_park_fid → p_fid; PG requires DROP before rename.

drop function if exists public.promote_canonical_dog_park_policy(bigint);

create or replace function public.promote_canonical_dog_park_policy(p_fid bigint)
returns void language plpgsql as $$
declare
  v_claimed    jsonb;
  v_source_url text;
  v_operator   bigint;
  v_conf       numeric;
  v_set_clause text;
  v_sql        text;
begin
  select claimed_values, source_url, operator_id, confidence
    into v_claimed, v_source_url, v_operator, v_conf
    from public.dog_park_enrichment_provenance
   where dog_park_fid = p_fid
     and field_group = 'park_v1'
     and is_canonical
   order by confidence desc nulls last, updated_at desc
   limit 1;

  if v_claimed is null then return; end if;

  -- Build the SET clause from the registry. Each row → one
  -- "target_column = coalesce(($1->>'jsonb_key')::data_type, ddp.target_column)"
  -- assignment. $1 is bound to v_claimed at EXECUTE time.
  select string_agg(
    format('%I = coalesce(($1->>%L)::%s, ddp.%I)',
           target_column, jsonb_key, data_type, target_column),
    ', '
  ) into v_set_clause
  from public.promote_registry
  where entity_type = 'dog_park' and field_group = 'park_v1';

  if v_set_clause is null then return; end if;

  v_sql := format($SQL$
    update public.dog_park_dog_policy ddp
       set %s,
           leash_policy         = coalesce(ddp.leash_policy, 'off_leash'),
           off_leash_flag       = coalesce(ddp.off_leash_flag, true),
           has_on_leash         = coalesce(ddp.has_on_leash, false),
           has_off_leash        = coalesce(ddp.has_off_leash, true),
           dogs_allowed         = coalesce(ddp.dogs_allowed, 'yes'),
           source               = 'operator_posted_v2',
           source_url           = coalesce($3, ddp.source_url),
           operator_id          = coalesce($4, ddp.operator_id),
           consensus_confidence = greatest(coalesce($5, 0::numeric),
                                           coalesce(ddp.consensus_confidence, 0::numeric)),
           curated_at           = now()
     where ddp.dog_park_fid = $2
  $SQL$, v_set_clause);

  execute v_sql using v_claimed, p_fid, v_source_url, v_operator, v_conf;
end;
$$;

comment on function public.promote_canonical_dog_park_policy is
  'Registry-driven promote — iterates promote_registry for (entity_type=''dog_park'', '
  'field_group=''park_v1'') and builds a dynamic UPDATE. Adding a new extracted amenity '
  'requires only ALTER TABLE + one INSERT into promote_registry — no function rewrite.';

commit;
notify pgrst, 'reload schema';
