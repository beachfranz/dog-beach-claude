-- 20260527_dog_park_promote_v2_amenities.sql
--
-- Extend promote_canonical_dog_park_policy() to copy the four new amenity
-- booleans from extracted claimed_values into dog_park_dog_policy:
--   has_shade · has_agility · has_water_play · has_picnic_tables
--
-- The LLM extractor (extract_dog_park_amenities.py) now captures these
-- alongside the existing 13 fields. Total claimed_values shape = 17 fields.

begin;

create or replace function public.promote_canonical_dog_park_policy(p_park_fid bigint)
returns void language plpgsql as $$
declare
  v_claimed    jsonb;
  v_source_url text;
  v_operator   bigint;
  v_conf       numeric;
begin
  select claimed_values, source_url, operator_id, confidence
    into v_claimed, v_source_url, v_operator, v_conf
    from public.dog_park_enrichment_provenance
   where dog_park_fid = p_park_fid
     and field_group = 'park_v1'
     and is_canonical
   order by confidence desc nulls last, updated_at desc
   limit 1;

  if v_claimed is null then
    return;
  end if;

  update public.dog_park_dog_policy ddp
     set leash_policy         = coalesce(ddp.leash_policy, 'off_leash'),
         off_leash_flag       = coalesce(ddp.off_leash_flag, true),
         has_on_leash         = coalesce(ddp.has_on_leash, false),
         has_off_leash        = coalesce(ddp.has_off_leash, true),
         dogs_allowed         = coalesce(ddp.dogs_allowed, 'yes'),
         hours_open_time      = coalesce(v_claimed->>'hours_open_time',                ddp.hours_open_time),
         hours_close_time     = coalesce(v_claimed->>'hours_close_time',               ddp.hours_close_time),
         hours_text           = coalesce(v_claimed->>'hours_text',                     ddp.hours_text),
         additional_rules     = coalesce(v_claimed->>'additional_rules',               ddp.additional_rules),
         has_fence            = coalesce((v_claimed->>'has_fence')::boolean,           ddp.has_fence),
         has_drinking_water   = coalesce((v_claimed->>'has_drinking_water')::boolean,  ddp.has_drinking_water),
         double_gate          = coalesce((v_claimed->>'double_gate')::boolean,         ddp.double_gate),
         small_dog_area       = coalesce((v_claimed->>'small_dog_area')::boolean,      ddp.small_dog_area),
         large_dog_area       = coalesce((v_claimed->>'large_dog_area')::boolean,      ddp.large_dog_area),
         lighting             = coalesce((v_claimed->>'lighting')::boolean,            ddp.lighting),
         -- NEW: 4 amenities Franz added 2026-05-27 LATE
         has_shade            = coalesce((v_claimed->>'has_shade')::boolean,           ddp.has_shade),
         has_agility          = coalesce((v_claimed->>'has_agility')::boolean,         ddp.has_agility),
         has_water_play       = coalesce((v_claimed->>'has_water_play')::boolean,      ddp.has_water_play),
         has_picnic_tables    = coalesce((v_claimed->>'has_picnic_tables')::boolean,   ddp.has_picnic_tables),
         surface_overlay      = coalesce(v_claimed->>'surface',                        ddp.surface_overlay),
         description_overlay  = coalesce(v_claimed->>'description',                    ddp.description_overlay),
         source               = 'operator_posted_v2',
         source_url           = coalesce(v_source_url,                                 ddp.source_url),
         operator_id          = coalesce(v_operator,                                   ddp.operator_id),
         consensus_confidence = greatest(coalesce(v_conf, 0),
                                         coalesce(ddp.consensus_confidence, 0)),
         curated_at           = now()
   where ddp.dog_park_fid = p_park_fid;
end;
$$;

comment on function public.promote_canonical_dog_park_policy is
  'Read the canonical park_v1 dpep row, merge its 17-field claimed_values jsonb into '
  'dog_park_dog_policy. Includes has_shade/agility/water_play/picnic_tables (added 2026-05-27). '
  'Leash fields pinned at off-leash defaults (dog park = off-leash by definition).';

commit;
