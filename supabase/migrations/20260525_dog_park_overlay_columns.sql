-- 20260525_dog_park_overlay_columns.sql
--
-- Phase DP-A4 (REFINED 2026-05-25 LATE): backward-design from the 14
-- consumer-page fields. Single field_group ('park_v1') per park = single
-- source per park = no consensus needed for v1.
--
-- Adds:
--   1. surface_overlay  + description_overlay  columns on dog_park_dog_policy
--      (Option A: keep dog_parks_gold raw; overlay wins on consumer read).
--   2. promote_canonical_dog_park_policy() — simplified to read ONE
--      field_group ('park_v1') per park; drops leash/access/dogs branches
--      since those are definitional defaults.

begin;

alter table public.dog_park_dog_policy
  add column if not exists surface_overlay     text,
  add column if not exists description_overlay text,
  add column if not exists large_dog_area      boolean;

comment on column public.dog_park_dog_policy.surface_overlay is
  'Extracted/curated surface (grass / dirt / wood_chips / sand / decomposed_granite / mixed / artificial_turf). Wins over dog_parks_gold.surface (raw OSM) on consumer read via coalesce. OSM raw stays untouched.';

comment on column public.dog_park_dog_policy.description_overlay is
  'Extracted/curated park description (1-3 sentences, honest prose per [[honest-brand-prefers-candor]]). Wins over dog_parks_gold.description on consumer read via coalesce.';

comment on column public.dog_park_dog_policy.large_dog_area is
  'True if the park has a designated large-dog area. Companion to small_dog_area. Some parks have one, the other, or both.';

-- Refined promote: ONE field_group ('park_v1'), all 14 fields in one
-- claimed_values jsonb. Leash fields pinned at defaults. Source set to
-- 'operator_posted_v2' so the consumer surface picks up the "Verified" badge.
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
     set -- Leash fields pinned (dog park = off-leash by definition; not extracted)
         leash_policy         = coalesce(ddp.leash_policy, 'off_leash'),
         off_leash_flag       = coalesce(ddp.off_leash_flag, true),
         has_on_leash         = coalesce(ddp.has_on_leash, false),
         has_off_leash        = coalesce(ddp.has_off_leash, true),
         dogs_allowed         = coalesce(ddp.dogs_allowed, 'yes'),
         -- Extracted hours
         hours_open_time      = coalesce(v_claimed->>'hours_open_time',                ddp.hours_open_time),
         hours_close_time     = coalesce(v_claimed->>'hours_close_time',               ddp.hours_close_time),
         hours_text           = coalesce(v_claimed->>'hours_text',                     ddp.hours_text),
         -- Extracted amenities
         additional_rules     = coalesce(v_claimed->>'additional_rules',               ddp.additional_rules),
         has_fence            = coalesce((v_claimed->>'has_fence')::boolean,           ddp.has_fence),
         has_drinking_water   = coalesce((v_claimed->>'has_drinking_water')::boolean,  ddp.has_drinking_water),
         double_gate          = coalesce((v_claimed->>'double_gate')::boolean,         ddp.double_gate),
         small_dog_area       = coalesce((v_claimed->>'small_dog_area')::boolean,      ddp.small_dog_area),
         large_dog_area       = coalesce((v_claimed->>'large_dog_area')::boolean,      ddp.large_dog_area),
         lighting             = coalesce((v_claimed->>'lighting')::boolean,            ddp.lighting),
         -- Extracted overlays (raw OSM/CPAD stays untouched)
         surface_overlay      = coalesce(v_claimed->>'surface',                        ddp.surface_overlay),
         description_overlay  = coalesce(v_claimed->>'description',                    ddp.description_overlay),
         -- Provenance
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
  'Read the canonical park_v1 dpep row for a park, merge its 14-field claimed_values jsonb into dog_park_dog_policy. Leash fields pinned at off-leash defaults (dog park = off-leash by definition).';

commit;
