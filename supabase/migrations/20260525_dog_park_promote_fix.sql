-- 20260525_dog_park_promote_fix.sql
--
-- Fix promote_canonical_dog_park_policy() — the v1 migration cast jsonb
-- text→boolean for dogs_allowed and jsonb text→time for hours_open_time /
-- hours_close_time, but the actual column types are text. Replace with
-- text-preserving casts.

begin;

create or replace function public.promote_canonical_dog_park_policy(p_park_fid bigint)
returns void language plpgsql as $$
declare
  v_amenities       jsonb;
  v_leash           jsonb;
  v_hours           jsonb;
  v_access          jsonb;
  v_dogs            jsonb;
  v_source_url      text;
  v_operator_id     bigint;
  v_max_conf        numeric;
begin
  select claimed_values, source_url, operator_id, confidence
    into v_amenities, v_source_url, v_operator_id, v_max_conf
    from public.dog_park_enrichment_provenance
   where dog_park_fid = p_park_fid and field_group = 'amenities' and is_canonical;

  select claimed_values into v_leash
    from public.dog_park_enrichment_provenance
   where dog_park_fid = p_park_fid and field_group = 'leash' and is_canonical;

  select claimed_values into v_hours
    from public.dog_park_enrichment_provenance
   where dog_park_fid = p_park_fid and field_group = 'hours' and is_canonical;

  select claimed_values into v_access
    from public.dog_park_enrichment_provenance
   where dog_park_fid = p_park_fid and field_group = 'access' and is_canonical;

  select claimed_values into v_dogs
    from public.dog_park_enrichment_provenance
   where dog_park_fid = p_park_fid and field_group = 'dogs' and is_canonical;

  if v_amenities is null and v_leash is null and v_hours is null
     and v_access is null and v_dogs is null then
    return;
  end if;

  update public.dog_park_dog_policy ddp
     set dogs_allowed       = coalesce(v_dogs->>'dogs_allowed',                       ddp.dogs_allowed),
         leash_policy       = coalesce(v_leash->>'leash_policy',                      ddp.leash_policy),
         off_leash_flag     = coalesce((v_leash->>'off_leash_flag')::boolean,         ddp.off_leash_flag),
         has_on_leash       = coalesce((v_leash->>'has_on_leash')::boolean,           ddp.has_on_leash),
         has_off_leash      = coalesce((v_leash->>'has_off_leash')::boolean,          ddp.has_off_leash),
         hours_open_time    = coalesce(v_hours->>'hours_open_time',                   ddp.hours_open_time),
         hours_close_time   = coalesce(v_hours->>'hours_close_time',                  ddp.hours_close_time),
         hours_text         = coalesce(v_hours->>'hours_text',                        ddp.hours_text),
         additional_rules   = coalesce(v_amenities->>'additional_rules',              ddp.additional_rules),
         has_fence          = coalesce((v_amenities->>'has_fence')::boolean,          ddp.has_fence),
         has_drinking_water = coalesce((v_amenities->>'has_drinking_water')::boolean, ddp.has_drinking_water),
         double_gate        = coalesce((v_amenities->>'double_gate')::boolean,        ddp.double_gate),
         small_dog_area     = coalesce((v_amenities->>'small_dog_area')::boolean,     ddp.small_dog_area),
         lighting           = coalesce((v_amenities->>'lighting')::boolean,           ddp.lighting),
         source             = case
                                when v_amenities is not null or v_leash is not null or v_hours is not null
                                  then 'operator_posted_v2'
                                else ddp.source
                              end,
         source_url         = coalesce(v_source_url,                                  ddp.source_url),
         operator_id        = coalesce(v_operator_id,                                 ddp.operator_id),
         consensus_confidence = greatest(coalesce(v_max_conf, 0),
                                         coalesce(ddp.consensus_confidence, 0)),
         curated_at         = now()
   where ddp.dog_park_fid = p_park_fid;
end;
$$;

commit;
