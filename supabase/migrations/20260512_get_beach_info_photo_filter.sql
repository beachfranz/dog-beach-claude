-- get_beach_info: curated-first photo filter (option B).
--
-- Current behavior: returns ALL photos (no curation filter), `limit 6`
-- is a no-op because it sits outside jsonb_agg. Beaches with 30 photos
-- return all 30 on the public detail/beach pages, including uncurated
-- garbage.
--
-- New behavior (option B per 2026-05-12 product call):
--   1. Curated photos (curated_at IS NOT NULL) sort first
--   2. Then uncurated photos that meet the model's keep threshold
--      (predicted_keep_prob >= 0.65 — matches the post-hoc rule layer's
--      surf/active_people floor; all dog photos pass via the 0.85 floor)
--   3. Cap at 6 total
--
-- FOR LAUNCH (option C): un-comment the source='cdpr' lines below to
-- auto-pass CDPR park gallery photos as a middle tier (above the model
-- threshold, below human-curated). CDPR is a trusted source — their
-- gallery is implicitly curated by CA State Parks themselves.

create or replace function public.get_beach_info(p_fid bigint)
returns jsonb
language sql
stable
security definer
set search_path to 'public'
as $$
  with selected_photos as (
    select p.source, p.image_url, p.thumb_url, p.attribution, p.license,
           p.page_url, p.captured_at, p.curated_at,
           p.sort_order, p.id,
           coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) keep_prob
      from public.beach_photos p
     where p.arena_group_id = p_fid
       and (
         p.curated_at is not null
         -- or p.source = 'cdpr'   -- UNCOMMENT FOR LAUNCH (option C)
         or coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) >= 0.65
       )
     order by
       (p.curated_at is null)::int asc,        -- curated (false=0) first
       -- case when p.source='cdpr' then 0 else 1 end,  -- UNCOMMENT FOR LAUNCH
       coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) desc,
       p.sort_order asc,
       p.id asc
     limit 6
  )
  select jsonb_build_object(
    'beach', jsonb_build_object(
      'fid',           g.fid,
      'location_id',   g.location_id,
      'name',          g.name,
      'display_name',  coalesce(g.display_name_override, g.name),
      'county',        g.county_name,
      'state',         g.state,
      'address',       g.address,
      'website',       g.website,
      'timezone',      g.timezone,
      'lat',           st_y(g.geom),
      'lng',           st_x(g.geom),
      'geom',          st_asgeojson(g.geom)::jsonb,
      'open_time',     g.open_time,
      'close_time',    g.close_time,
      'is_scoreable',  g.is_scoreable
    ),
    'dog_policy', case when bdp.arena_group_id is not null then
      jsonb_build_object(
        'dogs_allowed',           bdp.dogs_allowed,
        'leash_policy',           bdp.leash_policy,
        'has_off_leash',          bdp.has_off_leash,
        'has_on_leash',           bdp.has_on_leash,
        'access_rule',            bdp.access_rule,
        'off_leash_flag',         bdp.off_leash_flag,
        'dogs_prohibited_start',  bdp.dogs_prohibited_start,
        'dogs_prohibited_end',    bdp.dogs_prohibited_end,
        'zone_rules',             bdp.zone_rules,
        'notes',                  bdp.notes
      )
    end,
    'amenities', case when ba.arena_group_id is not null then
      to_jsonb(ba) - 'arena_group_id'
    end,
    'description', case when bd.arena_group_id is not null then
      jsonb_build_object(
        'text', bd.description,
        'generated_at', bd.generated_at
      )
    end,
    'photos', coalesce((
      select jsonb_agg(jsonb_build_object(
        'source',      sp.source,
        'image_url',   sp.image_url,
        'thumb_url',   sp.thumb_url,
        'attribution', sp.attribution,
        'license',     sp.license,
        'page_url',    sp.page_url,
        'captured_at', sp.captured_at,
        'curated',     sp.curated_at is not null,
        'keep_prob',   sp.keep_prob
      ) order by (sp.curated_at is null)::int, sp.keep_prob desc, sp.sort_order, sp.id)
      from selected_photos sp
    ), '[]'::jsonb)
  )
  from public.beaches_gold g
  left join public.beach_dog_policy bdp on bdp.arena_group_id = g.group_id
  left join public.beach_amenities  ba  on ba.arena_group_id  = g.group_id
  left join public.beach_descriptions bd on bd.arena_group_id = g.fid
  where g.fid = p_fid and g.is_active
  limit 1;
$$;
