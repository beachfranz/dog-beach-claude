-- 20260504_unified_pipeline_phase1_canonical_promoter.sql
--
-- Unified pipeline phase 1: auto-promote canonical evidence into the
-- consumer-facing tables (beach_dog_policy, beach_amenities, beaches_gold).
-- Closes the gap left by harmony — the resolver picks canonical evidence
-- but nothing currently propagates it to where HTML reads.
--
-- Design (per Franz, 2026-05-04): promotion to canonical does NOT need to
-- be curator-mediated. Resolver picks are trustworthy when the evidence
-- meets confidence + source-whitelist gates; curator role becomes
-- exception-handler + reviewer + cold-start filler, not the only path.
--
-- Safeguards:
--   - source='manual_curator' rows are NEVER overwritten (curator wins always)
--   - source='auto_promoted_from_resolver' is the new tag (distinct from the
--     legacy 'auto_promoted_from_production' which came from the older
--     promote_production_to_beach_dog_policy.py script — same intent,
--     different mechanism)
--   - per-field source whitelist (only promote from sources at/above
--     calibration threshold)
--   - confidence floor (default 0.5)
--   - idempotent UPSERT — only updates when value actually changed
--   - normalizers map evidence vocab → canonical enum values
--
-- Trigger sequence in the unified pipeline:
--   populate_polygon_containment_gold      (Layer C)
--   populate_from_*_gold (cpad, research, park_url, park_operators, ...)  (Layer D source)
--   _resolve_dogs_gold / _resolve_practical_gold / _resolve_field_group_gold('access')
--   promote_canonical_to_consumer_tables   (Layer D promote — THIS FILE)
--
-- Curator override: any beach-editor-gold.html save sets source='manual_curator'
-- which permanently blocks auto-overwrites for that row.

begin;

-- ─────────────────────────────────────────────────────────────────────────
-- Normalizers: evidence vocab → canonical enum values
-- ─────────────────────────────────────────────────────────────────────────

create or replace function public._norm_dogs_allowed(p_value text)
returns text language sql immutable as $$
  select case lower(coalesce(p_value, ''))
    when 'yes'        then 'yes'
    when 'no'         then 'no'
    when 'restricted' then 'yes'   -- "any allowed = yes" per Franz's rule
    when 'seasonal'   then 'seasonal'
    when 'mixed'      then 'mixed'
    when 'off_leash'  then 'yes'
    when 'on_leash'   then 'yes'
    when 'prohibited' then 'no'
    else null  -- unclear / unknown / null → don't promote
  end;
$$;

create or replace function public._norm_leash_policy(p_value text)
returns text language sql immutable as $$
  select case lower(coalesce(p_value, ''))
    when 'required'        then 'on_leash'
    when 'optional'        then 'off_leash'
    when 'mixed_by_zone'   then 'mixed'
    when 'varies_by_time'  then 'varies_by_time'
    when 'on_leash'        then 'on_leash'
    when 'off_leash'       then 'off_leash'
    when 'mixed'           then 'mixed'
    else null
  end;
$$;

create or replace function public._norm_bool(p_value jsonb)
returns boolean language sql immutable as $$
  select case
    when p_value is null then null
    when jsonb_typeof(p_value) = 'boolean' then (p_value)::text::boolean
    when lower(p_value::text) in ('"true"','"yes"','"y"') then true
    when lower(p_value::text) in ('"false"','"no"','"n"') then false
    else null
  end;
$$;

-- "9:00 a.m." / "9 AM" / "09:00" / "10pm" → '09:00' / '21:00' (HH:MM as text)
create or replace function public._norm_time_text(p_value text)
returns text language plpgsql immutable as $$
declare
  s text;
  h int; m int;
  is_pm bool := false;
begin
  if p_value is null or trim(p_value) = '' then return null; end if;
  s := lower(regexp_replace(trim(p_value), '\s+', '', 'g'));
  s := replace(replace(s, 'a.m.', 'am'), 'p.m.', 'pm');
  is_pm := s ~ 'pm';
  s := regexp_replace(s, '(am|pm)$', '');
  -- accept "9", "9:00", "09", "09:00"
  if s ~ '^[0-9]{1,2}:[0-9]{2}$' then
    h := split_part(s, ':', 1)::int;
    m := split_part(s, ':', 2)::int;
  elsif s ~ '^[0-9]{1,2}$' then
    h := s::int; m := 0;
  else
    return null;
  end if;
  if is_pm and h < 12 then h := h + 12; end if;
  if not is_pm and h = 12 then h := 0; end if;
  if h < 0 or h > 23 or m < 0 or m > 59 then return null; end if;
  return to_char(h, 'FM00') || ':' || to_char(m, 'FM00');
end;
$$;

-- ─────────────────────────────────────────────────────────────────────────
-- Promoter: canonical dogs evidence → beach_dog_policy
-- ─────────────────────────────────────────────────────────────────────────

create or replace function public.promote_canonical_dogs_to_beach_dog_policy(
  p_fid bigint default null,
  p_min_confidence numeric default 0.5
)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint) as $$
declare
  ins int := 0; upd int := 0; skp int := 0;
begin
  with canon as (
    select e.gold_fid as fid, e.source, e.confidence, e.claimed_values
      from public.beach_enrichment_provenance e
      join public.beaches_gold g on g.fid = e.gold_fid  -- skip orphan evidence
     where e.field_group = 'dogs'
       and e.is_canonical = true
       and (p_fid is null or e.gold_fid = p_fid)
       and e.confidence >= p_min_confidence
       and e.source = any(array['manual','llm','park_url','park_operators',
                                'research','old_school_llm','json_explode'])
  ),
  derived as (
    select c.fid, c.source, c.confidence,
           public._norm_dogs_allowed(c.claimed_values->>'allowed')        as new_dogs_allowed,
           public._norm_leash_policy(c.claimed_values->>'leash_required') as new_leash_policy,
           public._norm_bool(c.claimed_values->'off_leash_exists')        as new_off_leash_flag,
           public._norm_time_text(split_part(c.claimed_values->>'time_prohibited_hours','-',1))
                                                                          as new_prohib_start,
           public._norm_time_text(split_part(c.claimed_values->>'time_prohibited_hours','-',2))
                                                                          as new_prohib_end,
           c.claimed_values->>'areas_evidence'  as new_dogs_allowed_areas,
           c.claimed_values->>'areas_boundaries' as new_areas_boundaries
      from canon c
  ),
  -- Skip beaches where curator wrote manually
  protected as (
    select arena_group_id from public.beach_dog_policy where source = 'manual_curator'
  ),
  upsert as (
    insert into public.beach_dog_policy
      (arena_group_id, dogs_allowed, leash_policy, off_leash_flag,
       dogs_prohibited_start, dogs_prohibited_end, dogs_allowed_areas,
       source, curated_at, notes)
    select
      d.fid,
      d.new_dogs_allowed,
      d.new_leash_policy,
      d.new_off_leash_flag,
      d.new_prohib_start,
      d.new_prohib_end,
      coalesce(d.new_dogs_allowed_areas, d.new_areas_boundaries),
      'auto_promoted_from_resolver',
      now(),
      format('Promoted from canonical evidence (source=%s, confidence=%s)',
             d.source, d.confidence)
    from derived d
    where d.fid not in (select arena_group_id from protected)
      and (
        d.new_dogs_allowed is not null
        or d.new_leash_policy is not null
        or d.new_off_leash_flag is not null
        or d.new_prohib_start is not null
        or d.new_dogs_allowed_areas is not null
      )
    on conflict (arena_group_id) do update
      set dogs_allowed          = excluded.dogs_allowed,
          leash_policy          = excluded.leash_policy,
          off_leash_flag        = excluded.off_leash_flag,
          dogs_prohibited_start = excluded.dogs_prohibited_start,
          dogs_prohibited_end   = excluded.dogs_prohibited_end,
          dogs_allowed_areas    = excluded.dogs_allowed_areas,
          source                = excluded.source,
          curated_at            = now(),
          notes                 = excluded.notes
      where beach_dog_policy.source <> 'manual_curator'  -- defense-in-depth
        and (
          beach_dog_policy.dogs_allowed          is distinct from excluded.dogs_allowed
       or beach_dog_policy.leash_policy          is distinct from excluded.leash_policy
       or beach_dog_policy.off_leash_flag        is distinct from excluded.off_leash_flag
       or beach_dog_policy.dogs_prohibited_start is distinct from excluded.dogs_prohibited_start
       or beach_dog_policy.dogs_prohibited_end   is distinct from excluded.dogs_prohibited_end
       or beach_dog_policy.dogs_allowed_areas    is distinct from excluded.dogs_allowed_areas
        )
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted),
         count(*) filter (where not inserted),
         0
    into ins, upd, skp
    from upsert;

  return query select ins::bigint, upd::bigint, skp::bigint;
end;
$$ language plpgsql;

-- ─────────────────────────────────────────────────────────────────────────
-- Promoter: canonical practical evidence → beach_amenities + beaches_gold
-- ─────────────────────────────────────────────────────────────────────────

create or replace function public.promote_canonical_practical_to_beach_amenities(
  p_fid bigint default null,
  p_min_confidence numeric default 0.5
)
returns table(rows_inserted bigint, rows_updated bigint, gold_open_close_set bigint) as $$
declare
  ins int := 0; upd int := 0; gold_set int := 0;
begin
  with canon as (
    select e.gold_fid as fid, e.source, e.confidence, e.claimed_values
      from public.beach_enrichment_provenance e
      join public.beaches_gold g on g.fid = e.gold_fid  -- skip orphan evidence
     where e.field_group = 'practical'
       and e.is_canonical = true
       and (p_fid is null or e.gold_fid = p_fid)
       and e.confidence >= p_min_confidence
       and e.source = any(array['manual','llm','park_url','park_operators',
                                'old_school_llm','json_explode','ccc'])
  ),
  protected as (
    select arena_group_id from public.beach_amenities where source = 'manual_curator'
  ),
  upsert_amen as (
    insert into public.beach_amenities
      (arena_group_id, has_restrooms, has_lifeguards, has_showers,
       has_drinking_water, has_disabled_access, has_food, has_fire_pits,
       has_picnic_area, parking_type, hours_text, source, curated_at, notes)
    select
      c.fid,
      public._norm_bool(c.claimed_values->'has_restrooms'),
      public._norm_bool(c.claimed_values->'has_lifeguards'),
      public._norm_bool(c.claimed_values->'has_showers'),
      public._norm_bool(c.claimed_values->'has_drinking_water'),
      public._norm_bool(c.claimed_values->'has_disabled_access'),
      public._norm_bool(c.claimed_values->'has_food'),
      public._norm_bool(c.claimed_values->'has_fire_pits'),
      public._norm_bool(c.claimed_values->'has_picnic_area'),
      c.claimed_values->>'parking_type',
      coalesce(
        c.claimed_values->>'hours_text',
        case
          when (c.claimed_values->>'hours_open') is not null
            or (c.claimed_values->>'hours_close') is not null
          then trim(both '-' from format('%s-%s',
            coalesce(c.claimed_values->>'hours_open',''),
            coalesce(c.claimed_values->>'hours_close','')))
        end
      ),
      'auto_promoted_from_resolver',
      now(),
      format('Promoted from canonical evidence (source=%s, confidence=%s)',
             c.source, c.confidence)
    from canon c
    where c.fid not in (select arena_group_id from protected)
    on conflict (arena_group_id) do update
      set has_restrooms        = excluded.has_restrooms,
          has_lifeguards       = excluded.has_lifeguards,
          has_showers          = excluded.has_showers,
          has_drinking_water   = excluded.has_drinking_water,
          has_disabled_access  = excluded.has_disabled_access,
          has_food             = excluded.has_food,
          has_fire_pits        = excluded.has_fire_pits,
          has_picnic_area      = excluded.has_picnic_area,
          parking_type         = coalesce(excluded.parking_type, beach_amenities.parking_type),
          hours_text           = coalesce(excluded.hours_text, beach_amenities.hours_text),
          source               = excluded.source,
          curated_at           = now(),
          notes                = excluded.notes
      where beach_amenities.source <> 'manual_curator'
    returning (xmax = 0) as inserted, arena_group_id
  ),
  -- beaches_gold.open_time / close_time are TEXT columns (HH:MM strings),
  -- not TIME — _norm_time_text returns the right shape directly.
  parsed_hours as (
    select c.fid,
           public._norm_time_text(c.claimed_values->>'hours_open')  as t_open,
           public._norm_time_text(c.claimed_values->>'hours_close') as t_close
      from canon c
  ),
  -- Promote hours_open/hours_close into beaches_gold.open_time/close_time
  --   Only fill nulls (don't overwrite existing values)
  upd_gold as (
    update public.beaches_gold g
       set open_time  = coalesce(g.open_time,  ph.t_open),
           close_time = coalesce(g.close_time, ph.t_close)
      from parsed_hours ph
     where g.fid = ph.fid
       and (
         (g.open_time  is null and ph.t_open  is not null)
       or (g.close_time is null and ph.t_close is not null)
       )
    returning g.fid
  )
  select count(*) filter (where inserted),
         count(*) filter (where not inserted),
         (select count(*) from upd_gold)
    into ins, upd, gold_set
    from upsert_amen;

  return query select ins::bigint, upd::bigint, gold_set::bigint;
end;
$$ language plpgsql;

-- ─────────────────────────────────────────────────────────────────────────
-- Orchestrator: run all canonical promoters
-- ─────────────────────────────────────────────────────────────────────────

create or replace function public.promote_canonical_to_consumer_tables(
  p_fid bigint default null,
  p_min_confidence numeric default 0.5
)
returns table(
  field_group text,
  rows_inserted bigint,
  rows_updated bigint,
  side_effects bigint
) as $$
begin
  return query
    select 'dogs'::text, r.rows_inserted, r.rows_updated, 0::bigint
      from public.promote_canonical_dogs_to_beach_dog_policy(p_fid, p_min_confidence) r;
  return query
    select 'practical'::text, r.rows_inserted, r.rows_updated, r.gold_open_close_set
      from public.promote_canonical_practical_to_beach_amenities(p_fid, p_min_confidence) r;
end;
$$ language plpgsql;

comment on function public.promote_canonical_to_consumer_tables(bigint, numeric) is
  'Phase 1 unified pipeline (2026-05-04). Auto-promote canonical evidence rows '
  'from beach_enrichment_provenance into the consumer-facing tables '
  '(beach_dog_policy, beach_amenities, beaches_gold open_time/close_time). '
  'Conservative defaults: never overwrites manual_curator rows; per-field '
  'source whitelist; confidence floor; idempotent. See docs/unified-pipeline.md.';

commit;
