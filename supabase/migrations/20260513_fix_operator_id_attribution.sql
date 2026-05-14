-- 20260513_fix_operator_id_attribution.sql
--
-- Fixes the populate_from_pad_us_gold operator-id reverse lookup that was
-- matching on `mng_name` (generic agency code like 'FWS', 'BLM', 'NPS') —
-- which is shared by dozens of operators per state — instead of `unit_name`
-- (specific to one PAD-US unit). With LIMIT 1, the lookup picked whichever
-- operator happened to be first by id, producing wildly wrong attributions:
--   Sporthaven Beach (south OR coast) → Baskett Slough NWR (Salem inland)
--   ...because both have 'FWS' in pad_us_mng_name.
--
-- Three changes:
--   1. populate_from_pad_us_gold: lookup matches on slug derived from
--      unit_name (Pass 3 / federal) OR loc_mang (Passes 4-8). Specific.
--   2. _op_pass3_federal: stop appending generic mng_names to
--      pad_us_mng_name. Only loc_mangs (which are specific manager strings).
--   3. Backfill: strip short uppercase agency codes from existing
--      operators.pad_us_mng_name arrays.
--
-- Re-running populate_from_pad_us_gold(p_fid=NULL) after this migration
-- will replace bad operator_id values via the ON CONFLICT DO UPDATE clause
-- (no need to clear stale rows first).

begin;

-- ────────────────────────────────────────────────────────────────────────
-- 1. Fix populate_from_pad_us_gold operator_id lookup
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.populate_from_pad_us_gold(p_fid bigint default null)
returns integer
language plpgsql
set statement_timeout to '600s'
as $function$
declare rows_touched int;
begin
  with target as (
    select fid,
           coalesce(display_name_override, name) as full_name,
           geom, state,
           geom::geography as geom_geog
      from public.beaches_gold
     where (p_fid is null or fid = p_fid)
       and geom is not null
       and is_active = true
  ),
  candidates as (
    select t.fid, t.state, c.unit_name, c.mng_type, c.mng_name,
      c.loc_mang,  -- ★ NEW: needed for non-federal operator lookup
      c.raw_attrs->>'Pub_Access' as pub_access_code,
      st_contains(c.geom, t.geom::geometry)               as inside,
      st_distance(t.geom_geog, c.geom_geog)               as dist_m,
      st_area(c.geom_geog)                                as area_m2,
      cardinality(public.shared_name_tokens(t.full_name, c.unit_name)) > 0 as name_match
    from target t
    join public.pad_us_units c on st_dwithin(c.geom, t.geom::geometry, 0.005)
   where st_distance(t.geom_geog, c.geom_geog) <= 500
  ),
  with_conf as (
    select *,
      case
        when inside     and name_match                       then 0.90
        when not inside and dist_m <= 100 and name_match     then 0.80
        when inside     and not name_match                   then 0.70
        when not inside and dist_m <= 500 and name_match     then 0.60
        when not inside and dist_m <= 100 and not name_match then 0.45
        else null
      end as confidence
    from candidates
  ),
  scored as (select * from with_conf where confidence is not null),
  ranked as (
    select *, row_number() over (partition by fid
      order by confidence desc, area_m2 asc) as rnk
    from scored
  ),
  best as (select * from ranked where rnk = 1),
  with_type as (
    select *,
      case mng_type
        when 'FED'  then 'federal'
        when 'STAT' then 'state'
        when 'LOC'  then 'local'
        when 'DIST' then 'special_district'
        when 'NGO'  then 'nonprofit'
        when 'PVT'  then 'private'
        when 'TRIB' then 'tribal'
        when 'JNT'  then 'joint'
        else             'unknown'
      end as gov_type,
      case pub_access_code
        when 'OA' then 'public'
        when 'RA' then 'restricted'
        when 'XA' then 'private'
        when 'UK' then 'unknown'
        else case mng_type
          when 'PVT'  then 'private'
          when 'TRIB' then 'restricted'
          when 'UNK'  then 'unknown'
          else             'public'
        end
      end as access_status,
      -- ★ FIXED: match on slug derived from unit_name (Pass 3 federal)
      -- OR loc_mang (Passes 4-8). Deterministic 1:1 from PAD-US unit
      -- to its seeding operator. The old code matched on `mng_name`
      -- (generic agency code) which produced wildly wrong attributions.
      (select o.id from public.operators o
        where o.is_active
          and o.state_code = best.state
          and (
            -- Federal-Pass-3 seeded: slug from unit_name
            (best.unit_name is not null and trim(best.unit_name) <> '' and
             o.slug = public.slugify_agency(best.unit_name || ' (' || best.state || ')'))
            -- Passes 4-8 seeded: slug from loc_mang
            or (best.loc_mang is not null and trim(best.loc_mang) <> '' and
                o.slug = public.slugify_agency(best.loc_mang || ' (' || best.state || ')'))
          )
        limit 1) as operator_id
    from best
  ),
  upsert_governance as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, operator_id, updated_at)
    select fid, 'governance', 'pad_us', confidence,
      jsonb_build_object('type', gov_type,
        'name', public.canonical_agency_name(gov_type, mng_name)),
      operator_id, now()
    from with_type
    where mng_type is not null
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence = excluded.confidence,
          claimed_values = excluded.claimed_values,
          operator_id = excluded.operator_id,
          updated_at = now(),
          is_canonical = false
    returning 1
  ),
  upsert_access as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'access', 'pad_us', confidence,
      jsonb_build_object('status', access_status), now()
    from with_type
    where access_status is not null
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at = now(),
          is_canonical = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from upsert_governance union all select * from upsert_access
  ) _;
  return rows_touched;
end;
$function$;

-- ────────────────────────────────────────────────────────────────────────
-- 2. Fix _op_pass3_federal: stop appending mng_names to pad_us_mng_name
-- ────────────────────────────────────────────────────────────────────────

create or replace function public._op_pass3_federal(p_state text)
returns integer language plpgsql as $function$
declare v_n int := 0; v_state_fp text := public._op_state_fp(p_state);
begin
  with ranked as (
    select public.slugify_agency(pu.unit_name || ' (' || p_state || ')') as slug,
           max(pu.unit_name) as unit_name,
           array_agg(distinct coalesce(pu.mng_name, 'FED')) as mng_names,
           array_agg(distinct pu.loc_mang) filter (
             where public._normalize_agency_text(pu.loc_mang) is not null
           ) as loc_mangs,
           st_multi(st_collectionextract(st_collect(pu.geom), 3)) as state_geom
      from public.pad_us_units pu
     where pu.state = p_state
       and pu.mng_type = 'FED'
       and pu.unit_name is not null and trim(pu.unit_name) <> ''
     group by public.slugify_agency(pu.unit_name || ' (' || p_state || ')')
  ),
  ins as (
    insert into public.operators (
      slug, canonical_name, short_name, aliases, level, subtype,
      state_code, fips_state, geom, pad_us_mng_name, origin_source
    )
    select
      slug, unit_name,
      regexp_replace(unit_name,
        ' (National Seashore|National Wildlife Refuge|National Lakeshore'
        '|National Recreation Area|National Park|National Monument|National Forest'
        '|National Marine Sanctuary|National Estuarine Research Reserve)$',
        '', 'i'),
      array[unit_name] || coalesce(loc_mangs, array[]::text[]),
      'federal',
      lower(coalesce(
        substring(unit_name from
                  '(National Seashore|National Wildlife Refuge|National Lakeshore'
                  '|National Recreation Area|National Park|National Monument'
                  '|National Park|National Monument|National Forest'
                  '|National Marine Sanctuary)'),
        'federal')),
      p_state, v_state_fp, state_geom,
      -- ★ FIXED: only loc_mangs, NOT mng_names. mng_names are generic
      -- agency codes (FWS, USFS, BLM) shared by every unit of that agency
      -- — useless for reverse operator-id lookup, harmful when populators
      -- LIMIT 1 against shared values. unit_name is in `aliases` for
      -- naming continuity; the operator's own canonical_name is the
      -- specific unit name and that's what populate_from_pad_us_gold
      -- now matches on via slug.
      coalesce(loc_mangs, array[]::text[]),
      'seed_federal_widened'
    from ranked
    on conflict (slug) do update set
      geom            = excluded.geom,
      pad_us_mng_name = (select array_agg(distinct n)
                         from unnest(coalesce(public.operators.pad_us_mng_name, '{}'::text[])
                                     || excluded.pad_us_mng_name) n
                         where n is not null),
      aliases         = (select array_agg(distinct a)
                         from unnest(public.operators.aliases || excluded.aliases) a),
      updated_at      = now()
    returning 1
  )
  select count(*) into v_n from ins;
  return v_n;
end $function$;

-- ────────────────────────────────────────────────────────────────────────
-- 3. Backfill: strip short uppercase agency codes from existing arrays.
--    Conservative pattern: 2–5 uppercase letters with no spaces or
--    punctuation. Covers FWS, USFS, BLM, NPS, USACE, USBR, DOD, NRCS,
--    BOEM, OTHF, UNK, USA, USFWS, BPA, ARS, YMCA — every observed code.
--    Preserves any string with spaces, mixed case, or punctuation
--    (which is what specific unit_names / loc_mangs look like).
-- ────────────────────────────────────────────────────────────────────────

update public.operators o
   set pad_us_mng_name = (
     select array_agg(s)
       from unnest(o.pad_us_mng_name) s
      where s is not null
        and s !~ '^[A-Z]{2,5}$'
   ),
   updated_at = now()
 where pad_us_mng_name is not null
   and exists (
     select 1 from unnest(pad_us_mng_name) s where s ~ '^[A-Z]{2,5}$'
   );

commit;

-- Verify (informational):
--   select count(*) from public.operators
--    where exists (select 1 from unnest(pad_us_mng_name) s where s ~ '^[A-Z]{2,5}$');
--   -- expect: 0
