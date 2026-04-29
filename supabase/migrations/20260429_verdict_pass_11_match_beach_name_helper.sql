-- Pass 11 — single canonical name-matching helper (match_beach_name)
-- replacing the strip-first-then-compare pattern that was duplicated
-- across compute_dogs_verdict_core, the CPAD unit lookup, the
-- precompute table, and the cross-source dedupe matchers.
--
-- Why now: same bug class kept resurfacing through Passes 9, 10 and
-- the Coronado / Pismo / Ocean Beach Dog Beach cases. The root issue
-- was that clean_beach_name() strips "state beach", "park", "beach",
-- etc. BEFORE comparison, so beaches with names that reduce to one
-- common token (Pismo State Beach → "pismo", Coronado Beach →
-- "coronado") were matching anything else containing that token.
--
-- New design — tiered scoring, full-name first, stripped name as
-- last-resort fallback. Single canonical helper that all call sites
-- can delegate to.
--
--   1.00  full-name exact (after lowercase + trim + whitespace
--                          collapse + abbrev expansion + apostrophe
--                          / & / hyphen normalization)
--   0.90  full-name substring (beach contains exception, multi-word
--                              exception only — single-word too generic)
--   0.85  full-name trigram >= 0.85 (multi-word only)
--   0.80  cleaned-name exact (after stripping)
--   0.70  cleaned-name substring (multi-word cleaned exception only)
--   0.65  cleaned-name trigram >= 0.65 (multi-word cleaned exception only)
--   0     no match
--
-- Critically NOT included: the symmetric "exception contains beach"
-- substring direction. That was the Pass 9 bug — it's the direction
-- where sub-area exception names like "Pismo Dunes Natural Preserve"
-- accidentally swallow the parent "Pismo State Beach". Dropped from
-- both full-name and cleaned-name tiers.
--
-- Dry-run before this migration confirmed:
--   * 41/42 beaches with active operator_exception keep the same matches
--   * 1 beach gains a more-specific match (no verdict change)
--   * 0 beaches lose a current match
--   * 3 of 993 operator_default-only beaches gain a NEW correct
--     exception match — including Santa Cruz Main Beach (silent
--     false positive: should be 'no', was 'yes')
--   * 13 named regression cases (Pismo, Coronado Dog Beach, OB Dog
--     Beach, Goat Rock, Salmon Creek, La Jolla Cove, Will Rogers,
--     etc.) all produce correct behavior

begin;

-- ── 1. Abbrev expansion (suffix-position designations) ─────────
create or replace function public.expand_beach_abbrevs(s text) returns text
immutable language sql as $$
  select
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(coalesce(s,''),
      '\m(SVRA)\.?\M', 'State Vehicular Recreation Area', 'g'),
      '\m(SRA)\.?\M',  'State Recreation Area', 'g'),
      '\m(SHP)\.?\M',  'State Historic Park', 'g'),
      '\m(SMR)\.?\M',  'State Marine Reserve', 'g'),
      '\m(SMCA)\.?\M', 'State Marine Conservation Area', 'g'),
      '\m(SMP)\.?\M',  'State Marine Park', 'g'),
      '\m(SB)\.?\M',   'State Beach', 'g'),
      '\m(SP)\.?\M',   'State Park', 'g'),
      '\m(SR)\.?\M',   'State Reserve', 'g'),
      '\m(NHP)\.?\M',  'National Historical Park', 'g'),
      '\m(NHS)\.?\M',  'National Historic Site', 'g'),
      '\m(NWR)\.?\M',  'National Wildlife Refuge', 'g'),
      '\m(NRA)\.?\M',  'National Recreation Area', 'g'),
      '\m(NM)\.?\M',   'National Monument', 'g'),
      '\m(NS)\.?\M',   'National Seashore', 'g'),
      '\m(NP)\.?\M',   'National Park', 'g'),
      '\m(NF)\.?\M',   'National Forest', 'g'),
      '\m(MPA)\.?\M',  'Marine Protected Area', 'g'),
      '\m(ER)\.?\M',   'Ecological Reserve', 'g');
$$;

-- ── 2. Full-name normalization (apostrophe / & / hyphen / case / ws) ──
create or replace function public.normalize_beach_name_full(s text) returns text
immutable language sql as $$
  select lower(regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(public.expand_beach_abbrevs(coalesce(s,'')),
      '''',     '',    'g'),     -- strip apostrophes (Rosie's → Rosies)
      '\s*&\s*', ' and ', 'g'),  -- & → and
      '-',      ' ',   'g'),     -- hyphens → spaces
      '\s+',    ' ',   'g'),     -- collapse whitespace
    '^\s+|\s+$', '', 'g')                -- trim
  );
$$;

-- ── 3. Canonical match scorer ──────────────────────────────────
create or replace function public.match_beach_name(a text, b text)
returns numeric immutable language sql as $$
  with norm as (
    select public.normalize_beach_name_full(a) as af,
           public.normalize_beach_name_full(b) as bf,
           public.clean_beach_name(public.expand_beach_abbrevs(a)) as ac,
           public.clean_beach_name(public.expand_beach_abbrevs(b)) as bc
  )
  select case
    when af = bf and af <> ''                                                              then 1.00
    when af like '%' || bf || '%' and position(' ' in bf) > 0                              then 0.90
    when similarity(af, bf) >= 0.85 and position(' ' in bf) > 0                            then 0.85
    when ac = bc and ac <> ''                                                              then 0.80
    when ac like '%' || bc || '%' and position(' ' in bc) > 0                              then 0.70
    when similarity(ac, bc) >= 0.65 and position(' ' in bc) > 0                            then 0.65
    else 0
  end::numeric from norm;
$$;

grant execute on function public.expand_beach_abbrevs(text)        to anon, authenticated;
grant execute on function public.normalize_beach_name_full(text)   to anon, authenticated;
grant execute on function public.match_beach_name(text, text)      to anon, authenticated;

-- ── 4. Retrofit compute_dogs_verdict_core ──────────────────────
create or replace function public.compute_dogs_verdict_core(
  p_geom geometry, p_name text, p_operator_id bigint
) returns table (verdict text, confidence numeric, meta jsonb)
language plpgsql security definer as $$
declare
  v_unit_id     integer;
  v_unit_name   text;
  v_cpad_val    text;  v_cpad_wt numeric;  v_cpad_url text;  v_cpad_kind text;
  v_cu_val      text;  v_cu_wt   numeric;  v_cu_tag   text;
  v_op_val      text;  v_op_wt   numeric;  v_op_tag   text;
  v_yes_wt      numeric := 0;
  v_no_wt       numeric := 0;
  v_sources     text[]  := array[]::text[];
  v_verdict     text;   v_conf numeric;  v_margin numeric;
  v_review      boolean := false;
  v_meta        jsonb;
begin
  if p_geom is null then
    return query select null::text, null::numeric, null::jsonb;
    return;
  end if;

  -- CPAD unit lookup: overlay-demote → match_beach_name score → smallest area
  select cu.unit_id, cu.unit_name into v_unit_id, v_unit_name
    from public.cpad_units cu
   where st_contains(cu.geom, p_geom::geometry)
   order by
     (cu.unit_name ~* '\m(marine park|marine protected|marine conservation|marine reserve|ecological reserve|wildlife area|wildlife refuge)\M')::int asc,
     public.match_beach_name(coalesce(p_name,''), coalesce(cu.unit_name,'')) desc,
     (cu.unit_name ~* '\mbeach\M')::int desc,
     st_area(cu.geom) asc
   limit 1;

  -- cpad_unit_exception via match_beach_name >= 0.65
  if v_unit_id is not null then
    select case when e->>'rule' in ('off_leash','allowed','yes','restricted') then 'yes'
                when e->>'rule' in ('prohibited','no')                         then 'no' else null end
      into v_cu_val
      from public.cpad_unit_dogs_policy p, jsonb_array_elements(coalesce(p.exceptions,'[]'::jsonb)) e
     where p.cpad_unit_id = v_unit_id and p_name is not null
       and e->>'beach_name' is not null and length(e->>'beach_name') >= 6
       and lower(e->>'beach_name') not in ('the beach','beach','city beach','city beaches')
       and public.match_beach_name(p_name, e->>'beach_name') >= 0.65
     order by public.match_beach_name(p_name, e->>'beach_name') desc
     limit 1;
    if v_cu_val is not null then v_cu_wt := 1.0; v_cu_tag := 'cpad_unit_exception';
    else
      select case when default_rule in ('yes','restricted') then 'yes'
                  when default_rule = 'no' then 'no' else null end
        into v_cu_val from public.cpad_unit_dogs_policy where cpad_unit_id = v_unit_id;
      if v_cu_val is not null then v_cu_wt := 0.7; v_cu_tag := 'cpad_unit_default'; end if;
    end if;
  end if;

  if v_cu_val is null and v_unit_id is not null then
    select case when r.response_value in ('yes','restricted','seasonal') then 'yes'
                when r.response_value = 'no' then 'no' else null end,
           r.response_confidence, r.source_url
      into v_cpad_val, v_cpad_wt, v_cpad_url
      from public.geo_entity_response_current r
     where r.entity_type = 'cpad' and r.entity_id = v_unit_id and r.response_scope = 'dogs_allowed';
    v_cpad_kind := case
      when v_cpad_url is null              then null
      when v_cpad_url like 'admin://%'     then 'cpad_manual'
      when v_cpad_url like 'llm-prior://%' then 'cpad_llm'
      else 'cpad_web'
    end;
  end if;

  -- operator_exception via match_beach_name >= 0.65
  if p_operator_id is not null and p_name is not null then
    select case when e->>'rule' in ('off_leash','allowed','yes','restricted') then 'yes'
                when e->>'rule' in ('prohibited','no')                         then 'no' else null end
      into v_op_val
      from public.operator_dogs_policy odp, jsonb_array_elements(odp.exceptions) e
     where odp.operator_id = p_operator_id and e->>'rule' is not null
       and length(e->>'beach_name') >= 6
       and lower(e->>'beach_name') not in ('the beach','beach','city beach','city beaches')
       and public.match_beach_name(p_name, e->>'beach_name') >= 0.65
     order by public.match_beach_name(p_name, e->>'beach_name') desc
     limit 1;
    if v_op_val is not null then v_op_wt := 1.0; v_op_tag := 'operator_exception';
    else
      select case when default_rule in ('yes','restricted') then 'yes'
                  when default_rule = 'no' then 'no' else null end
        into v_op_val from public.operator_dogs_policy where operator_id = p_operator_id;
      v_op_wt := 0.4; v_op_tag := 'operator_default';
    end if;
  end if;

  if v_cpad_kind = 'cpad_manual' and v_cpad_val is not null then
    v_verdict := v_cpad_val; v_conf := coalesce(v_cpad_wt,1.0); v_margin := null; v_review := false;
    v_sources := array['cpad_manual'];
    v_meta := jsonb_build_object('override','manual','sources',to_jsonb(v_sources),
                                 'cpad_unit_id', v_unit_id, 'review', false, 'computed_at', now());
  else
    if v_cu_val   is not null then if v_cu_val   = 'yes' then v_yes_wt := v_yes_wt + v_cu_wt;   else v_no_wt := v_no_wt + v_cu_wt;   end if; v_sources := array_append(v_sources, v_cu_tag); end if;
    if v_cpad_val is not null then if v_cpad_val = 'yes' then v_yes_wt := v_yes_wt + v_cpad_wt; else v_no_wt := v_no_wt + v_cpad_wt; end if; v_sources := array_append(v_sources, v_cpad_kind); end if;
    if v_op_val   is not null then if v_op_val   = 'yes' then v_yes_wt := v_yes_wt + v_op_wt;   else v_no_wt := v_no_wt + v_op_wt;   end if; v_sources := array_append(v_sources, v_op_tag); end if;

    if array_length(v_sources, 1) is null then
      v_verdict := null; v_conf := null; v_margin := null;
    else
      if v_yes_wt > v_no_wt then
        v_verdict := 'yes'; v_conf := v_yes_wt / (v_yes_wt + v_no_wt); v_margin := v_yes_wt - v_no_wt;
      elsif v_no_wt > v_yes_wt then
        v_verdict := 'no';  v_conf := v_no_wt  / (v_yes_wt + v_no_wt); v_margin := v_no_wt - v_yes_wt;
      else
        v_verdict := 'no';  v_conf := 0.5; v_margin := 0;
      end if;
      v_review := (v_yes_wt > 0 and v_no_wt > 0 and v_margin < 0.10);
    end if;
    v_meta := jsonb_build_object('yes_weight', round(v_yes_wt,4), 'no_weight', round(v_no_wt,4),
                                 'margin', case when v_margin is not null then round(v_margin,4) end,
                                 'sources', to_jsonb(v_sources), 'cpad_unit_id', v_unit_id,
                                 'review', v_review, 'computed_at', now());
  end if;

  return query select v_verdict,
                      case when v_conf is not null then round(v_conf,4) end,
                      v_meta;
end;
$$;

-- ── 5. Rebuild cpad_unit_for_beach precompute with helper ─────
truncate public.cpad_unit_for_beach;

insert into public.cpad_unit_for_beach (origin_key, beach_name, beach_county, lat, lng, unit_id, unit_area_m2)
  select bl.origin_key, bl.name,
         (select c.name from public.counties c where st_intersects(c.geom, bl.geom) limit 1),
         st_y(bl.geom)::float8, st_x(bl.geom)::float8,
         cu.unit_id, cu.area_m2
    from public.beach_locations bl
    left join lateral (
      select cu2.unit_id, st_area(cu2.geom::geography) as area_m2
        from public.cpad_units cu2
       where st_contains(cu2.geom, bl.geom)
       order by
         (cu2.unit_name ~* '\m(marine park|marine protected|marine conservation|marine reserve|ecological reserve|wildlife area|wildlife refuge)\M')::int asc,
         public.match_beach_name(coalesce(bl.name,''), coalesce(cu2.unit_name,'')) desc,
         (cu2.unit_name ~* '\mbeach\M')::int desc,
         st_area(cu2.geom) asc
       limit 1
    ) cu on true;

commit;
