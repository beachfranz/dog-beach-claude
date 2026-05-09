-- 20260509_agency_dictionary_complete.sql
--
-- Final integration of the canonical agency dictionary:
--   1. Normalizer no longer strips "City of"/"County of"/"State of" —
--      preserves them in the alias_normalized index to avoid cross-state
--      and city/state collisions (Franz's concern). Resolver gets a
--      strip-variant FALLBACK step for novel free-text references.
--   2. populate_from_operators_gold's PAD-US join switches from text-match
--      against op.canonical_name + op.aliases to dictionary lookup via
--      resolve_agency_id (faster, fuzzy-tolerant, source-traceable).
--   3. Bulk backfill: PAD-US Loc_Mang/Loc_Own + OSM operator tags →
--      agency_aliases. Now fast thanks to pg_trgm GIN index.

begin;

-- ── 1a. Normalizer: drop the strip step ────────────────────────────────
-- Keep lowercase + punctuation collapse + whitespace collapse only.
-- Boilerplate ("City of", "County of", "State of") preserved so that
-- "City of Washington" and "State of Washington" stay distinct entries.

create or replace function public._normalize_agency_text(p_text text)
returns text
language sql
immutable
as $function$
  select trim(regexp_replace(
    regexp_replace(
      lower(coalesce(p_text, '')),
      '[^a-z0-9 ]+', ' ', 'g'
    ),
    '\s+', ' ', 'g'
  ));
$function$;


-- Recompute alias_normalized for all existing rows under the new rule
update public.agency_aliases
   set alias_normalized = public._normalize_agency_text(alias)
 where alias_normalized is distinct from public._normalize_agency_text(alias);


-- ── 1b. Resolver: add strip-variant fallback ───────────────────────────
-- After normalized-exact misses, try a stripped form ("City of Boston" →
-- "boston") to find aliases that don't carry the boilerplate. Only used
-- as a last resort before fuzzy. Confidence reduced (0.80) reflecting
-- ambiguity risk — caller should pass state_code when invoking from a
-- single-state context.

create or replace function public.resolve_agency(
  p_alias        text,
  p_alias_source text default null,
  p_state_code   text default null,
  p_min_confidence numeric default 0.65
)
returns table(operator_id bigint, confidence numeric, method text)
language plpgsql
stable
as $function$
declare
  v_norm text := public._normalize_agency_text(p_alias);
  v_stripped text := trim(regexp_replace(
    v_norm,
    '^(the |city of |town of |county of |state of |village of |borough of |township of )',
    '', 'i'
  ));
  v_stripped_with_county text := trim(regexp_replace(
    v_norm,
    ' (city|town|county|borough|township)$',
    '', 'i'
  ));
begin
  if p_alias is null or trim(p_alias) = '' then
    return;
  end if;

  -- Step a: exact alias match
  return query
    select aa.operator_id, 1.0::numeric, 'exact'::text
      from public.agency_aliases aa
      join public.operators op on op.id = aa.operator_id
     where aa.alias = p_alias
       and (p_state_code is null or op.state_code = p_state_code)
     order by aa.confidence desc
     limit 1;
  if found then return; end if;

  -- Step b: normalized exact (boilerplate-preserving)
  return query
    select aa.operator_id, 0.90::numeric, 'normalized_exact'::text
      from public.agency_aliases aa
      join public.operators op on op.id = aa.operator_id
     where aa.alias_normalized = v_norm
       and (p_state_code is null or op.state_code = p_state_code)
     order by aa.confidence desc
     limit 1;
  if found then return; end if;

  -- Step c: stripped-variant exact (lower confidence — ambiguity risk)
  if v_stripped <> v_norm then
    return query
      select aa.operator_id, 0.80::numeric, 'normalized_stripped'::text
        from public.agency_aliases aa
        join public.operators op on op.id = aa.operator_id
       where aa.alias_normalized = v_stripped
         and (p_state_code is null or op.state_code = p_state_code)
       order by aa.confidence desc
       limit 1;
    if found then return; end if;
  end if;
  if v_stripped_with_county <> v_norm then
    return query
      select aa.operator_id, 0.80::numeric, 'normalized_stripped'::text
        from public.agency_aliases aa
        join public.operators op on op.id = aa.operator_id
       where aa.alias_normalized = v_stripped_with_county
         and (p_state_code is null or op.state_code = p_state_code)
       order by aa.confidence desc
       limit 1;
    if found then return; end if;
  end if;

  -- Step d: trigram similarity using GIN-indexed `%` operator (much
  -- faster than similarity() >= X). Uses set_limit() for the threshold.
  perform set_limit(p_min_confidence::real);
  return query
    select aa.operator_id,
           similarity(aa.alias_normalized, v_norm)::numeric as sim,
           'fuzzy'::text
      from public.agency_aliases aa
      join public.operators op on op.id = aa.operator_id
     where aa.alias_normalized % v_norm
       and (p_state_code is null or op.state_code = p_state_code)
     order by sim desc
     limit 1;
end $function$;


-- ── 2. populate_from_operators_gold: dictionary-based PAD-US join ──────
-- Replace the textual op.canonical_name = pu.raw_attrs->>'Loc_Mang' join
-- with a per-fid lateral lookup via resolve_agency_id. Falls through if
-- no confident match; preserves county+city arms unchanged.

create or replace function public.populate_from_operators_gold(p_fid bigint default null)
returns table(emitted_dogs int, emitted_practical int)
language plpgsql
as $function$
declare
  v_dogs int := 0;
  v_pract int := 0;
begin
  -- ── PAD-US containment via resolver ────────────────────────────────
  with hits as (
    select distinct on (g.fid)
           g.fid as gold_fid, op.id as operator_id, op.canonical_name,
           odp.default_rule, odp.leash_required,
           odp.summary, odp.source_url
      from public.beaches_gold g
      join public.pad_us_units pu
        on st_contains(pu.geom, g.geom)
      cross join lateral (
        -- Try Loc_Mang first, fall back to Loc_Own. Pass state_code for
        -- single-state disambiguation. Both 'normalized_stripped' and
        -- 'fuzzy' are accepted with min_confidence=0.65.
        select coalesce(
          (select operator_id from public.resolve_agency(
              pu.raw_attrs->>'Loc_Mang', 'pad_us_loc_mang', g.state, 0.65) limit 1),
          (select operator_id from public.resolve_agency(
              pu.raw_attrs->>'Loc_Own',  'pad_us_loc_own',  g.state, 0.65) limit 1)
        ) as resolved_op_id
      ) r
      join public.operators op
        on op.id = r.resolved_op_id and op.is_active
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id and odp.policy_found = true
     where g.is_active
       and (p_fid is null or g.fid = p_fid)
     order by g.fid, st_area(pu.geom) asc
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'operator_pad_us',
           source_url,
           jsonb_strip_nulls(jsonb_build_object(
             'dogs_allowed', case default_rule when 'yes' then 'yes' when 'no' then 'no'
                                               when 'mixed' then 'mixed' when 'seasonal' then 'seasonal' end,
             'leash_policy', case when leash_required is true then 'on_leash'
                                  when leash_required is false then 'off_leash' end,
             'dogs_policy_notes', summary
           )),
           0.70, false, 'derived_url_crawl', 'beach_access', now()
      from hits where source_url is not null
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values, updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
    returning 1
  )
  select count(*) into v_dogs from ins_dogs;

  -- County via county_fips (unchanged)
  with hits as (
    select distinct g.fid as gold_fid, op.canonical_name,
           odp.default_rule, odp.leash_required, odp.summary, odp.source_url
      from public.beaches_gold g
      join public.operators op
        on op.level = 'county' and op.county_geoid = g.county_fips and op.is_active
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id and odp.policy_found = true
     where g.is_active and (p_fid is null or g.fid = p_fid)
  ),
  ins_dogs2 as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'operator_county', source_url,
           jsonb_strip_nulls(jsonb_build_object(
             'dogs_allowed', case default_rule when 'yes' then 'yes' when 'no' then 'no'
                                               when 'mixed' then 'mixed' when 'seasonal' then 'seasonal' end,
             'leash_policy', case when leash_required is true then 'on_leash'
                                  when leash_required is false then 'off_leash' end,
             'dogs_policy_notes', summary
           )),
           0.55, false, 'derived_url_crawl', 'beach_access', now()
      from hits where source_url is not null
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values, updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
    returning 1
  )
  select count(*) into v_dogs from ins_dogs2;

  -- City via jurisdictions PIP (unchanged)
  with hits as (
    select distinct g.fid as gold_fid, op.canonical_name,
           odp.default_rule, odp.leash_required, odp.summary, odp.source_url
      from public.beaches_gold g
      join public.jurisdictions j on st_contains(j.geom, g.geom) and j.place_type like 'C%'
      join public.operators op
        on op.level = 'city' and op.jurisdiction_id = j.id and op.is_active
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id and odp.policy_found = true
     where g.is_active and (p_fid is null or g.fid = p_fid)
  ),
  ins_dogs3 as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'operator_city', source_url,
           jsonb_strip_nulls(jsonb_build_object(
             'dogs_allowed', case default_rule when 'yes' then 'yes' when 'no' then 'no'
                                               when 'mixed' then 'mixed' when 'seasonal' then 'seasonal' end,
             'leash_policy', case when leash_required is true then 'on_leash'
                                  when leash_required is false then 'off_leash' end,
             'dogs_policy_notes', summary
           )),
           0.50, false, 'derived_url_crawl', 'beach_access', now()
      from hits where source_url is not null
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values, updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
    returning 1
  )
  select count(*) into v_dogs from ins_dogs3;

  return query select v_dogs, v_pract;
end $function$;


grant execute on function public._normalize_agency_text(text)                       to anon, authenticated, service_role;
grant execute on function public.resolve_agency(text, text, text, numeric)          to anon, authenticated, service_role;
grant execute on function public.populate_from_operators_gold(bigint)               to anon, authenticated, service_role;

commit;
