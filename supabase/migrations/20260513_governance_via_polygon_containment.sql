-- 20260513_governance_via_polygon_containment.sql
--
-- True architectural unification: governance attribution rides on top
-- of the EXISTING polygon_containment matcher chain. One spatial
-- matching process (populate_polygon_containment_gold, already shipped),
-- one canonical resolver (_resolve_polygon_containment), and now ONE
-- polygon→operator dispatcher (polygon_to_operator).
--
-- populate_governance_gold becomes a slim reader: pulls canonical
-- polygon_containment BEP rows for a beach, resolves each to an operator
-- via polygon_to_operator(kind, claimed_values, state), emits governance
-- BEP rows with confidence inherited from the polygon_containment match.
--
-- Replaces the 4-source approach from earlier today (slug / FK city /
-- FK county / resolve_agency fuzzy) — all 4 paths now live in
-- polygon_to_operator, dispatched by polygon_kind. The previous helper
-- lookup_operator is dropped (it was a half-step that still required
-- callers to pick the right argument).
--
-- Confidence:
--   inherited verbatim from the canonical polygon_containment row.
--   PAD-US: 0.68-0.95 (rich, name+proximity)
--   CPAD:   0.80-0.95 (name-tier)
--   TIGER/military/tribal: fixed (e.g. 0.90)
-- _resolve_governance_gold downstream still picks canonical from
-- among the multiple emitted sources (one per polygon_kind).

begin;

-- ────────────────────────────────────────────────────────────────────────
-- 1. polygon_to_operator: the ONE dispatcher.
--    Given a polygon_containment row (kind + claimed_values + state),
--    returns the canonical active operator_id or null.
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.polygon_to_operator(
  p_polygon_kind   text,
  p_claimed_values jsonb,
  p_state          text
)
returns bigint
language plpgsql stable as $function$
declare
  v_op_id bigint;
  v_polygon_id text;
  v_polygon_name text;
  v_loc_mang text;
begin
  if p_polygon_kind is null or p_claimed_values is null or p_state is null then
    return null;
  end if;
  v_polygon_id   := p_claimed_values->>'polygon_id';
  v_polygon_name := p_claimed_values->>'polygon_name';

  case p_polygon_kind
    -- ── TIGER place (incorporated city) ─────────────────────────────────
    when 'c1_city' then
      if v_polygon_id is not null then
        select id into v_op_id
          from public.operators
         where state_code = p_state
           and level = 'city'
           and jurisdiction_id = v_polygon_id::bigint
           and is_active and parent_operator_id is null
         limit 1;
      end if;

    -- ── TIGER CDP (unincorporated census-designated place) ──────────────
    -- Treated same as city for attribution purposes — operators were
    -- seeded with jurisdiction_id = jurisdictions.id regardless of C1/U1.
    when 'cdp' then
      if v_polygon_id is not null then
        select id into v_op_id
          from public.operators
         where state_code = p_state
           and level = 'city'
           and jurisdiction_id = v_polygon_id::bigint
           and is_active and parent_operator_id is null
         limit 1;
      end if;

    -- ── TIGER county ────────────────────────────────────────────────────
    when 'county' then
      if v_polygon_id is not null then
        select id into v_op_id
          from public.operators
         where state_code = p_state
           and level = 'county'
           and county_geoid = v_polygon_id
           and is_active and parent_operator_id is null
         limit 1;
      end if;

    -- ── PAD-US unit (federal/state/etc seeded by operator_seeding) ──────
    -- Two-step lookup: unit_name slug (Pass 3 federal seed identity) OR
    -- loc_mang slug (Passes 4-8 state-and-below seed identity). Falls
    -- back to resolve_agency() fuzzy match on the unit/loc_mang names
    -- when slug match doesn't find one (handles name variants).
    when 'pad_us_unit' then
      v_loc_mang := p_claimed_values->>'loc_mang';
      -- Try unit_name slug first
      if v_polygon_name is not null and trim(v_polygon_name) <> '' then
        select id into v_op_id
          from public.operators
         where state_code = p_state
           and slug = public.slugify_agency(v_polygon_name || ' (' || p_state || ')')
           and is_active and parent_operator_id is null
         limit 1;
      end if;
      -- Try loc_mang slug
      if v_op_id is null and v_loc_mang is not null and trim(v_loc_mang) <> '' then
        select id into v_op_id
          from public.operators
         where state_code = p_state
           and slug = public.slugify_agency(v_loc_mang || ' (' || p_state || ')')
           and is_active and parent_operator_id is null
         limit 1;
      end if;
      -- Fuzzy fallback on unit_name
      if v_op_id is null and v_polygon_name is not null and trim(v_polygon_name) <> '' then
        select operator_id into v_op_id
          from public.resolve_agency(v_polygon_name, 'polygon_pad_us', p_state, 0.65)
         order by confidence desc limit 1;
      end if;

    -- ── CPAD unit (CA-only) ─────────────────────────────────────────────
    -- CPAD has its own mng_ag_lev hierarchy; defer to existing CPAD-
    -- specific resolution by using resolve_agency on the unit name.
    when 'cpad_unit' then
      if v_polygon_name is not null then
        select operator_id into v_op_id
          from public.resolve_agency(v_polygon_name, 'cpad_unit', p_state, 0.65)
         order by confidence desc limit 1;
      end if;

    -- ── Military base ───────────────────────────────────────────────────
    -- No dedicated operator seeding for military_bases today. Try fuzzy
    -- resolve on the base name. Likely returns null for most states.
    when 'military_base' then
      if v_polygon_name is not null then
        select operator_id into v_op_id
          from public.resolve_agency(v_polygon_name, 'military_base', p_state, 0.65)
         order by confidence desc limit 1;
      end if;

    -- ── Tribal land ─────────────────────────────────────────────────────
    -- Pass 7 + Pass 9 seed tribal operators from PAD-US TRIB +
    -- BIA tribal_lands. Slug-match the lar_name.
    when 'tribal_land' then
      if v_polygon_name is not null then
        select id into v_op_id
          from public.operators
         where state_code = p_state
           and level = 'tribal'
           and slug = public.slugify_agency(v_polygon_name || ' (' || p_state || ')')
           and is_active and parent_operator_id is null
         limit 1;
        if v_op_id is null then
          select operator_id into v_op_id
            from public.resolve_agency(v_polygon_name, 'tribal_land', p_state, 0.65)
           order by confidence desc limit 1;
        end if;
      end if;

    else
      v_op_id := null;
  end case;

  return v_op_id;
end $function$;

grant execute on function public.polygon_to_operator(text, jsonb, text)
  to anon, authenticated, service_role;

-- ────────────────────────────────────────────────────────────────────────
-- 2. populate_governance_gold — now a slim reader.
--
-- Reads canonical polygon_containment BEP rows for the beach, dispatches
-- each through polygon_to_operator(kind, claimed_values, state), emits
-- one governance BEP row per polygon source with confidence inherited
-- from the polygon_containment match.
--
-- Source name format: <polygon_kind>_governance (e.g. 'c1_city_governance',
-- 'county_governance', 'pad_us_unit_governance'). Distinct per kind so
-- _resolve_governance_gold downstream can pick canonical across them.
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.populate_governance_gold(p_fid bigint)
returns integer
language plpgsql
set statement_timeout to '600s'
as $function$
declare
  v_state text;
  v_total int := 0;
begin
  select state into v_state from public.beaches_gold where fid = p_fid;
  if v_state is null then return 0; end if;

  with src as (
    select
      bep.claimed_values->>'polygon_kind' as kind,
      bep.claimed_values             as claimed_values,
      bep.confidence                 as confidence
    from public.beach_enrichment_provenance bep
    where bep.gold_fid = p_fid
      and bep.field_group = 'polygon_containment'
      and bep.is_canonical = true
  ),
  with_op as (
    select kind, claimed_values, confidence,
           public.polygon_to_operator(kind, claimed_values, v_state) as operator_id
      from src
      where kind is not null
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, operator_id, updated_at)
    select p_fid, 'governance',
           kind || '_governance' as source,
           confidence,
           jsonb_build_object(
             'polygon_kind',  kind,
             'polygon_id',    claimed_values->>'polygon_id',
             'polygon_name',  claimed_values->>'polygon_name',
             'operator_name', (select canonical_name from public.operators
                                 where id = with_op.operator_id)
           ),
           operator_id,
           now()
      from with_op
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update set confidence     = excluded.confidence,
                  claimed_values = excluded.claimed_values,
                  operator_id    = excluded.operator_id,
                  updated_at     = now(),
                  is_canonical   = false
    returning 1
  )
  select count(*) into v_total from upserted;

  return v_total;
end $function$;

-- ────────────────────────────────────────────────────────────────────────
-- 3. Drop lookup_operator (half-step; superseded by polygon_to_operator).
-- ────────────────────────────────────────────────────────────────────────

drop function if exists public.lookup_operator(text, bigint, text, text, text, text, numeric);

commit;
