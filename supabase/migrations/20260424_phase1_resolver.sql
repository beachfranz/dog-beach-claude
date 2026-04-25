-- Phase 1 resolver — provenance.claimed_values + 4 resolve_* functions
-- (2026-04-24)
--
-- Two parts:
--   1. Schema fix: add claimed_values jsonb (each evidence row stores
--      the value(s) its source claims) and a unique index on
--      (fid, field_group, source) to enforce same-source freshness —
--      when CPAD re-runs, it UPDATEs its existing row, not INSERT a new
--      one. This implements rule #10 (freshness) at schema level.
--   2. Four resolve_<group>(p_fid) functions implementing Phase 1 of
--      the resolution-rule design (memory project_resolution_rules_design):
--      - manual override always wins
--      - highest confidence wins, tie-broken by source precedence
--      - multi-source agreement boost (+0.10 per agreeing source, cap 0.99)
--      - field-group atomicity (governance type+name as a unit via jsonb)
--      - canonical winner's claimed_values written back to staging columns

-- ── 1. Schema additions ──────────────────────────────────────────────────────

alter table public.beach_enrichment_provenance
  add column if not exists claimed_values jsonb;

-- Same-source freshness: at most one row per (fid, field_group, source).
-- When the source re-runs, the pipeline UPSERTs and overwrites in place.
create unique index if not exists bep_one_per_fid_group_source
  on public.beach_enrichment_provenance(fid, field_group, source);

comment on column public.beach_enrichment_provenance.claimed_values is
  'jsonb of values this source claims for the field_group. Shape per group: governance={type,name}, access={status,fee_modifier?}, dogs={allowed,leash_required,restricted_hours,seasonal_rules,zone_description}, practical={open_time,close_time,hours_text,has_parking,parking_type,parking_notes,has_restrooms,...}. Resolver reads claimed_values across all evidence rows to pick canonical and write back to us_beach_points_staging columns.';

-- ── 2. Source precedence helper (centralizes the ordering) ───────────────────

create or replace function public.source_precedence(p_source text)
returns int
language sql immutable
as $$
  select case p_source
    when 'manual'        then 0    -- always wins (handled separately, but ranked 0)
    when 'cpad'          then 10
    when 'pad_us'        then 11
    when 'park_operators' then 12  -- override layer above CPAD's 'state' default
    when 'plz'           then 15   -- private_land_zones bbox override
    when 'nps_places'    then 20
    when 'tribal_lands'  then 21
    when 'csp_parks'     then 22
    when 'sma_code_mappings' then 23
    when 'ccc'           then 30
    when 'tiger_places'  then 40
    when 'military_bases' then 50
    when 'state_config'  then 60   -- per-state coastal_default fallback
    when 'web_scrape'    then 70
    when 'llm'           then 80
    else                      99
  end;
$$;

comment on function public.source_precedence(text) is
  'Lower number = higher priority. Used as tiebreaker when multiple sources have equal confidence. Phase 1 ordering: cpad > pad_us > park_operators_override > plz > nps/tribal/csp/sma > ccc > tiger > military > state_config > web_scrape > llm. Manual is special-cased in resolvers (always wins regardless of this ordering).';

-- ── 3. Multi-source agreement booster ────────────────────────────────────────
-- Returns the boosted confidence given the base confidence and the count of
-- agreeing sources. Each agreeing source past the first adds 0.10, capped
-- at 0.99 to keep room for manual=1.00.

create or replace function public.boost_for_agreement(
  p_base_confidence numeric,
  p_agreeing_count  int
)
returns numeric
language sql immutable
as $$
  select least(0.99, coalesce(p_base_confidence, 0) + greatest(0, p_agreeing_count - 1) * 0.10);
$$;

-- ── 4. Generic resolver ──────────────────────────────────────────────────────
-- Picks the canonical evidence row for (p_fid, p_field_group) per Phase 1 rules.
-- Returns the chosen row id, or null if no evidence exists.
-- Side effects: updates is_canonical on all evidence rows for this group.
-- Does NOT write back to staging columns — that's group-specific (see resolve_*).

create or replace function public.pick_canonical_evidence(
  p_fid int,
  p_field_group text
)
returns bigint
language plpgsql
as $$
declare
  winner_id bigint;
begin
  -- Reset any prior canonical for this group
  update public.beach_enrichment_provenance
    set is_canonical = false
    where fid = p_fid and field_group = p_field_group and is_canonical = true;

  -- 1. Manual override always wins (most recent if multiple)
  select id into winner_id
    from public.beach_enrichment_provenance
    where fid = p_fid and field_group = p_field_group and source = 'manual'
    order by updated_at desc
    limit 1;

  if winner_id is not null then
    update public.beach_enrichment_provenance set is_canonical = true where id = winner_id;
    return winner_id;
  end if;

  -- 2. Compute boosted confidence per row (multi-source agreement)
  --    Group rows by claimed_values; rows with the same value get a
  --    combined confidence = boost_for_agreement(max_base, count).
  --    Pick the row with the highest boosted confidence; tiebreak by
  --    source_precedence ascending, then most-recent updated_at.
  with rows as (
    select id, source, confidence, claimed_values, updated_at,
           public.source_precedence(source) as src_prec
    from public.beach_enrichment_provenance
    where fid = p_fid and field_group = p_field_group
      and claimed_values is not null
  ),
  agreement as (
    -- For each row, count how many OTHER rows have the same claimed_values
    select
      r.id,
      r.source,
      r.confidence,
      r.src_prec,
      r.updated_at,
      public.boost_for_agreement(
        max(r.confidence) over (partition by r.claimed_values),
        count(*)          over (partition by r.claimed_values)::int
      ) as boosted
    from rows r
  )
  select id into winner_id
    from agreement
    order by boosted desc nulls last, src_prec asc, updated_at desc
    limit 1;

  if winner_id is not null then
    update public.beach_enrichment_provenance set is_canonical = true where id = winner_id;
  end if;

  return winner_id;
end;
$$;

comment on function public.pick_canonical_evidence(int, text) is
  'Phase 1 generic resolver. Picks the canonical evidence row for (fid, field_group): manual wins, else highest boosted confidence (multi-source agreement), tiebroken by source_precedence then updated_at. Updates is_canonical flags. Returns winner id or null. Per-group resolvers wrap this and write canonical values to staging columns.';

-- ── 5. Per-group resolvers (write canonical values back to staging) ──────────

create or replace function public.resolve_governance(p_fid int)
returns bigint
language plpgsql
as $$
declare
  winner_id bigint;
  v jsonb;
begin
  winner_id := public.pick_canonical_evidence(p_fid, 'governance');
  if winner_id is null then
    return null;
  end if;

  select claimed_values into v
    from public.beach_enrichment_provenance where id = winner_id;

  update public.us_beach_points_staging
    set governing_body_type = v->>'type',
        governing_body_name = v->>'name'
    where fid = p_fid;

  return winner_id;
end;
$$;

create or replace function public.resolve_access(p_fid int)
returns bigint
language plpgsql
as $$
declare
  winner_id bigint;
  v jsonb;
begin
  winner_id := public.pick_canonical_evidence(p_fid, 'access');
  if winner_id is null then
    return null;
  end if;

  select claimed_values into v
    from public.beach_enrichment_provenance where id = winner_id;

  update public.us_beach_points_staging
    set access_status = v->>'status'
    where fid = p_fid;

  return winner_id;
end;
$$;

create or replace function public.resolve_dogs(p_fid int)
returns bigint
language plpgsql
as $$
declare
  winner_id bigint;
  v jsonb;
begin
  winner_id := public.pick_canonical_evidence(p_fid, 'dogs');
  if winner_id is null then
    return null;
  end if;

  select claimed_values into v
    from public.beach_enrichment_provenance where id = winner_id;

  update public.us_beach_points_staging
    set dogs_allowed           = v->>'allowed',
        dogs_leash_required    = v->>'leash_required',
        dogs_restricted_hours  = v->'restricted_hours',
        dogs_seasonal_rules    = v->'seasonal_rules',
        dogs_zone_description  = v->>'zone_description'
    where fid = p_fid;

  return winner_id;
end;
$$;

create or replace function public.resolve_practical(p_fid int)
returns bigint
language plpgsql
as $$
declare
  winner_id bigint;
  v jsonb;
begin
  winner_id := public.pick_canonical_evidence(p_fid, 'practical');
  if winner_id is null then
    return null;
  end if;

  select claimed_values into v
    from public.beach_enrichment_provenance where id = winner_id;

  -- 'practical' covers hours, parking, amenities. The jsonb may carry any
  -- subset of these — only update keys present in claimed_values, leaving
  -- staging columns unchanged where the source didn't claim them.
  update public.us_beach_points_staging
    set
      open_time            = coalesce((v->>'open_time')::time,            open_time),
      close_time           = coalesce((v->>'close_time')::time,           close_time),
      hours_text           = coalesce(v->>'hours_text',                   hours_text),
      has_parking          = coalesce((v->>'has_parking')::boolean,       has_parking),
      parking_type         = coalesce(v->>'parking_type',                 parking_type),
      parking_notes        = coalesce(v->>'parking_notes',                parking_notes),
      has_restrooms        = coalesce((v->>'has_restrooms')::boolean,     has_restrooms),
      has_showers          = coalesce((v->>'has_showers')::boolean,       has_showers),
      has_drinking_water   = coalesce((v->>'has_drinking_water')::boolean,has_drinking_water),
      has_lifeguards       = coalesce((v->>'has_lifeguards')::boolean,    has_lifeguards),
      has_disabled_access  = coalesce((v->>'has_disabled_access')::boolean,has_disabled_access),
      has_food             = coalesce((v->>'has_food')::boolean,          has_food),
      has_fire_pits        = coalesce((v->>'has_fire_pits')::boolean,     has_fire_pits),
      has_picnic_area      = coalesce((v->>'has_picnic_area')::boolean,   has_picnic_area)
    where fid = p_fid;

  return winner_id;
end;
$$;

comment on function public.resolve_governance(int) is 'Phase 1 resolver for governance. Picks canonical, writes governing_body_type/name to staging.';
comment on function public.resolve_access(int)     is 'Phase 1 resolver for access_status. Picks canonical, writes access_status to staging.';
comment on function public.resolve_dogs(int)       is 'Phase 1 resolver for dog policy. Picks canonical, writes dogs_* fields (5 columns) to staging.';
comment on function public.resolve_practical(int)  is 'Phase 1 resolver for hours/parking/amenities. Picks canonical, writes the subset of practical columns the canonical source claimed (others left unchanged).';
