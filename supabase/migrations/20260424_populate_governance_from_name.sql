-- populate_governance_from_name + name-confirmation boost (2026-04-24)
--
-- Adds the name-keyword signal as a governance evidence source. Patterns
-- are conservative — only fire on clear "Foo State Beach" / "Foo City
-- Beach" / "Foo County Beach" / "Foo National X" structures.
--
-- Confidence 0.65 alone (below most structural sources) so the signal
-- never wins by itself. But when the name signal AGREES with another
-- source's claim, the resolver applies a SIGNIFICANT extra boost:
--
--   normal agreement:    +0.10 per agreeing source past the first
--   name-confirmed:      +0.10 (normal) + 0.10 (name bonus) = +0.20
--
-- Result: structural source + name signal pair = base + 0.20 boost.
-- A pure structural-only pair = base + 0.10 boost. Name agreement
-- counts as 2x a normal source agreement.

-- ── 1. Add 'name' to the source CHECK enum ──────────────────────────────────
alter table public.beach_enrichment_provenance
  drop constraint if exists beach_enrichment_provenance_source_check;

alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_source_check
  check (source in (
    'manual','plz','cpad','tiger_places','ccc','llm','web_scrape',
    'research', 'csp_parks','park_operators','nps_places','tribal_lands',
    'military_bases', 'pad_us','sma_code_mappings','jurisdictions',
    'csp_places',
    'name'                  -- NEW: name-keyword signal from display_name
  ));

-- ── 2. Source precedence — name slots between web_scrape and llm ────────────
-- (Loses to all structural sources but beats LLM as a fallback.)
create or replace function public.source_precedence(p_source text)
returns int
language sql immutable
as $$
  select case p_source
    when 'manual'             then 0
    when 'cpad'               then 10
    when 'pad_us'             then 11
    when 'park_operators'     then 12
    when 'plz'                then 15
    when 'nps_places'         then 20
    when 'tribal_lands'       then 21
    when 'csp_parks'          then 22
    when 'sma_code_mappings'  then 23
    when 'ccc'                then 30
    when 'tiger_places'       then 40
    when 'military_bases'     then 50
    when 'state_config'       then 60
    when 'web_scrape'         then 70
    when 'name'               then 75   -- between web_scrape and llm
    when 'llm'                then 80
    else                            99
  end;
$$;

-- ── 3. The name signal populator ────────────────────────────────────────────
create or replace function public.populate_governance_from_name(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with classified as (
    select fid, display_name,
      case
        -- State patterns: "X State Beach", "State Park", "State Recreation Area",
        -- "State Marine X", "State Reserve", "SRA"
        when display_name ~* '\m(state\s+(beach|park|recreation\s+area|marine|reserve|historic))\M'
          then 'state'
        when display_name ~* '\m(srs|sra|smr|smca|smr)\M'   -- state abbreviations
          then 'state'
        -- County patterns
        when display_name ~* '\m(county\s+(beach|park|recreation))\M'
          then 'county'
        -- City patterns
        when display_name ~* '\m(city\s+(beach|park|recreation))\M'
          then 'city'
        -- Federal patterns: "National Park", "National Seashore", "NRA", etc.
        when display_name ~* '\m(national\s+(park|seashore|monument|recreation|preserve|memorial))\M'
          then 'federal'
        when display_name ~* '\m(nps|us\s+army|navy|coast\s+guard)\M'
          then 'federal'
        else null
      end as gov_type_from_name
    from public.locations_stage
    where (p_fid is null or fid = p_fid)
  ),
  ins as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at, notes)
    select fid, 'governance', 'name', 0.65,
      jsonb_build_object('type', gov_type_from_name, 'name', null),
      now(),
      'name-keyword signal from display_name: ' || display_name
    from classified
    where gov_type_from_name is not null
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          notes          = excluded.notes,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from ins;
  return rows_touched;
end;
$$;

comment on function public.populate_governance_from_name(int) is
  'Layer 2: emit governance type evidence from display_name keywords (City/County/State/National). Confidence 0.65 — below structural sources so it never wins alone, but the resolver applies an extra +0.10 boost when name agrees with another source (significant agreement signal). Always emits {type:X, name:null} since the keyword tells us TYPE only, not the agency name.';

-- ── 4. Modified resolver — name-confirmation boost ──────────────────────────
-- pick_canonical_evidence now applies extra +0.10 to any row whose group
-- contains source='name' AND has 2+ rows (i.e., name agrees with at least
-- one other source).

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
  update public.beach_enrichment_provenance
    set is_canonical = false
    where fid = p_fid and field_group = p_field_group and is_canonical = true;

  -- Manual override always wins
  select id into winner_id
    from public.beach_enrichment_provenance
    where fid = p_fid and field_group = p_field_group and source = 'manual'
    order by updated_at desc
    limit 1;

  if winner_id is not null then
    update public.beach_enrichment_provenance set is_canonical = true where id = winner_id;
    return winner_id;
  end if;

  -- Compute boosted confidence per row, with TYPE-LEVEL grouping for the
  -- name signal: name's claimed_values is {type:X, name:null}, so it agrees
  -- with any other row whose claimed_values->>'type' equals name's type,
  -- regardless of name string. We grant the name-bonus to the canonical
  -- group when 'name' source is among its members.
  with rows as (
    select id, source, confidence, claimed_values, updated_at,
           public.source_precedence(source) as src_prec,
           claimed_values->>'type' as type_value
    from public.beach_enrichment_provenance
    where fid = p_fid and field_group = p_field_group
      and claimed_values is not null
  ),
  -- Group A: exact claimed_values agreement (existing logic)
  exact_groups as (
    select claimed_values,
      max(confidence) as max_conf,
      count(*) as n,
      bool_or(source = 'name') as has_name
    from rows
    group by claimed_values
  ),
  -- Group B: type-level agreement — match name signal to anyone with same type
  type_groups as (
    select type_value,
      max(confidence) filter (where source <> 'name') as max_struct_conf,
      count(*) filter (where source <> 'name') as n_struct,
      bool_or(source = 'name') as has_name
    from rows
    where type_value is not null
    group by type_value
  ),
  scored as (
    select r.id, r.source, r.confidence, r.src_prec, r.updated_at,
      -- Base boost from exact claimed_values agreement
      least(0.99, eg.max_conf + greatest(0, eg.n - 1) * 0.10
        -- name-bonus on exact agreement
        + case when eg.has_name and eg.n > 1 then 0.10 else 0 end
        -- name-bonus on type-level agreement (when name source agrees by TYPE
        -- with at least one structural source, even if claimed_values aren't
        -- exact-equal)
        + case
            when r.source <> 'name'
             and tg.has_name
             and tg.n_struct >= 1
            then 0.10
            else 0
          end
      ) as boosted
    from rows r
    join exact_groups eg using (claimed_values)
    left join type_groups tg on tg.type_value = r.type_value
  )
  select id into winner_id
    from scored
    order by boosted desc nulls last, src_prec asc, updated_at desc
    limit 1;

  if winner_id is not null then
    update public.beach_enrichment_provenance set is_canonical = true where id = winner_id;
  end if;

  return winner_id;
end;
$$;

comment on function public.pick_canonical_evidence(int, text) is
  'Phase 1 resolver. Picks canonical evidence row: manual wins outright; else highest boosted confidence. Boost rules: +0.10 per agreeing source on EXACT claimed_values; PLUS +0.10 bonus when source=name appears in the agreeing group; PLUS +0.10 bonus to any structural source whose TYPE matches a name-signal claim (lets name confirm structural sources even when their claimed_values jsonb differs by name-string). Tiebreakers: source_precedence ASC, updated_at DESC.';

-- ── 5. Add to the orchestrator ──────────────────────────────────────────────
create or replace function public.populate_all(p_fid int default null)
returns jsonb
language plpgsql
as $$
declare
  result jsonb := '{}'::jsonb;
  c int;
begin
  c := public.populate_layer1_geographic(p_fid);              result := result || jsonb_build_object('layer1_geographic', c);

  c := public.populate_from_cpad(p_fid);                      result := result || jsonb_build_object('cpad', c);
  c := public.populate_from_ccc(p_fid);                       result := result || jsonb_build_object('ccc', c);
  c := public.populate_from_jurisdictions(p_fid);             result := result || jsonb_build_object('jurisdictions', c);
  c := public.populate_from_csp_parks(p_fid);                 result := result || jsonb_build_object('csp_parks', c);
  c := public.populate_from_park_operators(p_fid);            result := result || jsonb_build_object('park_operators', c);
  c := public.populate_from_nps_places(p_fid);                result := result || jsonb_build_object('nps_places', c);
  c := public.populate_from_tribal_lands(p_fid);              result := result || jsonb_build_object('tribal_lands', c);
  c := public.populate_from_military_bases(p_fid);            result := result || jsonb_build_object('military_bases', c);
  c := public.populate_from_private_land_zones(p_fid);        result := result || jsonb_build_object('private_land_zones', c);
  c := public.populate_governance_from_name(p_fid);           result := result || jsonb_build_object('name', c);
  c := public.populate_from_research(p_fid);                  result := result || jsonb_build_object('research', c);

  declare
    gov_count       int := 0;
    access_count    int := 0;
    dogs_count      int := 0;
    practical_count int := 0;
    f int;
  begin
    for f in
      select fid from public.locations_stage where p_fid is null or fid = p_fid
    loop
      if public.resolve_governance(f) is not null then gov_count       := gov_count + 1;       end if;
      if public.resolve_access(f)     is not null then access_count    := access_count + 1;    end if;
      if public.resolve_dogs(f)       is not null then dogs_count      := dogs_count + 1;      end if;
      if public.resolve_practical(f)  is not null then practical_count := practical_count + 1; end if;
    end loop;

    result := result || jsonb_build_object(
      'resolve_governance', gov_count,
      'resolve_access',     access_count,
      'resolve_dogs',       dogs_count,
      'resolve_practical',  practical_count
    );
  end;

  return result;
end;
$$;
