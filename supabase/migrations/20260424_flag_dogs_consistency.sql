-- flag_dogs_consistency — detect research vs governing_body disagreement on
-- dogs_allowed / dogs_leash_required and surface for admin review.
-- (2026-04-24)
--
-- Per resolution rule memory: research keeps winning canonical (it's per-
-- beach), governing_body stays as evidence. But disagreement is signal:
-- either the LLM read a wrong page or this beach truly has an exception.
-- Either way, a human should look.
--
-- Detection: for each beach with BOTH research AND governing_body dogs
-- evidence, compare allowed + leash_required. Disagreement on either
-- triggers review_status='needs_review' and a diagnostic note.
--
-- Conservative: only sets review_status when currently null (won't
-- overwrite 'verified' or 'flagged' set by humans).

create or replace function public.flag_dogs_consistency(p_fid int default null)
returns int
language plpgsql
as $$
declare flagged int;
begin
  with disagreements as (
    select
      r.fid,
      r.claimed_values->>'allowed'        as r_allowed,
      r.claimed_values->>'leash_required' as r_leash,
      g.claimed_values->>'allowed'        as g_allowed,
      g.claimed_values->>'leash_required' as g_leash,
      g.source_url                        as g_source_url
    from public.beach_enrichment_provenance r
    join public.beach_enrichment_provenance g
      on g.fid = r.fid
     and g.field_group = 'dogs'
     and g.source = 'governing_body'
    where r.field_group = 'dogs'
      and r.source = 'research'
      and (p_fid is null or r.fid = p_fid)
      and (
        -- Disagreement on dogs_allowed (both filled, different values)
        (   r.claimed_values->>'allowed' is not null
        and g.claimed_values->>'allowed' is not null
        and r.claimed_values->>'allowed' <> g.claimed_values->>'allowed')
        or
        -- Disagreement on dogs_leash_required (both filled, different)
        (   r.claimed_values->>'leash_required' is not null
        and g.claimed_values->>'leash_required' is not null
        and r.claimed_values->>'leash_required' <> g.claimed_values->>'leash_required')
      )
  )
  update public.locations_stage s
    set review_status = case when s.review_status is null
                              then 'needs_review' else s.review_status end,
        review_notes  = 'dogs disagreement: research says allowed=' ||
                          coalesce(d.r_allowed, 'null') ||
                          ', leash=' || coalesce(d.r_leash, 'null') ||
                          ' — agency default says allowed=' ||
                          coalesce(d.g_allowed, 'null') ||
                          ', leash=' || coalesce(d.g_leash, 'null') ||
                          coalesce(' (' || d.g_source_url || ')', '')
    from disagreements d
    where s.fid = d.fid;

  get diagnostics flagged = row_count;
  return flagged;
end;
$$;

comment on function public.flag_dogs_consistency(int) is
  'Detects disagreement between per-beach research and agency-default governing_body on dogs allowed/leash. Sets review_status=needs_review (only when currently null) + writes a diagnostic note. Run after resolve_dogs.';

-- ── Add to orchestrator ─────────────────────────────────────────────────────
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
    f int;
  begin
    for f in
      select fid from public.locations_stage where p_fid is null or fid = p_fid
    loop
      if public.resolve_governance(f) is not null then gov_count    := gov_count + 1;       end if;
      if public.resolve_access(f)     is not null then access_count := access_count + 1;    end if;
    end loop;
    result := result || jsonb_build_object(
      'resolve_governance', gov_count, 'resolve_access', access_count
    );
  end;

  c := public.populate_dogs_from_governing_body(p_fid);
  result := result || jsonb_build_object('governing_body', c);

  declare
    dogs_count      int := 0;
    practical_count int := 0;
    f int;
  begin
    for f in
      select fid from public.locations_stage where p_fid is null or fid = p_fid
    loop
      if public.resolve_dogs(f)      is not null then dogs_count      := dogs_count + 1;      end if;
      if public.resolve_practical(f) is not null then practical_count := practical_count + 1; end if;
    end loop;
    result := result || jsonb_build_object(
      'resolve_dogs', dogs_count, 'resolve_practical', practical_count
    );
  end;

  -- Final consistency check — flag dogs disagreements
  c := public.flag_dogs_consistency(p_fid);
  result := result || jsonb_build_object('dogs_consistency_flagged', c);

  return result;
end;
$$;
