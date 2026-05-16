-- Consensus engine rewrite — Phase 5: entity-aware boolean-flag
-- promoter. Dual-path same as Phase 3 — beaches with
-- beach_policy_source rows get flags derived from the entity model;
-- legacy promoter still serves the remaining ~3,890 beaches.
--
-- DRAFT — NOT YET APPLIED. Review before running.
--
-- Per docs/consensus_engine_rewrite_design.md, Phase 5 is the
-- final structural change. Two new artifacts:
--
-- 1. `_canonical_dogs_from_policy_sources(p_fid)` — TABLE-valued
--    helper that walks beach_policy_source for one beach, picks
--    highest-authority operative rule per section, then aggregates
--    across sections to derive the boolean flags
--    (dogs_allowed, has_on_leash, has_off_leash, off_leash_flag,
--    leash_policy). Returns has_canonical=FALSE when the beach has
--    no entity rows.
--
-- 2. `promote_entity_dogs_to_beach_dog_policy(p_fid)` —
--    counterpart to the legacy promote_canonical_dogs_to_beach_dog_policy
--    that runs the helper above and upserts into beach_dog_policy.
--    Source = 'entity_promoted'. Skips manual_curator rows
--    (Phase 3 protection preserved).
--
-- And one modification:
--
-- 3. The legacy `promote_canonical_dogs_to_beach_dog_policy` adds a
--    NOT EXISTS gate excluding beaches with beach_policy_source
--    rows. Those beaches are now the entity promoter's responsibility.
--    Backward-compatible for all other ~3,890 beaches.
--
-- Idempotency: all functions are CREATE OR REPLACE. The entity
-- promoter is run for the six walkthrough beaches at the end of
-- this migration to populate their flags from the entity model.
-- Manual_curator beaches (HBDB) are skipped automatically.

-- ─── 1. Helper: derive flags from beach_policy_source for one beach ───

CREATE OR REPLACE FUNCTION public._canonical_dogs_from_policy_sources(p_fid bigint)
RETURNS TABLE(
  has_canonical    boolean,
  dogs_allowed     text,
  leash_policy     text,
  off_leash_flag   boolean,
  has_on_leash     boolean,
  has_off_leash    boolean,
  section_count    int,
  canonical_tier_min smallint
)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  v_count int;
BEGIN
  -- Quick existence check — no rows means no canonical to derive.
  SELECT count(*) INTO v_count
    FROM public.beach_policy_source
   WHERE beach_fid = p_fid AND operative_status = 'operative';

  IF v_count = 0 THEN
    RETURN QUERY SELECT FALSE, NULL::text, NULL::text, FALSE, FALSE, FALSE, 0, NULL::smallint;
    RETURN;
  END IF;

  RETURN QUERY
  WITH section_canonical AS (
    -- One row per section: the highest-authority operative rule.
    -- Lower tier number = higher authority. effective_date tiebreaks
    -- within tier.
    SELECT DISTINCT ON (bps.section)
      bps.section,
      bps.rule,
      policy_source_authority(ps.subtype) AS tier
    FROM public.beach_policy_source bps
    JOIN public.policy_source ps ON ps.id = bps.policy_source_id
    WHERE bps.beach_fid = p_fid
      AND bps.operative_status = 'operative'
    ORDER BY
      bps.section,
      policy_source_authority(ps.subtype) ASC,
      ps.effective_date DESC NULLS LAST,
      bps.last_verified DESC NULLS LAST
  ),
  agg AS (
    SELECT
      COUNT(*)::int                                                  AS n_sections,
      MIN(tier)                                                      AS min_tier,
      bool_or(rule IN ('off_leash', 'off_leash_voice_control'))      AS any_off_leash,
      bool_or(rule = 'on_leash')                                     AS any_on_leash,
      bool_and(rule = 'not_allowed')                                 AS all_not_allowed,
      bool_or(rule = 'not_allowed')                                  AS any_not_allowed,
      bool_or(rule IN ('off_leash','off_leash_voice_control','on_leash')) AS any_allowed
    FROM section_canonical
  )
  SELECT
    TRUE,
    -- dogs_allowed: yes / no / mixed
    CASE
      WHEN all_not_allowed THEN 'no'
      WHEN any_allowed AND any_not_allowed THEN 'mixed'
      WHEN any_allowed THEN 'yes'
      ELSE NULL
    END,
    -- leash_policy: simple rollup
    CASE
      WHEN any_off_leash AND any_on_leash THEN 'mixed'
      WHEN any_off_leash THEN 'off_leash'
      WHEN any_on_leash THEN 'on_leash'
      WHEN all_not_allowed THEN 'no_access'
      ELSE NULL
    END,
    COALESCE(any_off_leash, FALSE),
    COALESCE(any_on_leash,  FALSE),
    COALESCE(any_off_leash, FALSE),
    n_sections,
    min_tier
  FROM agg;
END $$;

COMMENT ON FUNCTION public._canonical_dogs_from_policy_sources(bigint) IS
  'Phase 5 (2026-05-16): derive canonical dog-policy boolean flags '
  'for one beach from beach_policy_source. Walks sections, picks '
  'highest-authority operative rule per section, aggregates to '
  'dogs_allowed / leash_policy / has_on_leash / has_off_leash. '
  'Returns has_canonical=FALSE when the beach has no entity rows.';

-- ─── 2. Entity promoter — counterpart to promote_canonical_dogs… ───

CREATE OR REPLACE FUNCTION public.promote_entity_dogs_to_beach_dog_policy(p_fid bigint DEFAULT NULL)
RETURNS TABLE(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
LANGUAGE plpgsql
AS $$
DECLARE
  v_inserted bigint := 0;
  v_updated  bigint := 0;
  v_skipped  bigint := 0;
BEGIN
  WITH targets AS (
    -- Beaches with at least one entity row, optionally filtered by p_fid.
    SELECT DISTINCT beach_fid
      FROM public.beach_policy_source
     WHERE (p_fid IS NULL OR beach_fid = p_fid)
  ),
  derived AS (
    SELECT
      t.beach_fid AS fid,
      c.dogs_allowed,
      c.leash_policy,
      c.off_leash_flag,
      c.has_on_leash,
      c.has_off_leash,
      c.section_count,
      c.canonical_tier_min
    FROM targets t
    CROSS JOIN LATERAL public._canonical_dogs_from_policy_sources(t.beach_fid) c
    WHERE c.has_canonical
  ),
  protected AS (
    -- Manual curator protection — same as the legacy promoter.
    SELECT arena_group_id FROM public.beach_dog_policy WHERE source = 'manual_curator'
  ),
  upsert AS (
    INSERT INTO public.beach_dog_policy
      (arena_group_id, dogs_allowed, leash_policy, off_leash_flag,
       has_on_leash, has_off_leash,
       source, curated_at, notes,
       consensus_confidence, disagreement_flag)
    SELECT
      d.fid, d.dogs_allowed, d.leash_policy, d.off_leash_flag,
      d.has_on_leash, d.has_off_leash,
      'entity_promoted', NOW(),
      format(
        'Phase 5 entity-promoted: dogs=%s, has_on=%s, has_off=%s. '
        '%s section(s); canonical tier %s.',
        d.dogs_allowed,
        coalesce(d.has_on_leash::text, '?'),
        coalesce(d.has_off_leash::text, '?'),
        d.section_count,
        d.canonical_tier_min
      ),
      -- Confidence is 1.0 for entity-promoted rows — the entity model
      -- doesn't manufacture confidence from voting; each row IS a
      -- claim with its tier-rank as its authority signal.
      1.0,
      FALSE
    FROM derived d
    WHERE d.fid NOT IN (SELECT arena_group_id FROM protected)
    ON CONFLICT (arena_group_id) DO UPDATE
      SET dogs_allowed         = EXCLUDED.dogs_allowed,
          leash_policy         = EXCLUDED.leash_policy,
          off_leash_flag       = EXCLUDED.off_leash_flag,
          has_on_leash         = EXCLUDED.has_on_leash,
          has_off_leash        = EXCLUDED.has_off_leash,
          source               = EXCLUDED.source,
          curated_at           = NOW(),
          notes                = EXCLUDED.notes,
          consensus_confidence = EXCLUDED.consensus_confidence,
          disagreement_flag    = EXCLUDED.disagreement_flag
      WHERE beach_dog_policy.source <> 'manual_curator'
        AND (
          beach_dog_policy.dogs_allowed   IS DISTINCT FROM EXCLUDED.dogs_allowed
          OR beach_dog_policy.leash_policy   IS DISTINCT FROM EXCLUDED.leash_policy
          OR beach_dog_policy.off_leash_flag IS DISTINCT FROM EXCLUDED.off_leash_flag
          OR beach_dog_policy.has_on_leash   IS DISTINCT FROM EXCLUDED.has_on_leash
          OR beach_dog_policy.has_off_leash  IS DISTINCT FROM EXCLUDED.has_off_leash
        )
    RETURNING (xmax = 0) AS inserted
  )
  SELECT
    count(*) FILTER (WHERE inserted),
    count(*) FILTER (WHERE NOT inserted),
    0
    INTO v_inserted, v_updated, v_skipped
    FROM upsert;

  RETURN QUERY SELECT v_inserted, v_updated, v_skipped;
END $$;

COMMENT ON FUNCTION public.promote_entity_dogs_to_beach_dog_policy(bigint) IS
  'Phase 5 (2026-05-16): entity-aware counterpart to '
  'promote_canonical_dogs_to_beach_dog_policy. Walks every beach '
  'with beach_policy_source rows; derives flags via '
  '_canonical_dogs_from_policy_sources; UPSERTs into '
  'beach_dog_policy with source=''entity_promoted''. Honors '
  'manual_curator protection.';

-- ─── 3. Modify legacy promoter to skip entity-covered beaches ──────
-- Adds a NOT EXISTS gate so the legacy CTE chain no longer
-- recomputes flags for beaches the entity promoter owns.
-- Otherwise the two promoters would race / overwrite each other.

CREATE OR REPLACE FUNCTION public.promote_canonical_dogs_to_beach_dog_policy(p_fid bigint DEFAULT NULL::bigint, p_min_confidence numeric DEFAULT 0.5)
 RETURNS TABLE(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
 LANGUAGE plpgsql
AS $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with consensus_dogs as (
    select gold_fid,
      max(winning_value) filter (where field_name = 'allowed')           as v_allowed,
      max(confidence)    filter (where field_name = 'allowed')           as conf_allowed,
      bool_or(disagreement) filter (where field_name = 'allowed')        as dis_allowed,
      max(winning_value) filter (where field_name = 'leash_required')    as v_leash,
      max(confidence)    filter (where field_name = 'leash_required')    as conf_leash,
      bool_or(disagreement) filter (where field_name = 'leash_required') as dis_leash,
      max(winning_value) filter (where field_name = 'off_leash_exists')  as v_off_leash,
      max(winning_value) filter (where field_name = 'has_on_leash')      as v_has_on_leash,
      max(confidence)    filter (where field_name = 'has_on_leash')      as conf_has_on_leash,
      max(winning_value) filter (where field_name = 'has_off_leash')     as v_has_off_leash,
      max(confidence)    filter (where field_name = 'has_off_leash')     as conf_has_off_leash
      from public.beach_field_consensus
     where field_group = 'dogs' and (p_fid is null or gold_fid = p_fid)
       -- Phase 5: skip beaches the entity promoter owns.
       and not exists (
         select 1 from public.beach_policy_source bps
          where bps.beach_fid = beach_field_consensus.gold_fid
       )
     group by gold_fid
  ),
  canon_payload as (
    select e.gold_fid as fid, e.claimed_values
      from public.beach_enrichment_provenance e
      join public.beaches_gold g on g.fid = e.gold_fid
     where e.field_group = 'dogs' and e.is_canonical = true
       and (p_fid is null or e.gold_fid = p_fid)
  ),
  supplemental as (
    select gold_fid as fid, jsonb_object_agg(k, v) as extra
      from (
        select distinct on (e.gold_fid, ek.key)
               e.gold_fid, ek.key as k, e.claimed_values -> ek.key as v
          from public.beach_enrichment_provenance e
          cross join lateral jsonb_object_keys(e.claimed_values) ek(key)
         where e.field_group = 'dogs' and not e.is_canonical
           and e.claimed_values -> ek.key is not null
           and (p_fid is null or e.gold_fid = p_fid)
         order by e.gold_fid, ek.key, e.confidence desc nulls last
      ) t
     group by gold_fid
  ),
  merged_cv as (
    select c.fid,
           coalesce(s.extra, '{}'::jsonb) || c.claimed_values as cv
      from canon_payload c
      left join supplemental s on s.fid = c.fid
  ),
  zr_payload as (
    select bdp.arena_group_id as fid,
           bdp.zone_rules is not null as has_zr,
           pw.prohib_start, pw.prohib_end
      from public.beach_dog_policy bdp
      left join lateral public._zone_rules_beach_wide_prohibition(bdp.zone_rules) pw on true
     where (p_fid is null or bdp.arena_group_id = p_fid)
  ),
  time_payload as (
    select cp.fid,
      case when coalesce(zr.has_zr, false) then zr.prohib_start
           else coalesce(
             case when jsonb_typeof(cp.claimed_values->'time_windows') = 'array'
                   and jsonb_array_length(cp.claimed_values->'time_windows') > 0
                   and (cp.claimed_values->'time_windows'->0->>'before') ~ '^[0-9]{1,2}:[0-9]{2}$'
                  then (cp.claimed_values->'time_windows'->0->>'before') end,
             public._norm_time_text(split_part(cp.claimed_values->>'time_prohibited_hours','-',1))
           ) end as prohib_start,
      case when coalesce(zr.has_zr, false) then zr.prohib_end
           else coalesce(
             case when jsonb_typeof(cp.claimed_values->'time_windows') = 'array'
                   and jsonb_array_length(cp.claimed_values->'time_windows') > 0
                   and (cp.claimed_values->'time_windows'->0->>'after') ~ '^[0-9]{1,2}:[0-9]{2}$'
                  then (cp.claimed_values->'time_windows'->0->>'after') end,
             public._norm_time_text(split_part(cp.claimed_values->>'time_prohibited_hours','-',2))
           ) end as prohib_end
    from canon_payload cp
    left join zr_payload zr on zr.fid = cp.fid
  ),
  derived as (
    select c.gold_fid as fid,
           public._norm_dogs_allowed(c.v_allowed)        as new_dogs_allowed,
           case when public._norm_dogs_allowed(c.v_allowed) = 'no' then null
                else public._norm_leash_policy(c.v_leash) end as new_leash_policy,
           case when public._norm_dogs_allowed(c.v_allowed) = 'no' then false
                else public._norm_bool(to_jsonb(c.v_off_leash)) end    as new_off_leash_flag,
           case when public._norm_dogs_allowed(c.v_allowed) = 'no' then false
                else public._norm_bool(to_jsonb(c.v_has_on_leash)) end as new_has_on_leash,
           case when public._norm_dogs_allowed(c.v_allowed) = 'no' then false
                else public._norm_bool(to_jsonb(c.v_has_off_leash)) end as new_has_off_leash,
           tp.prohib_start                               as new_prohib_start,
           tp.prohib_end                                 as new_prohib_end,
           case lower(m.cv->>'public_access')
             when 'yes'        then 'yes'
             when 'restricted' then 'restricted'
             when 'no'         then 'no'
             else null end                              as new_public_access,
           greatest(c.conf_allowed, c.conf_leash, c.conf_has_on_leash, c.conf_has_off_leash) as new_confidence,
           coalesce(c.dis_allowed, false) or coalesce(c.dis_leash, false) as new_disagreement
      from consensus_dogs c
      left join time_payload tp on tp.fid = c.gold_fid
      left join merged_cv m on m.fid = c.gold_fid
  ),
  protected as (
    select arena_group_id from public.beach_dog_policy
     where source IN ('manual_curator', 'entity_promoted')
  ),
  upsert as (
    insert into public.beach_dog_policy
      (arena_group_id, dogs_allowed, leash_policy, off_leash_flag,
       has_on_leash, has_off_leash,
       dogs_prohibited_start, dogs_prohibited_end, public_access,
       source, curated_at, notes,
       consensus_confidence, disagreement_flag)
    select d.fid, d.new_dogs_allowed, d.new_leash_policy, d.new_off_leash_flag,
           d.new_has_on_leash, d.new_has_off_leash,
           d.new_prohib_start, d.new_prohib_end, d.new_public_access,
           'auto_promoted_from_consensus', now(),
           format('Phase B consensus: dogs=%s, has_on=%s, has_off=%s%s',
                  d.new_dogs_allowed,
                  coalesce(d.new_has_on_leash::text, '?'),
                  coalesce(d.new_has_off_leash::text, '?'),
                  case when d.new_disagreement then ' [DISAGREEMENT]' else '' end),
           d.new_confidence, d.new_disagreement
      from derived d
     where d.fid not in (select arena_group_id from protected)
       and (d.new_dogs_allowed is not null or d.new_leash_policy is not null
            or d.new_off_leash_flag is not null or d.new_prohib_start is not null
            or d.new_has_on_leash is not null or d.new_has_off_leash is not null
            or d.new_public_access is not null)
       and coalesce(d.new_confidence, 1) >= p_min_confidence
    on conflict (arena_group_id) do update
      set dogs_allowed=excluded.dogs_allowed,
          leash_policy=excluded.leash_policy,
          off_leash_flag=excluded.off_leash_flag,
          has_on_leash=excluded.has_on_leash,
          has_off_leash=excluded.has_off_leash,
          dogs_prohibited_start=excluded.dogs_prohibited_start,
          dogs_prohibited_end=excluded.dogs_prohibited_end,
          public_access=coalesce(excluded.public_access, beach_dog_policy.public_access),
          source=excluded.source, curated_at=now(), notes=excluded.notes,
          consensus_confidence=excluded.consensus_confidence,
          disagreement_flag=excluded.disagreement_flag
      where beach_dog_policy.source NOT IN ('manual_curator', 'entity_promoted')
        and (beach_dog_policy.dogs_allowed is distinct from excluded.dogs_allowed
          or beach_dog_policy.leash_policy is distinct from excluded.leash_policy
          or beach_dog_policy.off_leash_flag is distinct from excluded.off_leash_flag
          or beach_dog_policy.has_on_leash is distinct from excluded.has_on_leash
          or beach_dog_policy.has_off_leash is distinct from excluded.has_off_leash
          or beach_dog_policy.dogs_prohibited_start is distinct from excluded.dogs_prohibited_start
          or beach_dog_policy.dogs_prohibited_end is distinct from excluded.dogs_prohibited_end
          or beach_dog_policy.public_access is distinct from excluded.public_access
          or beach_dog_policy.consensus_confidence is distinct from excluded.consensus_confidence
          or beach_dog_policy.disagreement_flag is distinct from excluded.disagreement_flag)
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upsert;
  return query select ins::bigint, upd::bigint, skp::bigint;
end;
$function$;

COMMENT ON FUNCTION public.promote_canonical_dogs_to_beach_dog_policy(bigint, numeric) IS
  'Phase 5 (2026-05-16): legacy vote-weighted promoter, NOW gated to '
  'skip beaches with beach_policy_source rows (those are owned by '
  'promote_entity_dogs_to_beach_dog_policy). Protection extended to '
  'include source=''entity_promoted'' alongside ''manual_curator''. '
  'Backward-compatible for the ~3,890 beaches without entity rows.';

-- ─── 4. Fire entity promoter for the six walkthrough beaches ───────
-- This populates flags from the entity model immediately. After this,
-- Rosie's has_off_leash etc. will be derived from beach_policy_source
-- rather than from the manual fix earlier this session.

SELECT * FROM public.promote_entity_dogs_to_beach_dog_policy();
