-- 20260611e_bhs_hazard_extraction.sql
--
-- Parse NWS Beach Hazards Statement (BHS) bodies into structured per-
-- hazard advisory rows. A single BHS bulletin typically enumerates 2–5
-- distinct hazards (rip currents + sneaker waves + longshore currents +
-- high surf + pier overtopping + beach erosion). Today they all land
-- as one opaque event_type='Beach Hazards Statement' with NULL
-- dog_impact_class. This extraction surfaces each hazard as its own
-- beach_advisory row with severity + dog impact text.
--
-- Hazard categories (event_type):
--   rip_current_risk      (extends; merges with SRF source)
--   sneaker_wave_risk     (BHS-only)
--   high_surf_risk        (BHS-only today; marine grid swell may join later)
--   longshore_current_risk
--   pier_hazard_risk
--   beach_erosion_risk
--   cold_water_shock_risk
--   shore_break_risk
--
-- Severity rubric:
--   severe  — explicit severity adjective ("dangerous", "high risk of",
--             "life-threatening", "strong", "hazardous") or quantitative
--             trigger (surf ≥ 8ft, sets ≥ 8ft)
--   moderate — bare hazard mention without escalating adjective, or
--              "elevated" / "increased" risk language
--   not emitted at all — hazard not present in body
--
-- Output rows: source='bhs_extracted' for all hazards except rip current
-- (kept as source='rip_current' to preserve existing chip semantics).
-- Watermark sweep covers both sources.

begin;

CREATE OR REPLACE FUNCTION public.refresh_rip_current_advisories(
  p_state text   DEFAULT NULL,
  p_fid   bigint DEFAULT NULL
) RETURNS TABLE(upserted bigint, retired bigint)
LANGUAGE plpgsql AS $function$
DECLARE
  v_now      timestamptz := now();
  v_upserted bigint := 0;
  v_retired  bigint := 0;
BEGIN
  WITH
  -- ── SRF SOURCE (rip current only) ────────────────────────────────
  latest_srf AS (
    SELECT DISTINCT ON (zone_id)
      zone_id, wfo, forecast_date, period, rip_current_risk
    FROM public.wfo_srf_forecast
    WHERE period = 'current'
      AND forecast_date >= current_date - 1
    ORDER BY zone_id, forecast_date DESC, fetched_at DESC
  ),
  srf_rip AS (
    SELECT
      bg.fid AS beach_fid,
      CASE srf.rip_current_risk WHEN 'High' THEN 2 WHEN 'Moderate' THEN 1 END AS rank,
      srf.rip_current_risk AS srf_risk,
      srf.zone_id, srf.wfo, srf.forecast_date
    FROM public.beaches_gold bg
    JOIN latest_srf srf ON srf.zone_id = bg.coastal_zone_id
    WHERE bg.is_active
      AND bg.scoring_tier IN ('daily','hourly')
      AND bg.coastal_zone_id IS NOT NULL
      AND bg.coastal_zone_id <> '__no_zone__'
      AND srf.rip_current_risk IN ('Moderate','High')
      AND (p_state IS NULL OR bg.state = upper(p_state))
      AND (p_fid   IS NULL OR bg.fid   = p_fid)
  ),

  -- ── BHS BODIES (one row per beach + active BHS) ──────────────────
  -- Per-beach BHS row carries its own valid window — NWS-authoritative.
  bhs_bodies AS (
    SELECT
      ba.beach_fid,
      ba.raw_data->>'description' AS body,
      ba.valid_from AS bhs_valid_from,
      ba.valid_to   AS bhs_valid_to,
      ba.advisory_key AS source_bhs_key
    FROM public.beach_advisory ba
    JOIN public.beaches_gold bg ON bg.fid = ba.beach_fid
    WHERE ba.event_type = 'Beach Hazards Statement'
      AND ba.valid_from <= v_now AND ba.valid_to > v_now
      AND ba.raw_data->>'description' IS NOT NULL
      AND bg.is_active
      AND bg.scoring_tier IN ('daily','hourly')
      AND (p_state IS NULL OR bg.state = upper(p_state))
      AND (p_fid   IS NULL OR bg.fid   = p_fid)
  ),

  -- ── PER-HAZARD EXTRACTION (8 CTEs) ───────────────────────────────
  -- Each emits (beach_fid, event_type, rank, matched_phrase, bhs_*)
  -- where rank: 2=severe, 1=moderate. Only the highest matching rank
  -- per (beach_fid, event_type) is kept (via DISTINCT ON later).

  bhs_rip AS (
    SELECT
      beach_fid, 'rip_current_risk'::text AS event_type,
      CASE
        WHEN body ~* '\m(dangerous|life.?threatening|hazardous|extreme|strong) rip current' THEN 2
        WHEN body ~* 'high risk of rip current' THEN 2
        ELSE 1
      END AS rank,
      CASE
        WHEN body ~* '\mdangerous rip current'         THEN 'dangerous'
        WHEN body ~* '\mlife.?threatening rip current' THEN 'life-threatening'
        WHEN body ~* '\mhazardous rip current'         THEN 'hazardous'
        WHEN body ~* '\mextreme rip current'           THEN 'extreme'
        WHEN body ~* '\mstrong rip current'            THEN 'strong'
        WHEN body ~* 'high risk of rip current'        THEN 'high risk'
        ELSE 'mentioned'
      END AS matched_phrase,
      bhs_valid_from, bhs_valid_to, source_bhs_key
    FROM bhs_bodies
    WHERE body ~* '\mrip current'
  ),

  bhs_sneaker AS (
    SELECT
      beach_fid, 'sneaker_wave_risk'::text AS event_type,
      CASE
        WHEN body ~* '\m(dangerous|hazardous) sneaker wave'      THEN 2
        WHEN body ~* '(increased|elevated) risk of sneaker wave' THEN 2
        WHEN body ~* 'sneaker waves? can sweep'                  THEN 2
        ELSE 1
      END AS rank,
      CASE
        WHEN body ~* 'increased risk of sneaker wave'  THEN 'increased risk'
        WHEN body ~* 'elevated risk of sneaker wave'   THEN 'elevated risk'
        WHEN body ~* 'sneaker waves? can sweep'        THEN 'can sweep'
        WHEN body ~* '\mdangerous sneaker wave'        THEN 'dangerous'
        ELSE 'mentioned'
      END AS matched_phrase,
      bhs_valid_from, bhs_valid_to, source_bhs_key
    FROM bhs_bodies
    WHERE body ~* '\msneaker wave'
  ),

  bhs_surf AS (
    SELECT
      beach_fid, 'high_surf_risk'::text AS event_type,
      CASE
        WHEN body ~* '\mhigh wave action'                          THEN 2
        WHEN body ~* '\m(dangerous|hazardous) waves?'              THEN 2
        WHEN body ~* '\mbreaking waves?'                           THEN 2
        WHEN body ~* '\msurf of [0-9]+ to (8|9|1[0-9]) feet'       THEN 2  -- upper ≥ 8ft
        WHEN body ~* 'sets (up to|to|of)\s+(8|9|1[0-9]) feet'      THEN 2  -- big sets
        WHEN body ~* '\m(elevated|large|big) (surf|waves?)'        THEN 1
        WHEN body ~* '\msurf of [0-9]+ to [3-7] feet'              THEN 1
        ELSE 1
      END AS rank,
      CASE
        WHEN body ~* '\mhigh wave action'                          THEN 'high wave action'
        WHEN body ~* '\mdangerous wave'                            THEN 'dangerous waves'
        WHEN body ~* '\mhazardous wave'                            THEN 'hazardous waves'
        WHEN body ~* '\mbreaking waves?'                           THEN 'breaking waves'
        WHEN body ~* 'sets (up to|to|of)\s+(8|9|1[0-9]) feet'      THEN 'large sets'
        WHEN body ~* '\melevated surf'                             THEN 'elevated surf'
        WHEN body ~* '\m(large|big) waves?'                        THEN 'large waves'
        ELSE 'high surf'
      END AS matched_phrase,
      bhs_valid_from, bhs_valid_to, source_bhs_key
    FROM bhs_bodies
    WHERE body ~* '\m(high wave action|dangerous wave|hazardous wave|breaking wave|elevated surf|large waves?|big waves?|surf of [0-9]+ to [0-9]+ feet)'
  ),

  bhs_longshore AS (
    SELECT
      beach_fid, 'longshore_current_risk'::text AS event_type,
      CASE
        WHEN body ~* '\m(strong|dangerous|high) (longshore|along.?shore) current' THEN 2
        WHEN body ~* '\mstrong currents?'                                          THEN 2
        WHEN body ~* 'currents.{0,40}structures'                                   THEN 2
        ELSE 1
      END AS rank,
      CASE
        WHEN body ~* '\mstrong (longshore|along.?shore) current' THEN 'strong longshore'
        WHEN body ~* '\mstrong currents?'                         THEN 'strong currents'
        WHEN body ~* 'currents.{0,40}structures'                  THEN 'currents along structures'
        ELSE 'longshore current'
      END AS matched_phrase,
      bhs_valid_from, bhs_valid_to, source_bhs_key
    FROM bhs_bodies
    WHERE body ~* '\m(longshore current|along.?shore current|strong currents?|currents.{0,40}structures)'
  ),

  bhs_pier AS (
    SELECT
      beach_fid, 'pier_hazard_risk'::text AS event_type,
      CASE
        WHEN body ~* 'piers? (may be )?(heavily )?swamped'   THEN 2
        WHEN body ~* '(sweep|wash) people off (piers?|jetties?|rocks?)' THEN 2
        ELSE 1
      END AS rank,
      CASE
        WHEN body ~* 'piers? .{0,20}swamped'                   THEN 'piers swamped'
        WHEN body ~* 'sweep people off'                        THEN 'sweep off'
        WHEN body ~* 'wash people off'                         THEN 'wash off'
        WHEN body ~* '\m(slippery|wet) (rocks?|jetty|jetties|piers?)' THEN 'slippery rocks'
        ELSE 'pier hazard'
      END AS matched_phrase,
      bhs_valid_from, bhs_valid_to, source_bhs_key
    FROM bhs_bodies
    WHERE body ~* '(piers? .{0,20}swamped|sweep people off|wash people off|\m(slippery|wet) (rocks?|jetty|jetties|piers?))'
  ),

  bhs_erosion AS (
    SELECT
      beach_fid, 'beach_erosion_risk'::text AS event_type,
      CASE
        WHEN body ~* '\m(severe|active|significant) (beach )?erosion' THEN 2
        ELSE 1
      END AS rank,
      CASE
        WHEN body ~* '\msevere (beach )?erosion'  THEN 'severe erosion'
        WHEN body ~* '\mactive (beach )?erosion'  THEN 'active erosion'
        WHEN body ~* '\mlocalized (beach )?erosion' THEN 'localized erosion'
        ELSE 'beach erosion'
      END AS matched_phrase,
      bhs_valid_from, bhs_valid_to, source_bhs_key
    FROM bhs_bodies
    WHERE body ~* '\m(beach erosion|localized erosion|coastal erosion|active erosion)'
  ),

  bhs_cold AS (
    SELECT
      beach_fid, 'cold_water_shock_risk'::text AS event_type,
      CASE
        WHEN body ~* '\m(severe|extreme|dangerous) cold water' THEN 2
        WHEN body ~* 'cold water shock'                        THEN 2
        WHEN body ~* 'water temps? in the 4[0-9]'              THEN 2  -- <50°F
        WHEN body ~* 'water temps? in the (5[0-5])'            THEN 1  -- 50-55°F
        ELSE 1
      END AS rank,
      CASE
        WHEN body ~* 'cold water shock' THEN 'cold water shock'
        WHEN body ~* 'water temps? in the 4[0-9]' THEN 'water in 40s'
        ELSE 'cold water'
      END AS matched_phrase,
      bhs_valid_from, bhs_valid_to, source_bhs_key
    FROM bhs_bodies
    WHERE body ~* '(cold water|water temps? in the [4-5][0-9])'
  ),

  bhs_shore_break AS (
    SELECT
      beach_fid, 'shore_break_risk'::text AS event_type,
      CASE
        WHEN body ~* '\m(dangerous|hazardous|powerful) shore.?break' THEN 2
        WHEN body ~* 'plunging waves? at the (shoreline|waterline)'  THEN 2
        ELSE 1
      END AS rank,
      'shore break'::text AS matched_phrase,
      bhs_valid_from, bhs_valid_to, source_bhs_key
    FROM bhs_bodies
    WHERE body ~* '(\mshore.?break|plunging waves? at the (shoreline|waterline))'
  ),

  -- ── COMBINE all BHS hazards ─────────────────────────────────────
  bhs_all AS (
    SELECT * FROM bhs_rip
    UNION ALL SELECT * FROM bhs_sneaker
    UNION ALL SELECT * FROM bhs_surf
    UNION ALL SELECT * FROM bhs_longshore
    UNION ALL SELECT * FROM bhs_pier
    UNION ALL SELECT * FROM bhs_erosion
    UNION ALL SELECT * FROM bhs_cold
    UNION ALL SELECT * FROM bhs_shore_break
  ),

  -- ── MERGE RIP CURRENT (SRF + BHS) ───────────────────────────────
  -- Pick MAX rank across both sources. Provenance preserved separately.
  rip_merged AS (
    SELECT
      coalesce(srf.beach_fid, bhs.beach_fid) AS beach_fid,
      'rip_current_risk'::text AS event_type,
      GREATEST(coalesce(srf.rank, 0), coalesce(bhs.rank, 0)) AS rank,
      srf.srf_risk, srf.zone_id, srf.wfo, srf.forecast_date,
      bhs.matched_phrase AS bhs_phrase,
      bhs.bhs_valid_from, bhs.bhs_valid_to, bhs.source_bhs_key,
      CASE WHEN srf.beach_fid IS NOT NULL AND bhs.beach_fid IS NOT NULL THEN 'srf+bhs'
           WHEN srf.beach_fid IS NOT NULL                                THEN 'srf'
           ELSE                                                                'bhs'
      END AS rip_provenance
    FROM srf_rip srf
    FULL OUTER JOIN (SELECT * FROM bhs_all WHERE event_type = 'rip_current_risk') bhs
      ON srf.beach_fid = bhs.beach_fid
  ),

  -- ── ASSEMBLE final insert payload ───────────────────────────────
  -- Rip uses merged row; other hazards use BHS extraction directly.
  to_insert AS (
    -- Rip current
    SELECT
      r.beach_fid,
      format('rip:rip_current_risk:%s', current_date) AS advisory_key,
      CASE WHEN r.rip_provenance = 'bhs' THEN 'bhs_extracted' ELSE 'rip_current' END AS source,
      'rip_current_risk'::text AS event_type,
      CASE r.rank WHEN 2 THEN 'severe' WHEN 1 THEN 'moderate' END AS severity,
      coalesce(r.bhs_valid_from, v_now - interval '6 hours')  AS valid_from,
      coalesce(r.bhs_valid_to,   v_now + interval '12 hours') AS valid_to,
      'skip_swim'::text AS dog_impact_class,
      CASE r.rank
        WHEN 2 THEN 'NWS High rip current risk in effect — life-threatening rip currents likely. Keep her on the dry sand; no swimming or fetching in the surf.'
        WHEN 1 THEN 'NWS Moderate rip current risk today — life-threatening rip currents are possible in the surf zone. Stick to the shore.'
      END AS dog_impact_text,
      'Rip current'::text AS label,
      CASE r.rank WHEN 2 THEN 'High' WHEN 1 THEN 'Moderate' END AS value,
      '🚩'::text AS icon,
      jsonb_build_object(
        'provenance', r.rip_provenance,
        'srf', CASE WHEN r.zone_id IS NOT NULL
                    THEN jsonb_build_object('zone_id', r.zone_id, 'wfo', r.wfo,
                                            'forecast_date', r.forecast_date, 'risk', r.srf_risk)
                    ELSE NULL END,
        'bhs', CASE WHEN r.bhs_phrase IS NOT NULL
                    THEN jsonb_build_object('matched_phrase', r.bhs_phrase, 'source_advisory', r.source_bhs_key)
                    ELSE NULL END
      ) AS raw_data
    FROM rip_merged r
    WHERE r.rank > 0

    UNION ALL

    -- Other 7 BHS-only hazards
    SELECT
      h.beach_fid,
      format('bhs:%s:%s', h.event_type, current_date) AS advisory_key,
      'bhs_extracted'::text AS source,
      h.event_type,
      CASE h.rank WHEN 2 THEN 'severe' WHEN 1 THEN 'moderate' END AS severity,
      h.bhs_valid_from AS valid_from,
      h.bhs_valid_to   AS valid_to,
      CASE h.event_type
        WHEN 'pier_hazard_risk' THEN 'review_required'
        ELSE                          'skip_swim'
      END AS dog_impact_class,
      CASE h.event_type || ':' || h.rank::text
        WHEN 'sneaker_wave_risk:2' THEN 'Sneaker waves can sweep across the shoreline without warning — pulling people and dogs into the sea from rocks, jetties, and beaches. Keep her well back from the waterline today.'
        WHEN 'sneaker_wave_risk:1' THEN 'Sneaker-wave risk noted — keep her back from the immediate shoreline, especially on rocks.'
        WHEN 'high_surf_risk:2'    THEN 'High surf today — dangerous breaking waves. Skip the swim and stay back from the wave-wash zone.'
        WHEN 'high_surf_risk:1'    THEN 'Elevated surf today — moderate waves. Small dogs especially should stay out of the surf.'
        WHEN 'longshore_current_risk:2' THEN 'Strong currents along the shoreline today — dogs in the surf can be pulled sideways away from where you expect, especially near piers and jetties.'
        WHEN 'longshore_current_risk:1' THEN 'Longshore currents noted — stay close to your dog if she goes near the water.'
        WHEN 'pier_hazard_risk:2'  THEN 'Waves may overtop piers and jetties today. Keep her off any pier or rocky structure near the water.'
        WHEN 'pier_hazard_risk:1'  THEN 'Slippery rocks and pier conditions today — use caution if she walks structures near the water.'
        WHEN 'beach_erosion_risk:2' THEN 'Active beach erosion — sand or cliff edges may give way underfoot. Stay back from any cliff edge or recent storm cut.'
        WHEN 'beach_erosion_risk:1' THEN 'Localized beach erosion noted today.'
        WHEN 'cold_water_shock_risk:2' THEN 'Water cold enough to cause shock on entry. Even strong-swimmer dogs should stay out of the water today.'
        WHEN 'cold_water_shock_risk:1' THEN 'Water temperatures are cool — limit how long she stays in.'
        WHEN 'shore_break_risk:2'  THEN 'Plunging waves at the waterline today — small dogs especially at risk. Stay well back from the immediate shoreline.'
        WHEN 'shore_break_risk:1'  THEN 'Shore break noted — stay back from the wave-wash zone.'
      END AS dog_impact_text,
      CASE h.event_type
        WHEN 'sneaker_wave_risk'      THEN 'Sneaker waves'
        WHEN 'high_surf_risk'         THEN 'High surf'
        WHEN 'longshore_current_risk' THEN 'Longshore current'
        WHEN 'pier_hazard_risk'       THEN 'Pier hazard'
        WHEN 'beach_erosion_risk'     THEN 'Beach erosion'
        WHEN 'cold_water_shock_risk'  THEN 'Cold water'
        WHEN 'shore_break_risk'       THEN 'Shore break'
      END AS label,
      CASE h.rank WHEN 2 THEN 'Severe' WHEN 1 THEN 'Moderate' END AS value,
      CASE h.event_type
        WHEN 'sneaker_wave_risk'      THEN '🌊'
        WHEN 'high_surf_risk'         THEN '🌊'
        WHEN 'longshore_current_risk' THEN '🌀'
        WHEN 'pier_hazard_risk'       THEN '⚠️'
        WHEN 'beach_erosion_risk'     THEN '⛰️'
        WHEN 'cold_water_shock_risk'  THEN '❄️'
        WHEN 'shore_break_risk'       THEN '💥'
      END AS icon,
      jsonb_build_object(
        'hazard_key', h.event_type,
        'matched_phrase', h.matched_phrase,
        'source_advisory', h.source_bhs_key
      ) AS raw_data
    FROM (
      -- For non-rip hazards, dedupe to highest rank per (beach_fid, event_type)
      SELECT DISTINCT ON (beach_fid, event_type)
        beach_fid, event_type, rank, matched_phrase, bhs_valid_from, bhs_valid_to, source_bhs_key
      FROM bhs_all
      WHERE event_type <> 'rip_current_risk'
      ORDER BY beach_fid, event_type, rank DESC
    ) h
  ),

  ins AS (
    INSERT INTO public.beach_advisory (
      beach_fid, advisory_key, source, event_type, severity,
      valid_from, valid_to, dog_impact_class, dog_impact_text,
      translation_source, label, value, icon, raw_data, fetched_at
    )
    SELECT
      t.beach_fid, t.advisory_key, t.source, t.event_type, t.severity,
      t.valid_from, t.valid_to, t.dog_impact_class, t.dog_impact_text,
      'rule', t.label, t.value, t.icon, t.raw_data, v_now
    FROM to_insert t
    ON CONFLICT (beach_fid, advisory_key) DO UPDATE SET
      severity        = EXCLUDED.severity,
      valid_from      = EXCLUDED.valid_from,
      valid_to        = EXCLUDED.valid_to,
      dog_impact_text = EXCLUDED.dog_impact_text,
      value           = EXCLUDED.value,
      raw_data        = EXCLUDED.raw_data,
      fetched_at      = EXCLUDED.fetched_at
    RETURNING 1
  )
  SELECT count(*) INTO v_upserted FROM ins;

  -- Watermark sweep: drop both rip_current and bhs_extracted rows not
  -- refreshed this pass (BHS body retired, beach binding cleared, etc.).
  WITH del AS (
    DELETE FROM public.beach_advisory ba
    USING public.beaches_gold bg
    WHERE ba.beach_fid = bg.fid
      AND ba.source IN ('rip_current', 'bhs_extracted')
      AND bg.is_active
      AND bg.scoring_tier IN ('daily','hourly')
      AND (p_state IS NULL OR bg.state = upper(p_state))
      AND (p_fid   IS NULL OR bg.fid   = p_fid)
      AND ba.fetched_at < v_now - interval '5 seconds'
    RETURNING 1
  )
  SELECT count(*) INTO v_retired FROM del;

  RETURN QUERY SELECT v_upserted, v_retired;
END;
$function$;

COMMENT ON FUNCTION public.refresh_rip_current_advisories(text, bigint) IS
  'Hourly refresh of NWS Beach Hazards Statement (BHS) bodies + SRF rip current. Parses 8 hazard categories from BHS narrative (rip_current, sneaker_wave, high_surf, longshore_current, pier_hazard, beach_erosion, cold_water_shock, shore_break) into structured beach_advisory rows. Rip current merges BHS + SRF sources picking max severity. Watermark sweep covers both source values.';

commit;
notify pgrst, 'reload schema';
