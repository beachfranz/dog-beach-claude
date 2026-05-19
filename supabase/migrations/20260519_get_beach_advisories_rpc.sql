-- water-conditions Phase 4a — consumer RPC over the unified beach_advisory.
--
-- Per Franz 2026-05-19 "one place to query for all sources." Replaces the
-- earlier multi-source-pull RPC that was scoped before unification.
--
-- Returns a JSONB array of active or upcoming advisories for one beach,
-- ordered by severity (severe → minor) then valid_from. UI sorts/groups
-- as needed.
--
-- Sources today: nws / marine_threshold / deterministic_weather /
--                deterministic_bacteria.
-- Future sources (HAB / AQI / plover / tsunami / etc.) plug in by writing
-- to beach_advisory with their own source tag — the RPC needs no changes.

CREATE OR REPLACE FUNCTION public.get_beach_advisories(
    p_fid bigint,
    p_horizon_hours int DEFAULT 24
) RETURNS jsonb
LANGUAGE sql STABLE PARALLEL SAFE
SECURITY DEFINER SET search_path TO 'public'
AS $$
  SELECT coalesce(jsonb_agg(jsonb_build_object(
    'source',           a.source,
    'severity',         a.severity,
    'event_type',       a.event_type,
    'dog_impact_class', a.dog_impact_class,
    'dog_impact_text',  a.dog_impact_text,
    'label',            a.label,
    'value',            a.value,
    'icon',             a.icon,
    'valid_from',       a.valid_from,
    'valid_to',         a.valid_to,
    'translation_source', a.translation_source,
    'raw_data',         a.raw_data
  ) ORDER BY public._advisory_severity_rank(a.severity) DESC, a.valid_from ASC),
                  '[]'::jsonb)
    FROM public.beach_advisory a
   WHERE a.beach_fid = p_fid
     AND a.valid_to   > now()
     AND a.valid_from < now() + (p_horizon_hours || ' hours')::interval
$$;

COMMENT ON FUNCTION public.get_beach_advisories(bigint, int) IS
  'Active + upcoming-within-horizon advisories for one beach, ordered by '
  'severity (severe → minor). Sources: nws / marine_threshold / '
  'deterministic_weather / deterministic_bacteria / future. Default '
  'horizon = 24 hours (catches today and starts-soon). Unified per '
  'Franz 2026-05-19; consumer surface (buildCautionsSection) is a '
  'single fetch.';

GRANT EXECUTE ON FUNCTION public.get_beach_advisories(bigint, int) TO anon, authenticated;
