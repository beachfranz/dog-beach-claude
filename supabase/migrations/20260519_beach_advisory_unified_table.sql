-- water-conditions refactor — unified advisory store.
--
-- Per Franz 2026-05-19 "use beach_active_alert for ALL advisories":
-- one canonical table for every source/severity (NWS, marine threshold,
-- deterministic weather, beach policy windows, plover seasonal, etc.).
-- Consumer reads with a single query.
--
-- Drops the NWS-specific beach_active_alert (created today, no data yet)
-- and creates beach_advisory with a generic shape. NWS columns (alert_id,
-- certainty, urgency, headline, description, instruction) collapse into
-- generic + raw_data jsonb.

BEGIN;

DROP TABLE IF EXISTS public.beach_active_alert CASCADE;

CREATE TABLE public.beach_advisory (
  beach_fid          bigint      NOT NULL REFERENCES public.beaches_gold(fid) ON DELETE CASCADE,

  -- Source-namespaced ID. Examples:
  --   'nws:urn:oid:2.49.0.1.840.0.abc123'
  --   'det:hot_sand:2026-05-19'
  --   'marine:wave_height:2026-05-19T14:00'
  --   'policy_temporal:dogs_prohibited_window:2026-05-19'
  --   'plover_seasonal:2026'
  advisory_key       text        NOT NULL,

  -- Source taxonomy. Indexed (small enum-ish set in practice).
  --   nws | marine_threshold | deterministic_weather | beach_policy_temporal
  --   plover_seasonal | hab | aqi | tsunami | manual | ...
  source             text        NOT NULL,

  -- Generic event label (NWS event_type for nws; rule key for others)
  event_type         text        NOT NULL,

  -- minor | moderate | severe | extreme
  severity           text        NOT NULL,

  -- Validity window. Indexed for "active right now" queries.
  valid_from         timestamptz NOT NULL,
  valid_to           timestamptz NOT NULL,

  -- Dog-impact translation (from YAML, LLM, or hardcoded rule)
  dog_impact_class   text,
  dog_impact_text    text,
  -- Where the translation came from: yaml_lookup | llm_auto |
  -- wildcard_pending | rule (deterministic) | manual
  translation_source text,
  translation_confidence numeric,   -- only set for llm_auto

  -- Display chrome
  label              text,          -- short caption ("Hot sand", "Cold water", "Rip current")
  value              text,          -- formatted measurement ("115°F", "1.5m", "12.8°C")
  icon               text,          -- emoji or icon hint

  -- Source-specific payload (NWS headline/desc/instruction; threshold
  -- inputs; deterministic-rule trigger values; etc.)
  raw_data           jsonb,

  -- Provenance
  fetched_at         timestamptz NOT NULL DEFAULT now(),
  created_at         timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (beach_fid, advisory_key)
);

COMMENT ON TABLE public.beach_advisory IS
  'Unified advisory store. Every source (NWS, marine thresholds, '
  'deterministic weather, policy windows, plover seasonal, future HAB/AQI/'
  'tsunami) writes here with source-namespaced advisory_key. Consumer '
  'reads with one query. Spec: docs/water_conditions_advisory_spec.md. '
  'Unified per Franz 2026-05-19 ("one place to query for all sources").';

CREATE INDEX beach_advisory_validity_idx
  ON public.beach_advisory (beach_fid, valid_from, valid_to);

CREATE INDEX beach_advisory_source_idx
  ON public.beach_advisory (source);

CREATE INDEX beach_advisory_pending_idx
  ON public.beach_advisory (translation_source)
  WHERE translation_source = 'wildcard_pending';

-- Severity rank helper for display sorting
CREATE OR REPLACE FUNCTION public._advisory_severity_rank(p_sev text)
RETURNS smallint LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE p_sev
    WHEN 'extreme'  THEN 4::smallint
    WHEN 'severe'   THEN 3::smallint
    WHEN 'moderate' THEN 2::smallint
    WHEN 'minor'    THEN 1::smallint
    ELSE                 2::smallint   -- unknown → moderate
  END
$$;

-- Verification
SELECT
  'beach_advisory_columns'    AS metric, count(*)::text AS val
  FROM information_schema.columns
  WHERE table_schema='public' AND table_name='beach_advisory'
UNION ALL SELECT
  'beach_active_alert_dropped',
  CASE WHEN to_regclass('public.beach_active_alert') IS NULL THEN 'yes' ELSE 'no' END;

COMMIT;
