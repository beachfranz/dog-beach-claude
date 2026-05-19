-- Phase (b) — dog park cascade v1 (parallel/specialized, NOT polymorphic).
--
-- Per Franz 2026-05-19: chose Option B over A/C. Polymorphic entity_policy_source
-- deferred until trails+restaurants land ("months out") because:
--   1. Dog-park policy semantics differ fundamentally from beach (no multi-source
--      stacking, no multi-region, no consensus engine needed — operator wins)
--   2. Just-shipped beach_policy_source cascade (yesterday's tier-boost helper)
--      shouldn't be touched again so soon
--   3. Refactor cost when trails arrive is bounded: rename + add entity_type
--      discriminator. Same shape on disk.
--
-- IF/WHEN we refactor to polymorphic entity_policy_source:
--   - dog_park_policy_source → entity_policy_source WHERE entity_type='dog_park'
--   - dog_park_dog_policy stays specialized (entity-specific semantics)
--   - dog_park_dog_policy stays separate from beach_dog_policy (different schemas)
--
-- This migration creates the FULL Phase (b) cascade:
--   1. dog_park_policy_source       (linkage: dog_park × policy_source × rule)
--   2. dog_park_dog_policy          (consumer flat table — one row per dog park)
--   3. refresh_dog_park_dog_policy  (priority: operator-posted > default)
--   4. tg_dog_park_policy_source    (insert/update/delete → refresh)
--   5. tg_dog_parks_gold_default    (after-insert: create default dog_park_dog_policy row)
--   6. Initial population: default row per existing dog_parks_gold entry
--
-- Universal default per [[dogpark-rules-are-operator-posted]]:
--   leash_policy='off_leash', dogs_allowed='yes', source='osm_default'
--   Hours from OSM opening_hours if present; else 'dawn-dusk' fallback.

BEGIN;

-- ─── 1. dog_park_policy_source — linkage table ──────────────────────

CREATE TABLE IF NOT EXISTS public.dog_park_policy_source (
  dog_park_fid        bigint NOT NULL REFERENCES public.dog_parks_gold(fid) ON DELETE CASCADE,
  policy_source_id    bigint NOT NULL REFERENCES public.policy_source(id)   ON DELETE CASCADE,
  rule                text   NOT NULL,
  rule_modifier       jsonb,
  operative_status    public.operative_status NOT NULL DEFAULT 'operative',
  evidence_verbatim   text,
  evidence_url        text,
  hours_open_time     text,                        -- '07:00' or NULL
  hours_close_time    text,                        -- '22:00' or NULL
  hours_text          text,                        -- 'dawn-dusk' / 'sunrise to 10pm' etc.
  additional_rules    text,                        -- vaccination, breed restrictions, weekly closures
  extracted_at        timestamptz,
  last_verified       timestamptz,
  created_at          timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (dog_park_fid, policy_source_id)
);

CREATE INDEX IF NOT EXISTS ix_dog_park_policy_source_dp   ON public.dog_park_policy_source (dog_park_fid);
CREATE INDEX IF NOT EXISTS ix_dog_park_policy_source_ps   ON public.dog_park_policy_source (policy_source_id);
CREATE INDEX IF NOT EXISTS ix_dog_park_policy_source_op   ON public.dog_park_policy_source (operative_status);

COMMENT ON TABLE public.dog_park_policy_source IS
  'Linkage: dog park × policy_source × rule. Mirrors beach_policy_source pattern '
  'minus section/region_name (dog parks are single-section, single-region). '
  'Hours columns added here (vs separate temporal table) because dog parks '
  'have simple open/close hours, not exception windows. Created 2026-05-19.';

-- ─── 2. dog_park_dog_policy — consumer flat table ────────────────────

CREATE TABLE IF NOT EXISTS public.dog_park_dog_policy (
  dog_park_fid          bigint PRIMARY KEY REFERENCES public.dog_parks_gold(fid) ON DELETE CASCADE,

  -- Core rule (mirrors beach_dog_policy columns)
  dogs_allowed          text,                      -- 'yes' typically; 'no' if e.g. dog park closed permanently
  leash_policy          text,                      -- 'off_leash' | 'off_leash_voice_control' | 'on_leash' | 'mixed'
  off_leash_flag        boolean,
  has_on_leash          boolean,
  has_off_leash         boolean,

  -- Hours
  hours_open_time       text,                      -- '07:00' or NULL
  hours_close_time      text,                      -- '22:00' or NULL
  hours_text            text,                      -- 'dawn-dusk' / human-readable

  -- Extras
  additional_rules      text,                      -- vaccination, breed, etc.
  has_fence             boolean,                   -- denormalized from dog_parks_gold for fast filtering
  has_drinking_water    boolean,                   -- ditto

  -- Provenance
  source                text NOT NULL,             -- 'operator_posted' | 'osm_default' | 'inferred'
  source_url            text,
  operator_id           bigint REFERENCES public.operator(id) ON DELETE SET NULL,
  consensus_confidence  numeric,                   -- 1.0 if operator_posted; lower for defaults
  curated_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_dog_park_dog_policy_leash    ON public.dog_park_dog_policy (leash_policy);
CREATE INDEX IF NOT EXISTS ix_dog_park_dog_policy_source   ON public.dog_park_dog_policy (source);
CREATE INDEX IF NOT EXISTS ix_dog_park_dog_policy_op       ON public.dog_park_dog_policy (operator_id) WHERE operator_id IS NOT NULL;

COMMENT ON TABLE public.dog_park_dog_policy IS
  'Consumer flat table — one row per dog_parks_gold entry. Populated by '
  'refresh_dog_park_dog_policy() which prioritizes operator-posted policy_source '
  'rows; falls back to universal default (off_leash + dawn-dusk per '
  '[[dogpark-rules-are-operator-posted]]). Analog of beach_dog_policy but '
  'simpler — no consensus engine, no multi-region. Created 2026-05-19.';

-- ─── 3. Refresh function ─────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.refresh_dog_park_dog_policy(p_fid bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_op_ps           record;
  v_dp              record;
  v_leash           text;
  v_off_flag        boolean;
  v_has_on          boolean;
  v_has_off         boolean;
  v_source          text;
  v_conf            numeric;
BEGIN
  SELECT has_fence, has_drinking_water, opening_hours_osm, inferred_operator_id
    INTO v_dp
    FROM public.dog_parks_gold WHERE fid = p_fid;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'refresh_dog_park_dog_policy: no dog_parks_gold row with fid=%', p_fid;
  END IF;

  -- Priority 1: operator-posted policy linked to this dog park
  SELECT dpps.rule, dpps.hours_open_time, dpps.hours_close_time, dpps.hours_text,
         dpps.additional_rules, dpps.evidence_url, ps.issuing_operator_id
    INTO v_op_ps
    FROM public.dog_park_policy_source dpps
    JOIN public.policy_source ps ON ps.id = dpps.policy_source_id
   WHERE dpps.dog_park_fid = p_fid
     AND dpps.operative_status = 'operative'
     AND ps.subtype = 'operator_posted_policy'
   ORDER BY ps.effective_date DESC NULLS LAST, dpps.last_verified DESC NULLS LAST
   LIMIT 1;

  IF FOUND THEN
    v_leash    := v_op_ps.rule;
    v_off_flag := v_op_ps.rule IN ('off_leash', 'off_leash_voice_control');
    v_has_on   := v_op_ps.rule = 'on_leash';
    v_has_off  := v_op_ps.rule IN ('off_leash', 'off_leash_voice_control');
    v_source   := 'operator_posted';
    v_conf     := 1.0;

    INSERT INTO public.dog_park_dog_policy (
      dog_park_fid, dogs_allowed, leash_policy, off_leash_flag,
      has_on_leash, has_off_leash,
      hours_open_time, hours_close_time, hours_text,
      additional_rules, has_fence, has_drinking_water,
      source, source_url, operator_id, consensus_confidence, curated_at
    )
    VALUES (
      p_fid, 'yes', v_leash, v_off_flag,
      v_has_on, v_has_off,
      v_op_ps.hours_open_time, v_op_ps.hours_close_time,
      COALESCE(v_op_ps.hours_text, v_dp.opening_hours_osm),
      v_op_ps.additional_rules, v_dp.has_fence, v_dp.has_drinking_water,
      v_source, v_op_ps.evidence_url, v_op_ps.issuing_operator_id, v_conf, now()
    )
    ON CONFLICT (dog_park_fid) DO UPDATE SET
      dogs_allowed         = EXCLUDED.dogs_allowed,
      leash_policy         = EXCLUDED.leash_policy,
      off_leash_flag       = EXCLUDED.off_leash_flag,
      has_on_leash         = EXCLUDED.has_on_leash,
      has_off_leash        = EXCLUDED.has_off_leash,
      hours_open_time      = EXCLUDED.hours_open_time,
      hours_close_time     = EXCLUDED.hours_close_time,
      hours_text           = EXCLUDED.hours_text,
      additional_rules     = EXCLUDED.additional_rules,
      has_fence            = EXCLUDED.has_fence,
      has_drinking_water   = EXCLUDED.has_drinking_water,
      source               = EXCLUDED.source,
      source_url           = EXCLUDED.source_url,
      operator_id          = EXCLUDED.operator_id,
      consensus_confidence = EXCLUDED.consensus_confidence,
      curated_at           = now();
    RETURN;
  END IF;

  -- Priority 2: universal default (off-leash inside fence + dawn-dusk)
  -- per [[dogpark-rules-are-operator-posted]]
  INSERT INTO public.dog_park_dog_policy (
    dog_park_fid, dogs_allowed, leash_policy, off_leash_flag,
    has_on_leash, has_off_leash,
    hours_open_time, hours_close_time, hours_text,
    has_fence, has_drinking_water,
    source, source_url, operator_id, consensus_confidence, curated_at
  )
  VALUES (
    p_fid, 'yes', 'off_leash', true,
    false, true,
    NULL, NULL, COALESCE(v_dp.opening_hours_osm, 'dawn-dusk'),
    v_dp.has_fence, v_dp.has_drinking_water,
    'osm_default', NULL, v_dp.inferred_operator_id, 0.5, now()
  )
  ON CONFLICT (dog_park_fid) DO UPDATE SET
    dogs_allowed         = EXCLUDED.dogs_allowed,
    leash_policy         = EXCLUDED.leash_policy,
    off_leash_flag       = EXCLUDED.off_leash_flag,
    has_on_leash         = EXCLUDED.has_on_leash,
    has_off_leash        = EXCLUDED.has_off_leash,
    hours_open_time      = EXCLUDED.hours_open_time,
    hours_close_time     = EXCLUDED.hours_close_time,
    hours_text           = EXCLUDED.hours_text,
    has_fence            = EXCLUDED.has_fence,
    has_drinking_water   = EXCLUDED.has_drinking_water,
    source               = EXCLUDED.source,
    source_url           = EXCLUDED.source_url,
    operator_id          = EXCLUDED.operator_id,
    consensus_confidence = EXCLUDED.consensus_confidence,
    curated_at           = now();
END $$;

COMMENT ON FUNCTION public.refresh_dog_park_dog_policy(bigint) IS
  'Recompute dog_park_dog_policy row for a single dog park. Priority: '
  'operator-posted policy (tier 1.0) > universal default off-leash + dawn-dusk (tier 0.5). '
  'Called by triggers on dog_park_policy_source + after-insert on dog_parks_gold.';

-- ─── 4. Trigger on dog_park_policy_source ────────────────────────────

CREATE OR REPLACE FUNCTION public.tg_dog_park_policy_source_refresh()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    PERFORM public.refresh_dog_park_dog_policy(OLD.dog_park_fid);
    RETURN OLD;
  ELSE
    PERFORM public.refresh_dog_park_dog_policy(NEW.dog_park_fid);
    RETURN NEW;
  END IF;
END $$;

DROP TRIGGER IF EXISTS tg_dog_park_policy_source_ins ON public.dog_park_policy_source;
DROP TRIGGER IF EXISTS tg_dog_park_policy_source_upd ON public.dog_park_policy_source;
DROP TRIGGER IF EXISTS tg_dog_park_policy_source_del ON public.dog_park_policy_source;

CREATE TRIGGER tg_dog_park_policy_source_ins
  AFTER INSERT ON public.dog_park_policy_source
  FOR EACH ROW EXECUTE FUNCTION public.tg_dog_park_policy_source_refresh();
CREATE TRIGGER tg_dog_park_policy_source_upd
  AFTER UPDATE ON public.dog_park_policy_source
  FOR EACH ROW EXECUTE FUNCTION public.tg_dog_park_policy_source_refresh();
CREATE TRIGGER tg_dog_park_policy_source_del
  AFTER DELETE ON public.dog_park_policy_source
  FOR EACH ROW EXECUTE FUNCTION public.tg_dog_park_policy_source_refresh();

-- ─── 5. After-insert trigger on dog_parks_gold ───────────────────────
-- New dog parks auto-get a default dog_park_dog_policy row.

CREATE OR REPLACE FUNCTION public.tg_dog_parks_gold_default_policy()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM public.refresh_dog_park_dog_policy(NEW.fid);
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS tg_dog_parks_gold_default_policy ON public.dog_parks_gold;
CREATE TRIGGER tg_dog_parks_gold_default_policy
  AFTER INSERT ON public.dog_parks_gold
  FOR EACH ROW EXECUTE FUNCTION public.tg_dog_parks_gold_default_policy();

-- ─── 6. Initial population — default row per existing dog_parks_gold ─

DO $$
DECLARE r record;
BEGIN
  FOR r IN SELECT fid FROM public.dog_parks_gold WHERE is_active = true LOOP
    PERFORM public.refresh_dog_park_dog_policy(r.fid);
  END LOOP;
END $$;

-- ─── 7. Verification ─────────────────────────────────────────────────

SELECT
  'dog_park_policy_source_rows'    AS metric, count(*)::text AS val FROM public.dog_park_policy_source
UNION ALL SELECT
  'dog_park_dog_policy_rows',           count(*)::text FROM public.dog_park_dog_policy
UNION ALL SELECT
  'by_leash_policy',
  string_agg(leash_policy || ':' || n::text, ', ' ORDER BY leash_policy)
  FROM (SELECT leash_policy, count(*) AS n FROM public.dog_park_dog_policy GROUP BY leash_policy) t
UNION ALL SELECT
  'by_source',
  string_agg(source || ':' || n::text, ', ' ORDER BY source)
  FROM (SELECT source, count(*) AS n FROM public.dog_park_dog_policy GROUP BY source) t
UNION ALL SELECT
  'with_operator_id',                   count(*)::text FROM public.dog_park_dog_policy WHERE operator_id IS NOT NULL;

COMMIT;
