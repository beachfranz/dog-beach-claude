-- 20260530_v2_signal_status_helper.sql
--
-- v2_signal_status(entity_type, signal_key, raw_value) → text
--
-- Single source of truth for translating a raw measurement (UV index,
-- asphalt temp, tide height, etc.) into a user-facing severity tier
-- {clear, advisory, caution, no_go} using v2 banded thresholds.
--
-- Why: the legacy v1 *_status columns on beach_day_hourly_scores were
-- driven by hardcoded thresholds in _shared/scoring.ts and read by
-- downstream consumers (compute_weather_advisories.py, mobile-beach
-- chips, find.html sort, Scout LLM prompts, find_beaches RPC, etc.).
-- Those thresholds drifted from scoring_config_v2's bands — example:
-- v1 fired "Hot asphalt advisory" at 105°F where v2 says no penalty
-- below 115°F. False-positive advisory pills + Scout narrating noise.
--
-- Mapping:
--   * Banded penalty signals (*_neg in scoring_config_v2) — derive
--     severity from the band's score as a fraction of that signal's
--     max penalty score:
--        score = 0           → 'clear'    (no advisory written)
--        0 < frac < 0.34     → 'advisory'
--        0.34 ≤ frac < 0.67  → 'caution'
--        frac ≥ 0.67         → 'no_go'
--   * Explicit gate overrides (compute_beach_hourly_v2's hardcoded
--     safety lines) force no_go regardless of band score:
--        uv_neg        ≥ 11    → no_go  (EPA UV gate)
--        asphalt_neg   ≥ 125°F → no_go  (Weave asphalt gate)
--        sand_temp_neg ≥ 145°F → no_go
--   * Three non-banded specials get inline thresholds, mirroring
--     compute_beach_hourly_v2's inline computations:
--        feels_like_cold: ≤20 no_go, ≤32 caution, ≤50 advisory
--        feels_like_hot:  ≥125 no_go, ≥95 caution, ≥85 advisory
--        precip_chance:   ≥80 no_go, ≥50 caution, ≥30 advisory
--
-- Per Franz 2026-05-30 (task #1 of v1-retirement track). Pin:
-- [[v2-signal-status-mapping]].

begin;

create or replace function public.v2_signal_status(
  p_entity_type text,
  p_signal_key  text,
  p_raw_value   numeric
) returns text
language plpgsql
stable
as $$
declare
  v_bands     jsonb;
  v_score     int;
  v_max_score int;
  v_frac      numeric;
begin
  -- Null input → no advisory determinable
  if p_raw_value is null then return null; end if;

  -- ── Non-banded specials ──────────────────────────────────────────
  if p_signal_key = 'feels_like_cold' then
    if p_raw_value <= 20 then return 'no_go';    end if;
    if p_raw_value <= 32 then return 'caution';  end if;
    if p_raw_value <= 50 then return 'advisory'; end if;
    return 'clear';
  elsif p_signal_key = 'feels_like_hot' then
    if p_raw_value >= 125 then return 'no_go';    end if;
    if p_raw_value >= 95  then return 'caution';  end if;
    if p_raw_value >= 85  then return 'advisory'; end if;
    return 'clear';
  elsif p_signal_key = 'precip_chance' then
    if p_raw_value >= 80 then return 'no_go';    end if;
    if p_raw_value >= 50 then return 'caution';  end if;
    if p_raw_value >= 30 then return 'advisory'; end if;
    return 'clear';
  end if;

  -- ── Banded penalty signals ───────────────────────────────────────
  select bands into v_bands
    from public.scoring_config_v2
   where entity_type = p_entity_type
     and signal_key  = p_signal_key;
  -- Unknown signal_key → caller's bug, surface as null
  if v_bands is null then return null; end if;

  -- Gate overrides — safety boundaries from compute_beach_hourly_v2.
  -- Apply BEFORE band lookup so a 130°F asphalt at score 6 still
  -- reads no_go even if 6/15 happens to round to "caution" tier.
  if p_signal_key = 'uv_neg'        and p_raw_value >= 11  then return 'no_go'; end if;
  if p_signal_key = 'asphalt_neg'   and p_raw_value >= 125 then return 'no_go'; end if;
  if p_signal_key = 'sand_temp_neg' and p_raw_value >= 145 then return 'no_go'; end if;

  v_score := public._v2_lookup_band(p_raw_value, v_bands);

  -- Largest positive band-score for this signal — denominator for
  -- the fraction. If every band is 0 the signal is purely informational.
  select max((b->>'score')::int) into v_max_score
    from jsonb_array_elements(v_bands) b
   where (b->>'score')::int > 0;

  if v_score <= 0 or v_max_score is null or v_max_score = 0 then
    return 'clear';
  end if;

  v_frac := v_score::numeric / v_max_score::numeric;
  if v_frac >= 0.67 then return 'no_go';   end if;
  if v_frac >= 0.34 then return 'caution'; end if;
  return 'advisory';
end;
$$;

comment on function public.v2_signal_status(text, text, numeric) is
  'v2 single-source-of-truth severity mapper: raw value → {clear,advisory,caution,no_go}. '
  'Reads scoring_config_v2.bands for *_neg signals; inline thresholds for feels_like_cold/hot + precip_chance. '
  'Gate overrides (UV≥11, asphalt≥125, sand≥145) force no_go. See migration header for full mapping.';

commit;
