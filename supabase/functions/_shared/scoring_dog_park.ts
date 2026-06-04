// scoring_dog_park.ts
// Dog-park scoring engine — sibling to scoring.ts (beach).
//
// Differences from beach scoring:
//   • NO tide metric
//   • NO crowd metric (BestTime soft-removed; see [[besttime-soft-removed]])
//   • NO sand metric (rare surface for dog parks; replaced by surface_lookup)
//   • NEW: surface-aware rescoring via config.surface_lookup
//       - rain × surface  → rainStatus + rainScore multiplier (grass=OK, dirt=mud, asphalt=slip)
//       - UV × surface    → asphaltTemp boost (paw burn risk varies by surface material)
//       - heat × surface  → tier escalation for hard surfaces
//
// Engine is intentionally separate from scoring.ts (B3 asymmetric refactor per
// [[dog-park-subpipeline-scope]]). Reuses NOTHING from scoring.ts to keep
// beach engine untouched; small utilities duplicated by design.

type HourStatus = "go" | "advisory" | "caution" | "no_go";
type Surface =
  | "grass" | "dirt" | "concrete" | "asphalt"
  | "decomposed_granite" | "sand" | "wood_chips" | null;

export interface DogParkScoringConfig {
  scoring_version: string;

  // Wind
  nogo_wind_speed: number;
  caution_wind_speed: number;
  advisory_wind_speed: number;
  norm_wind_max: number;
  norm_wind_falloff: number;
  norm_wind_ideal_cold: number;
  norm_wind_ideal_comfortable: number;
  norm_wind_ideal_hot: number;
  norm_wind_temp_cold_max: number;
  norm_wind_temp_hot_min: number;

  // Rain / WMO
  nogo_precip_chance: number;
  caution_precip_chance: number;
  advisory_precip_chance: number;
  nogo_wmo_codes: number[];
  caution_wmo_codes: number[];

  // Temp
  go_temp_cold_min: number;
  advisory_temp_cold_min: number;
  caution_temp_cold_min: number;
  advisory_temp_hot_max: number;
  caution_temp_hot_max: number;
  nogo_temp_hot_max: number;
  norm_temp_target: number;
  norm_temp_range: number;

  // UV + asphalt
  advisory_uv_index: number;
  caution_uv_index: number;
  nogo_uv_index: number;
  norm_uv_max: number;
  advisory_asphalt_temp: number;
  caution_asphalt_temp: number;
  nogo_asphalt_temp: number;

  // Weights (sum to 1.0; no tide, no crowd, no sand)
  weight_rain: number;
  weight_wind: number;
  weight_temp: number;
  weight_uv: number;
  weight_weather_code: number;
  weight_surface: number;

  // Surface × metric lookup
  surface_lookup: {
    rain?: Record<string, { tier?: HourStatus | "minor"; score_mul?: number }>;
    uv?: Record<string, { paw_temp_boost_f?: number; tier_override?: string }>;
    heat?: Record<string, { tier_escalation?: number }>;
  };

  // Status multipliers
  status_mult_advisory: number;
  status_mult_caution: number;
  status_mult_nogo: number;

  // Window selection
  window_score_threshold: number;
  window_peak_oversample_n: number;
  window_consistency_weight: number;
  window_length_penalty: number;
  window_soft_cap_hours: number;
}

export interface RawHourData {
  forecastTs: string;
  localDate: string;
  localHour: number;
  hourLabel: string;
  isDaylight: boolean;
  weatherCode: number | null;
  tempAir: number | null;
  feelsLike: number | null;
  windSpeed: number | null;
  precipChance: number | null;
  uvIndex: number | null;
  cloudCover: number | null;
  isParkOpen: boolean;
}

export interface ScoredHour {
  forecastTs: string;
  localDate: string;
  localHour: number;
  hourLabel: string;
  isDaylight: boolean;
  weatherCode: number | null;
  tempAir: number | null;
  feelsLike: number | null;
  windSpeed: number | null;
  precipChance: number | null;
  uvIndex: number | null;
  asphaltTemp: number | null;       // surface-boosted
  hourStatus: HourStatus;
  hourScore: number | null;
  isCandidateWindow: boolean;
  isInBestWindow: boolean;
  metricStatuses: Record<string, HourStatus | null>;
  componentScores: Record<string, number>;
  positiveReasonCodes: string[];
  riskReasonCodes: string[];
  hourText: string;
}

export interface BestWindow {
  hours: ScoredHour[];
  startTs: string;
  endTs: string;
  label: string;
  windowScore: number;
  status: HourStatus;
}

// ─── WMO ─────────────────────────────────────────────────────────────────────

const SEVERE_WMO = new Set([64,65,66,67,71,72,73,74,75,76,77,82,85,86,95,96,97,98,99]);
const WMO_SCORES: Record<number, number> = {
  0:1.0, 1:1.0, 2:0.9, 3:0.75,
  45:0.5, 48:0.35, 51:0.4, 53:0.35, 55:0.3, 56:0.15, 57:0.05,
  61:0.3, 63:0.2, 80:0.1, 81:0.05,
};

// ─── Status helpers ──────────────────────────────────────────────────────────

const STATUS_RANK: Record<HourStatus, number> = { go: 0, advisory: 1, caution: 2, no_go: 3 };

function worstOf(...statuses: (HourStatus | null)[]): HourStatus {
  let worst: HourStatus = "go";
  for (const s of statuses) {
    if (s !== null && STATUS_RANK[s] > STATUS_RANK[worst]) worst = s;
  }
  return worst;
}

function escalate(s: HourStatus, levels: number): HourStatus {
  const order: HourStatus[] = ["go", "advisory", "caution", "no_go"];
  const idx = Math.min(order.length - 1, Math.max(0, order.indexOf(s) + levels));
  return order[idx];
}

// ─── Per-metric ──────────────────────────────────────────────────────────────

function windStatus(s: number | null, c: DogParkScoringConfig): HourStatus | null {
  if (s === null) return null;
  if (s >= c.nogo_wind_speed) return "no_go";
  if (s >= c.caution_wind_speed) return "caution";
  if (s >= c.advisory_wind_speed) return "advisory";
  return "go";
}

function rainStatusBase(p: number | null, c: DogParkScoringConfig): HourStatus | null {
  if (p === null) return null;
  if (p >= c.nogo_precip_chance) return "no_go";
  if (p >= c.caution_precip_chance) return "caution";
  if (p >= c.advisory_precip_chance) return "advisory";
  return "go";
}

function tempColdStatus(feelsLike: number, c: DogParkScoringConfig): HourStatus {
  if (feelsLike >= c.go_temp_cold_min) return "go";
  if (feelsLike >= c.advisory_temp_cold_min) return "advisory";
  if (feelsLike >= c.caution_temp_cold_min) return "caution";
  return "no_go";
}

function tempHotStatus(feelsLike: number, c: DogParkScoringConfig): HourStatus {
  if (feelsLike <= c.advisory_temp_hot_max) return "go";
  if (feelsLike > c.nogo_temp_hot_max) return "no_go";
  if (feelsLike > c.caution_temp_hot_max) return "caution";
  return "advisory";
}

function uvStatus(uv: number | null, c: DogParkScoringConfig): HourStatus | null {
  if (uv === null) return null;
  if (uv >= c.nogo_uv_index) return "no_go";
  if (uv >= c.caution_uv_index) return "caution";
  if (uv >= c.advisory_uv_index) return "advisory";
  return "go";
}

function asphaltStatus(t: number | null, c: DogParkScoringConfig): HourStatus | null {
  if (t === null) return null;
  if (t >= c.nogo_asphalt_temp) return "no_go";
  if (t > c.caution_asphalt_temp) return "caution";
  if (t >= c.advisory_asphalt_temp) return "advisory";
  return "go";
}

function weatherCodeStatus(code: number | null, c: DogParkScoringConfig): HourStatus | null {
  if (code === null) return null;
  if (SEVERE_WMO.has(code)) return "no_go";
  if ((c.caution_wmo_codes ?? []).includes(code)) return "caution";
  return "go";
}

// ─── Surface lookup helpers ──────────────────────────────────────────────────

function surfaceKey(s: Surface): string {
  return s ?? "_default";
}

// Returns adjusted rain status (escalated by surface) AND the score multiplier.
function rainSurfaceAdjust(
  baseStatus: HourStatus | null,
  surface: Surface,
  c: DogParkScoringConfig,
): { status: HourStatus | null; scoreMul: number } {
  if (baseStatus === null || baseStatus === "go") return { status: baseStatus, scoreMul: 1 };
  const lookup = c.surface_lookup?.rain?.[surfaceKey(surface)]
    ?? c.surface_lookup?.rain?._default;
  if (!lookup) return { status: baseStatus, scoreMul: 1 };

  // "minor" tier means the surface tolerates rain → demote any base to "advisory"
  let adjusted: HourStatus = baseStatus;
  if (lookup.tier === "minor") {
    adjusted = baseStatus === "no_go" ? "advisory" : (baseStatus === "caution" ? "advisory" : "go");
  } else if (lookup.tier) {
    // Use the worse of base or surface-driven tier
    adjusted = worstOf(baseStatus, lookup.tier as HourStatus);
  }
  return { status: adjusted, scoreMul: lookup.score_mul ?? 1 };
}

function asphaltTempWithSurfaceBoost(
  baseAsphalt: number | null,
  surface: Surface,
  c: DogParkScoringConfig,
): number | null {
  if (baseAsphalt === null) return null;
  const lookup = c.surface_lookup?.uv?.[surfaceKey(surface)]
    ?? c.surface_lookup?.uv?._default;
  const boost = lookup?.paw_temp_boost_f ?? 0;
  return baseAsphalt + boost;
}

function heatTierEscalation(
  surface: Surface,
  c: DogParkScoringConfig,
): number {
  const lookup = c.surface_lookup?.heat?.[surfaceKey(surface)]
    ?? c.surface_lookup?.heat?._default;
  return lookup?.tier_escalation ?? 0;
}

// ─── Asphalt temp estimation (same physics as scoring.ts) ────────────────────

function estimateAsphaltTemp(
  airTemp: number | null,
  uvIndex: number | null,
  windSpeed: number | null,
  cloudCover: number | null,
  isDaylight: boolean,
): number | null {
  if (!isDaylight || airTemp === null) return null;
  const irradiance = Math.max(0, uvIndex ?? 0) * 100;
  const cloudFactor = cloudCover != null
    ? 1 - (Math.min(Math.max(cloudCover, 0), 100) / 100) * 0.7
    : 1;
  const effectiveSolar = irradiance * cloudFactor;
  const asphaltHeating = effectiveSolar * 0.05;
  const windCool = Math.min(Math.sqrt(Math.max(windSpeed ?? 0, 0)) * 1.2, 8);
  return round1(airTemp + asphaltHeating - windCool);
}

// ─── Window utility (shared shape with beach engine; standalone copy) ────────

function findBestWindow(hours: ScoredHour[], c: DogParkScoringConfig): BestWindow | null {
  const eligible = hours.filter((h) => h.isCandidateWindow);
  if (eligible.length === 0) return null;

  const runs: ScoredHour[][] = [];
  let current: ScoredHour[] = [];
  for (const h of eligible) {
    if (current.length === 0 || h.localHour === current[current.length - 1].localHour + 1) {
      current.push(h);
    } else { runs.push(current); current = [h]; }
  }
  if (current.length > 0) runs.push(current);

  const scoreRun = (run: ScoredHour[]): number => {
    const scores = run.map((h) => h.hourScore ?? 0);
    const peak = Math.max(...scores);
    const n = c.window_peak_oversample_n ?? 1;
    const wAvg = (scores.reduce((a, b) => a + b, 0) + peak * n) / (scores.length + n);
    const mean = scores.reduce((a, b) => a + b, 0) / scores.length;
    const sd = Math.sqrt(scores.reduce((a, v) => a + (v - mean) ** 2, 0) / scores.length);
    const cv = mean > 0 ? sd / mean : 0;
    const consistency = 1 + (c.window_consistency_weight ?? 0.10) * (1 - cv);
    const soft = c.window_soft_cap_hours ?? 4;
    const penalty = c.window_length_penalty ?? 0.05;
    const lenFactor = 1 - penalty * Math.max(0, run.length - soft);
    return wAvg * consistency * Math.max(0, lenFactor);
  };

  const best = runs.reduce((b, r) => scoreRun(r) > scoreRun(b) ? r : b);
  const peak = best.reduce((b, h) => (h.hourScore ?? 0) > (b.hourScore ?? 0) ? h : b);
  const peakIdx = best.indexOf(peak);
  const peakScore = peak.hourScore ?? 0;
  let threshold = c.window_score_threshold ?? 0.93;
  let window: ScoredHour[] = [];
  const STEP = 0.05;

  while (true) {
    const minScore = peakScore * threshold;
    window = [peak];
    for (let i = peakIdx + 1; i < best.length; i++) {
      const h = best[i], prev = window[window.length - 1];
      if (h.localHour !== prev.localHour + 1) break;
      if ((h.hourScore ?? 0) < minScore) break;
      window.push(h);
    }
    for (let i = peakIdx - 1; i >= 0; i--) {
      const h = best[i], next = window[0];
      if (next.localHour !== h.localHour + 1) break;
      if ((h.hourScore ?? 0) < minScore) break;
      window.unshift(h);
    }
    if (window.length >= 2) break;
    if (threshold <= 0) break;
    threshold = Math.max(0, threshold - STEP);
  }
  if (window.length < 2) return eligible.length === 1 ? singletonWindow(eligible[0]) : null;

  const status = window.reduce<HourStatus>(
    (w, h) => STATUS_RANK[h.hourStatus] > STATUS_RANK[w] ? h.hourStatus : w, "go");

  return {
    hours: window,
    startTs: window[0].forecastTs,
    endTs: window[window.length - 1].forecastTs,
    label: `${formatHour(window[0].localHour)}–${formatHour(window[window.length - 1].localHour + 1)}`,
    windowScore: round2(scoreRun(window)),
    status,
  };
}

function singletonWindow(h: ScoredHour): BestWindow {
  return {
    hours: [h],
    startTs: h.forecastTs,
    endTs: h.forecastTs,
    label: `${formatHour(h.localHour)}–${formatHour(h.localHour + 1)}`,
    windowScore: h.hourScore ?? 0,
    status: h.hourStatus,
  };
}

// ─── Public API ──────────────────────────────────────────────────────────────

export function scoreDogParkHours(
  hours: RawHourData[],
  surface: Surface,
  config: DogParkScoringConfig,
): ScoredHour[] {
  return hours.map((h) => scoreOneHour(h, surface, config));
}

export function selectBestWindowsDogPark(
  scored: ScoredHour[],
  config: DogParkScoringConfig,
): Map<string, BestWindow | null> {
  const byDate = new Map<string, ScoredHour[]>();
  for (const h of scored) {
    const arr = byDate.get(h.localDate) ?? [];
    arr.push(h);
    byDate.set(h.localDate, arr);
  }
  const out = new Map<string, BestWindow | null>();
  for (const [date, hs] of byDate) {
    out.set(date, findBestWindow([...hs].sort((a, b) => a.localHour - b.localHour), config));
  }
  return out;
}

export function applyBestWindowFlagsDogPark(
  scored: ScoredHour[],
  windows: Map<string, BestWindow | null>,
): void {
  const tsSet = new Set<string>();
  for (const w of windows.values()) {
    if (w) for (const h of w.hours) tsSet.add(h.forecastTs);
  }
  for (const h of scored) h.isInBestWindow = tsSet.has(h.forecastTs);
}

// ─── Core scoring ────────────────────────────────────────────────────────────

function scoreOneHour(
  raw: RawHourData,
  surface: Surface,
  c: DogParkScoringConfig,
): ScoredHour {
  const feelsLike = raw.feelsLike ?? raw.tempAir;
  const asphaltBase = estimateAsphaltTemp(raw.tempAir, raw.uvIndex, raw.windSpeed, raw.cloudCover, raw.isDaylight);
  const asphaltTemp = asphaltTempWithSurfaceBoost(asphaltBase, surface, c);

  // Statuses
  const ms = {
    wind: windStatus(raw.windSpeed, c),
    temp_cold: feelsLike !== null ? tempColdStatus(feelsLike, c) : null,
    temp_hot: feelsLike !== null ? tempHotStatus(feelsLike, c) : null,
    uv: uvStatus(raw.uvIndex, c),
    asphalt: asphaltStatus(asphaltTemp, c),
    weather_code: weatherCodeStatus(raw.weatherCode, c),
  };

  // Rain × surface
  const rainBase = rainStatusBase(raw.precipChance, c);
  const rainAdj = rainSurfaceAdjust(rainBase, surface, c);

  // Heat × surface escalation (on hot side of temp)
  const heatEsc = heatTierEscalation(surface, c);
  const tempHotEsc = ms.temp_hot && heatEsc > 0 && ms.temp_hot !== "go"
    ? escalate(ms.temp_hot, heatEsc) : ms.temp_hot;

  const tempCombined = worstOf(ms.temp_cold, tempHotEsc);

  // surface_status — synthesized from rain & heat surface effects
  let surfaceStatus: HourStatus | null = null;
  if (rainAdj.status && rainAdj.status !== rainBase) surfaceStatus = rainAdj.status;
  else if (heatEsc > 0 && tempHotEsc !== ms.temp_hot) surfaceStatus = tempHotEsc;

  const metricStatuses: Record<string, HourStatus | null> = {
    wind_status: ms.wind,
    rain_status: rainAdj.status,
    temp_status: tempCombined,
    temp_cold_status: ms.temp_cold,
    temp_hot_status: tempHotEsc,
    uv_status: ms.uv,
    asphalt_status: ms.asphalt,
    surface_status: surfaceStatus,
  };

  // Status multipliers
  const sm = (s: HourStatus | null): number => {
    if (s === "no_go") return c.status_mult_nogo ?? 0.5;
    if (s === "caution") return c.status_mult_caution ?? 0.75;
    if (s === "advisory") return c.status_mult_advisory ?? 0.9;
    return 1.0;
  };

  // Component scores (0-1 each)
  const rainScore = (raw.precipChance !== null ? clamp(1 - raw.precipChance / 100) : 0.5)
    * sm(rainAdj.status) * rainAdj.scoreMul;
  const windIdeal = windIdealSpeed(feelsLike, c);
  const windBase = raw.windSpeed !== null
    ? clamp(1 - Math.abs(raw.windSpeed - windIdeal) / (c.norm_wind_falloff ?? 20))
    : 0.5;
  const windScore = windBase * sm(ms.wind);
  const tempBase = feelsLike !== null
    ? clamp(1 - Math.abs(feelsLike - c.norm_temp_target) / c.norm_temp_range)
    : 0.5;
  const tempScore = tempBase * sm(ms.temp_cold) * sm(tempHotEsc);
  const uvScore = (raw.uvIndex !== null ? clamp(1 - raw.uvIndex / c.norm_uv_max) : 0.5) * sm(ms.uv);
  const weatherScore = (raw.weatherCode !== null ? (WMO_SCORES[raw.weatherCode] ?? 0.5) : 0.5)
    * sm(ms.weather_code);
  const surfaceScore = surfaceStatus ? sm(surfaceStatus) : 1.0;

  const hourScore = Math.round((
    rainScore * c.weight_rain +
    windScore * c.weight_wind +
    tempScore * c.weight_temp +
    uvScore * c.weight_uv +
    weatherScore * c.weight_weather_code +
    surfaceScore * c.weight_surface
  ) * 100);

  const componentScores = {
    rain_score: round2(rainScore),
    wind_score: round2(windScore),
    temp_score: round2(tempScore),
    uv_score: round2(uvScore),
    weather_score: round2(weatherScore),
    surface_score: round2(surfaceScore),
    hour_score: hourScore,
  };

  // Availability gates
  if (!raw.isDaylight) {
    return mkResult(raw, asphaltTemp, "no_go", null, false, metricStatuses, componentScores,
      [], ["no_daylight"]);
  }
  if (!raw.isParkOpen) {
    return mkResult(raw, asphaltTemp, "no_go", null, false, metricStatuses, componentScores,
      [], ["park_closed"]);
  }

  // Overall status
  const overall = worstOf(
    ms.weather_code, ms.wind, ms.temp_cold, tempHotEsc, ms.uv,
    ms.asphalt, rainAdj.status, surfaceStatus,
  );
  const isCandidate = overall !== "no_go";

  // Reason codes
  const pos: string[] = [];
  const risk: string[] = [];
  if (ms.weather_code === "no_go") risk.push("severe_weather");
  else if (ms.weather_code === "caution") risk.push("bad_weather");
  if (ms.wind === "no_go") risk.push("dangerous_wind");
  else if (ms.wind === "caution") risk.push("strong_wind");
  else if (ms.wind === "advisory") risk.push("breezy");
  if (rainAdj.status === "no_go") risk.push("heavy_rain");
  else if (rainAdj.status === "caution") {
    risk.push(surface === "concrete" || surface === "asphalt" ? "slick_surface" : "rain_risk");
  } else if (rainAdj.status === "advisory") risk.push("some_rain_chance");
  if (ms.uv === "no_go") risk.push("extreme_uv");
  else if (ms.uv === "caution") risk.push("high_uv");
  else if (ms.uv === "advisory") risk.push("moderate_uv");
  if (tempCombined === "no_go") risk.push("extreme_temp");
  else if (tempCombined === "caution") risk.push("temp_out_of_range");
  else if (tempCombined === "advisory") risk.push("cool_or_warm");
  if (ms.asphalt === "no_go" || ms.asphalt === "caution") {
    risk.push(surface === "asphalt" || surface === "concrete" ? "hot_paws" : "hot_pavement");
  } else if (ms.asphalt === "advisory") risk.push("warm_pavement");

  if (rainAdj.status === "go" || rainAdj.status === null) pos.push("dry");
  if (feelsLike !== null && feelsLike >= 60 && feelsLike <= 75) pos.push("perfect_temp");
  if (ms.uv === "go" || ms.uv === "advisory") pos.push("safe_paws");
  if (surface === "grass" && rainBase !== "go" && rainAdj.status === "advisory") pos.push("grass_handles_rain");

  return mkResult(raw, asphaltTemp, overall, hourScore, isCandidate, metricStatuses, componentScores, pos, risk);
}

function mkResult(
  raw: RawHourData,
  asphaltTemp: number | null,
  status: HourStatus,
  score: number | null,
  isCandidate: boolean,
  metricStatuses: Record<string, HourStatus | null>,
  componentScores: Record<string, number>,
  positiveReasonCodes: string[],
  riskReasonCodes: string[],
): ScoredHour {
  const parts: string[] = [];
  if (raw.windSpeed !== null) parts.push(`${Math.round(raw.windSpeed)}mph wind`);
  if (raw.feelsLike !== null) parts.push(`${Math.round(raw.feelsLike)}°F feels-like`);
  const hourText = parts.join(" · ") || status;
  return {
    forecastTs: raw.forecastTs,
    localDate: raw.localDate,
    localHour: raw.localHour,
    hourLabel: raw.hourLabel,
    isDaylight: raw.isDaylight,
    weatherCode: raw.weatherCode,
    tempAir: raw.tempAir,
    feelsLike: raw.feelsLike,
    windSpeed: raw.windSpeed,
    precipChance: raw.precipChance,
    uvIndex: raw.uvIndex,
    asphaltTemp,
    hourStatus: status,
    hourScore: score,
    isCandidateWindow: isCandidate,
    isInBestWindow: false,
    metricStatuses,
    componentScores,
    positiveReasonCodes,
    riskReasonCodes,
    hourText,
  };
}

// ─── Wind ideal (temp-dependent step function — copied from scoring.ts) ──────

function windIdealSpeed(feelsLike: number | null, c: DogParkScoringConfig): number {
  if (feelsLike === null) return c.norm_wind_ideal_comfortable ?? 8;
  if (feelsLike < (c.norm_wind_temp_cold_max ?? 55)) return c.norm_wind_ideal_cold ?? 2;
  if (feelsLike > (c.norm_wind_temp_hot_min ?? 80)) return c.norm_wind_ideal_hot ?? 15;
  return c.norm_wind_ideal_comfortable ?? 8;
}

// ─── Utilities ───────────────────────────────────────────────────────────────

function clamp(v: number): number { return Math.max(0, Math.min(1, v)); }
function round1(v: number): number { return Math.round(v * 10) / 10; }
function round2(v: number): number { return Math.round(v * 100) / 100; }
function formatHour(h: number): string {
  if (h === 0 || h === 24) return "12am";
  if (h === 12) return "12pm";
  return h < 12 ? `${h}am` : `${h - 12}pm`;
}
