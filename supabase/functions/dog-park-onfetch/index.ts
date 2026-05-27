// dog-park-onfetch/index.ts
// EXPERIMENT (Franz 2026-05-27): everything on page load.
// Fetch park+policy from DB, then in parallel call Open-Meteo +
// Sonnet narrative. Score 7-day hourly client-side using the
// _shared/scoring_dog_park.ts substrate. Return a single payload
// shaped like get-dog-park-summary PLUS scout_blurb + _meta.latencies.
//
// The hypothesis: for long-tail dog parks (low traffic, no daily
// cron value), running scoring + weather + LLM on page load is
// cheaper overall AND adds only ~500ms of perceived latency on top
// of the LLM call we already make.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import {
  scoreDogParkHours,
  selectBestWindowsDogPark,
  type RawHourData,
  type DogParkScoringConfig,
  type Surface,
} from "../_shared/scoring_dog_park.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ANTHROPIC_API_KEY    = Deno.env.get("anthropic_api_key")!;
const ANTHROPIC_API_URL    = "https://api.anthropic.com/v1/messages";
const MODEL                = "claude-sonnet-4-6";

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "GET, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: cors });

  const t0 = performance.now();
  const url = new URL(req.url);
  const fidStr = url.searchParams.get("fid");
  if (!fidStr || !/^\d+$/.test(fidStr)) return json({ error: "fid required" }, 400);
  const fid = parseInt(fidStr, 10);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  try {
    // ── DB fetch: park + scoring config ─────────────────────────────
    const tDb0 = performance.now();
    const [parkRes, cfgRes] = await Promise.all([
      supabase.from("dog_parks_gold")
        .select(`
          fid, name, display_name_override, lat, lon,
          state, county_name, address_street, address_city, address_state, address_zip,
          surface, has_fence, has_drinking_water, area_m2,
          website, description,
          dog_park_dog_policy(
            dogs_allowed, leash_policy, off_leash_flag,
            hours_open_time, hours_close_time, hours_text,
            additional_rules, double_gate, small_dog_area, large_dog_area, lighting,
            has_fence, has_drinking_water,
            surface_overlay, description_overlay,
            source, source_url
          ),
          operator:inferred_operator_id(id, name, web_url)
        `)
        .eq("fid", fid).maybeSingle(),
      supabase.from("dog_park_scoring_config")
        .select("*").eq("is_active", true).limit(1).maybeSingle(),
    ]);
    const tDbMs = Math.round(performance.now() - tDb0);
    if (parkRes.error || !parkRes.data) return json({ error: `park not found: fid=${fid}` }, 404);
    if (!cfgRes.data) return json({ error: "no active dog-park scoring config" }, 500);

    const p: any = parkRes.data;
    const config = cfgRes.data as DogParkScoringConfig;
    const dp = Array.isArray(p.dog_park_dog_policy) ? p.dog_park_dog_policy[0] : p.dog_park_dog_policy;
    const op = Array.isArray(p.operator) ? p.operator[0] : p.operator;

    if (p.lat == null || p.lon == null) return json({ error: "park has no coords; can't fetch weather" }, 500);

    const surface = ((dp?.surface_overlay ?? p.surface ?? "unknown") as Surface);
    const tz = tzForState(p.state) ?? "America/Los_Angeles";

    // ── Parallel: weather (grid → fallback Open-Meteo) + Sonnet narrative ──
    // Grid read is one indexed query, typically 50-150ms vs Open-Meteo's
    // 800-9000ms variance. Fall back to live Open-Meteo only when grid
    // is unloaded / stale for the park's cell.
    const tParallel0 = performance.now();
    let llmCached = false;
    let weatherSource: "grid" | "open_meteo" | null = null;
    const [wxResp, llmResp] = await Promise.allSettled([
      fetchWeatherGridOrLive(supabase, p.lat, p.lon, tz).then(r => {
        weatherSource = r.source;
        return r.hours;
      }),
      callScoutLlmCached(supabase, fid, p, dp, op).then(r => {
        llmCached = r.cached;
        return r.answer;
      }),
    ]);
    const tParallelMs = Math.round(performance.now() - tParallel0);

    let scoredDays: any[] = [];
    let now: any = null;
    let bestWindowsToday: any = null;
    let tScoreMs = 0;
    if (wxResp.status === "fulfilled") {
      const tScore0 = performance.now();
      const raw = wxResp.value;
      const scored = scoreDogParkHours(raw, surface, config);
      const bestWindows = selectBestWindowsDogPark(scored, config);
      // Today's row for `now`
      const todayLocal = localDate(tz);
      const todaysHours = scored.filter(h => h.localDate === todayLocal);
      const nowHour = new Date().getHours();
      now = todaysHours.find(h => h.localHour === nowHour) ?? todaysHours[0] ?? null;
      // Day rollups
      const byDate = new Map<string, any[]>();
      for (const h of scored) {
        const arr = byDate.get(h.localDate) ?? [];
        arr.push(h);
        byDate.set(h.localDate, arr);
      }
      scoredDays = Array.from(byDate.entries()).slice(0, 7).map(([date, hrs]) => {
        const win = bestWindows.get(date) ?? null;
        const statuses = hrs.map(h => h.hourStatus);
        const counts = {
          go: statuses.filter(s => s === "go").length,
          advisory: statuses.filter(s => s === "advisory").length,
          caution: statuses.filter(s => s === "caution").length,
          no_go: statuses.filter(s => s === "no_go").length,
        };
        const day_status =
          counts.no_go > counts.go + counts.advisory + counts.caution ? "no_go" :
          counts.caution > counts.go + counts.advisory ? "caution" :
          counts.advisory > counts.go ? "advisory" : "go";
        const avgs = {
          avg_temp: avgOf(hrs.map(h => h.tempAir)),
          avg_wind: avgOf(hrs.map(h => h.windSpeed)),
          avg_uv:   avgOf(hrs.map(h => h.uvIndex)),
        };
        return {
          local_date: date,
          day_status,
          best_window_label: win?.label ?? null,
          best_window_start_ts: win?.startTs ?? null,
          best_window_end_ts: win?.endTs ?? null,
          go_hours_count: counts.go,
          advisory_hours_count: counts.advisory,
          caution_hours_count: counts.caution,
          no_go_hours_count: counts.no_go,
          ...avgs,
        };
      });
      bestWindowsToday = bestWindows.get(todayLocal) ?? null;
      tScoreMs = Math.round(performance.now() - tScore0);
    }

    const scoutBlurb = llmResp.status === "fulfilled" ? llmResp.value : null;
    const llmErr = llmResp.status === "rejected" ? String(llmResp.reason) : null;
    const wxErr = wxResp.status === "rejected" ? String(wxResp.reason) : null;

    const totalMs = Math.round(performance.now() - t0);

    return json({
      dog_park: {
        fid: p.fid,
        name: p.name,
        display_name: p.display_name_override ?? p.name,
        latitude: p.lat,
        longitude: p.lon,
        state: p.state,
        county_name: p.county_name,
        address: [p.address_street, p.address_city, p.address_state, p.address_zip]
          .filter(Boolean).join(", "),
        surface: p.surface,
        has_fence: p.has_fence,
        has_drinking_water: p.has_drinking_water,
        area_m2: p.area_m2,
        website: p.website,
        description: p.description,
        operator: op ? { id: op.id, name: op.name, web_url: op.web_url } : null,
        policy: dp ?? null,
      },
      days: scoredDays,
      now: now ? {
        forecast_ts: now.forecastTs,
        local_date: now.localDate,
        local_hour: now.localHour,
        hour_status: now.hourStatus,
        hour_score: now.hourScore,
        feels_like: now.feelsLike,
        wind_speed: now.windSpeed,
        uv_index: now.uvIndex,
        precip_chance: now.precipChance,
        weather_code: now.weatherCode,
        asphalt_temp: now.asphaltTemp,
        is_now: true,
      } : null,
      scout_blurb: scoutBlurb,
      _meta: {
        total_ms: totalMs,
        db_ms: tDbMs,
        parallel_ms: tParallelMs,
        scoring_ms: tScoreMs,
        weather_ok: wxResp.status === "fulfilled",
        llm_ok:     llmResp.status === "fulfilled",
        llm_cached: llmCached,
        weather_source: weatherSource,
        weather_err: wxErr,
        llm_err: llmErr,
      },
    });
  } catch (err) {
    return json({ error: String(err), total_ms: Math.round(performance.now() - t0) }, 500);
  }
});

// ─── Weather (grid-first, fallback to Open-Meteo) ──────────────────────
//
// Primary path: weather_for_point(lat, lng, start_ts, end_ts) RPC reads
// pre-loaded weather_grid_hourly rows for the cell containing the park.
// Cached by the Dagster hourly_weather_grid_schedule job.
// Fallback: direct Open-Meteo fetch when grid is empty/stale.

async function fetchWeatherGridOrLive(
  supabase: any, lat: number, lon: number, tz: string,
): Promise<{ hours: RawHourData[]; source: "grid" | "open_meteo" }> {
  const startTs = new Date().toISOString();
  const endTs = new Date(Date.now() + 7 * 24 * 3600 * 1000).toISOString();
  const { data: gridRows, error } = await supabase.rpc("weather_for_point", {
    p_lat: lat, p_lng: lon, p_start_ts: startTs, p_end_ts: endTs,
  });
  if (!error && Array.isArray(gridRows) && gridRows.length >= 48) {
    return { hours: gridRowsToRawHours(gridRows, tz), source: "grid" };
  }
  // Fallback: live Open-Meteo
  const wx = await fetchOpenMeteo(lat, lon, tz);
  return { hours: openMeteoToRawHours(wx, tz), source: "open_meteo" };
}

function gridRowsToRawHours(rows: any[], tz: string): RawHourData[] {
  return rows.map((r) => {
    const ts = new Date(r.forecast_ts);
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", hour12: false,
    }).formatToParts(ts);
    const get = (t: string) => parts.find(p => p.type === t)?.value ?? "";
    const localDate = `${get("year")}-${get("month")}-${get("day")}`;
    const localHour = parseInt(get("hour"), 10) % 24;
    return {
      forecastTs: r.forecast_ts,
      localDate, localHour,
      hourLabel: hourLabelFmt(localHour),
      isDaylight: !!r.is_day,
      weatherCode:  r.weather_code,
      tempAir:      r.temp_air,
      feelsLike:    r.feels_like,
      windSpeed:    r.wind_speed,
      precipChance: r.precip_chance,
      uvIndex:      r.uv_index,
      cloudCover:   r.cloud_cover,
      isParkOpen:   localHour >= 5 && localHour <= 22,
    };
  });
}

// ─── Open-Meteo (fallback path) ────────────────────────────────────────

async function fetchOpenMeteo(lat: number, lon: number, tz: string): Promise<any> {
  const params = new URLSearchParams({
    latitude: String(lat),
    longitude: String(lon),
    hourly: [
      "weather_code", "temperature_2m", "apparent_temperature",
      "wind_speed_10m", "precipitation_probability", "uv_index",
      "cloud_cover", "is_day",
    ].join(","),
    temperature_unit: "fahrenheit",
    wind_speed_unit: "mph",
    timezone: tz,
    forecast_days: "7",
  });
  const url = `https://api.open-meteo.com/v1/forecast?${params}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Open-Meteo ${res.status}: ${await res.text()}`);
  return await res.json();
}

function openMeteoToRawHours(wx: any, tz: string): RawHourData[] {
  const h = wx.hourly ?? {};
  const times: string[] = h.time ?? [];
  const out: RawHourData[] = [];
  for (let i = 0; i < times.length; i++) {
    const ts = times[i];  // e.g., "2026-05-27T14:00"
    const [date, hourStr] = ts.split("T");
    const localHour = parseInt(hourStr.slice(0, 2), 10);
    const hourLabel = hourLabelFmt(localHour);
    out.push({
      forecastTs: `${ts}:00${getOffsetSuffix(tz, date)}`,
      localDate: date,
      localHour,
      hourLabel,
      isDaylight: !!h.is_day?.[i],
      weatherCode:   h.weather_code?.[i] ?? null,
      tempAir:       h.temperature_2m?.[i] ?? null,
      feelsLike:     h.apparent_temperature?.[i] ?? null,
      windSpeed:     h.wind_speed_10m?.[i] ?? null,
      precipChance:  h.precipitation_probability?.[i] ?? null,
      uvIndex:       h.uv_index?.[i] ?? null,
      cloudCover:    h.cloud_cover?.[i] ?? null,
      isParkOpen:    localHour >= 5 && localHour <= 22,  // default dawn-to-dusk-ish
    });
  }
  return out;
}

function hourLabelFmt(h: number): string {
  const ampm = h >= 12 ? "pm" : "am";
  const h12 = ((h + 11) % 12) + 1;
  return `${h12}${ampm}`;
}

function getOffsetSuffix(tz: string, date: string): string {
  // Stub: forecast_ts isn't critical for this experiment; use Z fallback.
  return "Z";
}

function localDate(tz: string): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit",
  }).formatToParts(new Date());
  const get = (t: string) => parts.find(p => p.type === t)?.value ?? "";
  return `${get("year")}-${get("month")}-${get("day")}`;
}

function tzForState(state: string | null | undefined): string | null {
  if (!state) return null;
  const map: Record<string, string> = {
    CA: "America/Los_Angeles", OR: "America/Los_Angeles", WA: "America/Los_Angeles", NV: "America/Los_Angeles",
    AK: "America/Anchorage", HI: "Pacific/Honolulu",
    AZ: "America/Phoenix",
    MT: "America/Denver", ID: "America/Denver", UT: "America/Denver", WY: "America/Denver",
    CO: "America/Denver", NM: "America/Denver",
    ND: "America/Chicago", SD: "America/Chicago", NE: "America/Chicago", KS: "America/Chicago",
    OK: "America/Chicago", TX: "America/Chicago", MN: "America/Chicago", IA: "America/Chicago",
    MO: "America/Chicago", AR: "America/Chicago", LA: "America/Chicago", WI: "America/Chicago",
    IL: "America/Chicago", MS: "America/Chicago", AL: "America/Chicago", TN: "America/Chicago",
    KY: "America/New_York", IN: "America/New_York", MI: "America/Detroit", OH: "America/New_York",
    GA: "America/New_York", FL: "America/New_York", SC: "America/New_York", NC: "America/New_York",
    VA: "America/New_York", WV: "America/New_York", DC: "America/New_York", MD: "America/New_York",
    DE: "America/New_York", PA: "America/New_York", NJ: "America/New_York", NY: "America/New_York",
    CT: "America/New_York", RI: "America/New_York", MA: "America/New_York", NH: "America/New_York",
    VT: "America/New_York", ME: "America/New_York",
  };
  return map[state] ?? null;
}

function avgOf(arr: (number | null)[]): number | null {
  const nums = arr.filter((v): v is number => v !== null);
  if (!nums.length) return null;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

// ─── Sonnet narrative (60-min cache via dog_park_chat_cache) ────────────

async function sha256Hex(s: string): Promise<string> {
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, "0")).join("");
}

async function callScoutLlmCached(
  supabase: any, fid: number, p: any, dp: any, op: any,
): Promise<{ answer: string; cached: boolean }> {
  const prompt = buildScoutPrompt(p, dp, op);
  const question = "Write the Scout intro paragraph for visiting this dog park today. Bold the time window **right now**.";
  const hash = await sha256Hex(prompt + "␟" + question);
  const { data: cached } = await supabase
    .from("dog_park_chat_cache")
    .select("answer, generated_at")
    .eq("dog_park_fid", fid)
    .eq("question_hash", hash)
    .maybeSingle();
  if (cached?.answer && cached?.generated_at) {
    const ageMs = Date.now() - new Date(cached.generated_at).getTime();
    if (ageMs < 60 * 60 * 1000) {
      return { answer: cached.answer, cached: true };
    }
  }
  const answer = await callAnthropic(prompt, question);
  supabase.from("dog_park_chat_cache").upsert({
    dog_park_fid: fid, question_hash: hash,
    answer, generated_at: new Date().toISOString(),
  }, { onConflict: "dog_park_fid,question_hash" }).then();
  return { answer, cached: false };
}

async function callAnthropic(systemPrompt: string, question: string): Promise<string> {
  const res = await fetch(ANTHROPIC_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: MODEL, max_tokens: 1024,
      system: systemPrompt,
      messages: [{ role: "user", content: question }],
    }),
  });
  if (!res.ok) throw new Error(`Anthropic ${res.status}: ${await res.text()}`);
  const data = await res.json();
  const text = (data.content ?? [])
    .map((b: any) => b.type === "text" ? (b.text ?? "") : "").join("\n");
  if (!text) throw new Error("Anthropic empty response");
  return text;
}

function buildScoutPrompt(p: any, dp: any, op: any): string {
  const name = p.display_name_override ?? p.name;
  const city = p.address_city ?? null;
  const state = p.state ?? null;
  const locationLine = [city, state].filter(Boolean).join(", ");
  const fence = (dp?.has_fence ?? p.has_fence);
  const water = (dp?.has_drinking_water ?? p.has_drinking_water);
  const surface = (dp?.surface_overlay ?? p.surface) ?? "unknown";

  const features: string[] = [];
  features.push("off-leash (definitional)");
  if (fence === true)  features.push("fully fenced");
  if (fence === false) features.push("UNFENCED — recall-trained dogs only");
  if (water === true)  features.push("on-site drinking water");
  if (water === false) features.push("no on-site water");
  if (dp?.double_gate === true)    features.push("double-gate entry");
  if (dp?.small_dog_area === true) features.push("small-dog area");
  if (dp?.large_dog_area === true) features.push("large-dog area");
  if (dp?.lighting === true)       features.push("evening lighting");
  if (surface && surface !== "unknown") features.push(`surface: ${surface}`);
  if (p.area_m2) features.push(`area ${(Number(p.area_m2) * 0.000247).toFixed(1)} acres`);

  const systemPrompt = [
    `You are Scout, a dog-park guide. Voice: warm, conversational, knowledgeable, dog-first.`,
    `Lead with the FUN — what's worth doing in the current window — not a forecast recitation.`,
    ``,
    `PARK: ${name}${locationLine ? ` (${locationLine})` : ""}`,
    `FEATURES: ${features.join("; ")}`,
    `HOURS: ${dp?.hours_text ?? "Dawn to dusk (default)"}`,
    `${dp?.description_overlay ?? p.description ? `CONTEXT (don't quote): ${dp?.description_overlay ?? p.description}` : ""}`,
    ``,
    `WRITING:`,
    `- ONE flowing paragraph, 3-4 sentences. No headers, no bullets.`,
    `- LEAD WITH THE FUN — stoke moment, not weather report.`,
    `- BOLD THE TIME WINDOW by wrapping it in **markdown asterisks**, e.g. **right now**. Don't bold anything else.`,
    `- Weave 2-3 concrete pack items (poop bags, water bottle, ball, etc.) naturally.`,
    `- NEVER suggest swimming or sand/surf (this is a DOG PARK).`,
    `- If unfenced, mention the long line as backup.`,
  ].filter(Boolean).join("\n");
  return systemPrompt;
}
