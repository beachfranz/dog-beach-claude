// dog-park-chat/index.ts
// Supabase Edge Function — Scout-voice narrative for a dog park.
// Mirrors beach-chat's structure but the prompt is dog-park-attuned
// (no tides; fence/water/double-gate/asphalt-heat/surface emphasis).
//
// Accepts POST { dog_park_fid, question, conversation_history? }
// Returns { answer: string }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ANTHROPIC_API_KEY    = Deno.env.get("anthropic_api_key")!;
const ANTHROPIC_API_URL    = "https://api.anthropic.com/v1/messages";
const MODEL                = "claude-sonnet-4-6";
const MAX_TOKENS           = 1024;

interface ConversationTurn { role: "user" | "assistant"; content: string; }

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: cors });
  if (req.method !== "POST")    return json({ error: "Method not allowed" }, 405);

  // ── Rate limit: 20/IP/hour (shares the same chat_rate_limits bucket) ──
  const ip = (req.headers.get("x-forwarded-for") ?? "").split(",").at(-1)?.trim() || "unknown";
  const hour = new Date(Math.floor(Date.now() / 3_600_000) * 3_600_000).toISOString();
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  const { data: rateCount } = await supabase.rpc("increment_chat_rate", { p_ip: ip, p_hour: hour });
  if ((rateCount ?? 0) > 20) {
    return json({ answer: "I'm taking a quick break — try again in a little while." }, 429);
  }

  let body: { dog_park_fid?: number; question?: string;
              conversation_history?: ConversationTurn[]; local_date?: string };
  try { body = await req.json(); }
  catch { return json({ error: "Invalid JSON" }, 400); }

  const { dog_park_fid, question, conversation_history = [], local_date } = body;
  if (!question || !dog_park_fid) {
    return json({ error: "question and dog_park_fid are required" }, 400);
  }

  try {
    // ── Fetch park + policy + today + now ──────────────────────────
    const [parkRes, daysRes, nowRes] = await Promise.all([
      supabase.from("dog_parks_gold")
        .select(`
          fid, name, display_name_override, lat, lon,
          state, county_name, address_street, address_city,
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
          operator:inferred_operator_id(canonical_name, web_url)
        `)
        .eq("fid", dog_park_fid)
        .maybeSingle(),
      supabase.from("dog_park_day_recommendations")
        .select("*")
        .eq("dog_park_fid", dog_park_fid)
        .gte("local_date", new Date().toISOString().slice(0, 10))
        .order("local_date", { ascending: true })
        .limit(2),
      supabase.from("dog_park_hourly_scores")
        .select("*")
        .eq("dog_park_fid", dog_park_fid)
        .eq("is_now", true)
        .maybeSingle(),
    ]);

    if (parkRes.error || !parkRes.data) {
      return json({ error: `dog park not found: fid=${dog_park_fid}` }, 404);
    }

    const p = parkRes.data as Record<string, unknown>;
    const dp = Array.isArray(p.dog_park_dog_policy) ? p.dog_park_dog_policy[0] : p.dog_park_dog_policy;
    const op = Array.isArray(p.operator) ? p.operator[0] : p.operator;
    const today = (daysRes.data ?? [])[0];
    const now = nowRes.data;

    const systemPrompt = buildPrompt(p, dp, op, today, now);

    // ── 60-min LLM cache (single-park context, no history) ─────────
    const isCacheable = !conversation_history || conversation_history.length === 0;
    let cacheHash: string | null = null;
    if (isCacheable) {
      cacheHash = await sha256Hex(systemPrompt + "␟" + question);
      const { data: cached } = await supabase
        .from("dog_park_chat_cache")
        .select("answer, generated_at")
        .eq("dog_park_fid", dog_park_fid)
        .eq("question_hash", cacheHash)
        .maybeSingle();
      if (cached?.answer && cached?.generated_at) {
        const ageMs = Date.now() - new Date(cached.generated_at).getTime();
        if (ageMs < 60 * 60 * 1000) {
          return json({ answer: cached.answer, cached: true });
        }
      }
    }

    const answer = await callAnthropic(systemPrompt, conversation_history, question);

    if (isCacheable && cacheHash && answer) {
      supabase.from("dog_park_chat_cache").upsert({
        dog_park_fid, question_hash: cacheHash,
        answer, generated_at: new Date().toISOString(),
      }, { onConflict: "dog_park_fid,question_hash" }).then();
    }

    return json({ answer });
  } catch (err) {
    console.error("dog-park-chat error:", String(err));
    return json({ error: String(err) }, 500);
  }
});

// ─── Prompt builder ─────────────────────────────────────────────────────

function buildPrompt(
  p: any, dp: any, op: any, today: any, now: any,
): string {
  const name = p.display_name_override ?? p.name;
  const city = p.address_city ?? null;
  const state = p.state ?? null;
  const locationLine = [city, state].filter(Boolean).join(", ");

  const fence = (dp?.has_fence ?? p.has_fence);
  const water = (dp?.has_drinking_water ?? p.has_drinking_water);
  const surface = (dp?.surface_overlay ?? p.surface) ?? "unknown";
  const description = (dp?.description_overlay ?? p.description) ?? null;
  const hoursText = dp?.hours_text
    ?? (dp?.hours_open_time && dp?.hours_close_time
        ? `${dp.hours_open_time}–${dp.hours_close_time}`
        : null)
    ?? "Dawn to dusk (default)";

  const features: string[] = [];
  features.push("off-leash (definitional for a dog park)");
  if (fence === true)  features.push("fenced perimeter");
  if (fence === false) features.push("UNFENCED — recall-trained dogs only");
  if (water === true)  features.push("on-site drinking water");
  if (water === false) features.push("no on-site water — bring a bottle");
  if (dp?.double_gate === true)    features.push("double-gate airlock entry");
  if (dp?.small_dog_area === true) features.push("separate small-dog area");
  if (dp?.large_dog_area === true) features.push("separate large-dog area");
  if (dp?.lighting === true)       features.push("lighting (evening visits OK)");
  if (surface && surface !== "unknown") features.push(`surface: ${surface}`);
  if (p.area_m2) features.push(`area ≈ ${(Number(p.area_m2) * 0.000247).toFixed(1)} acres`);

  const conditions: string[] = [];
  if (now) {
    if (now.feels_like != null)   conditions.push(`feels-like ${Math.round(now.feels_like)}°F`);
    if (now.wind_speed != null)   conditions.push(`wind ${Math.round(now.wind_speed)}mph`);
    if (now.uv_index != null)     conditions.push(`UV ${Number(now.uv_index).toFixed(1)}`);
    if (now.precip_chance != null) conditions.push(`rain ${Math.round(now.precip_chance)}%`);
    // Pavement-hot threshold = 115°F (matches asphalt penalty curve start).
    // 125°F+ is the burn-risk gate; flag explicitly when there.
    if (now.asphalt_temp != null && now.asphalt_temp >= 125)
      conditions.push(`PAVEMENT BURN-RISK (~${Math.round(now.asphalt_temp)}°F) — keep paws OFF`);
    else if (now.asphalt_temp != null && now.asphalt_temp >= 115)
      conditions.push(`pavement warm (~${Math.round(now.asphalt_temp)}°F)`);
  } else if (today) {
    if (today.avg_temp != null) conditions.push(`avg ${Math.round(today.avg_temp)}°F`);
    if (today.avg_wind != null) conditions.push(`wind ${Math.round(today.avg_wind)}mph`);
    if (today.summary_weather)  conditions.push(`weather: ${today.summary_weather}`);
  }

  const window = today?.best_window_label ?? "during operating hours";

  // Pack-list suggestions — concrete items the LLM can name.
  // Dog park staples + condition-triggered extras.
  const pack: string[] = [];
  // Pack-list thresholds aligned with v2 scoring bands:
  //   asphalt 115+°F → paw concern   (burn-risk gate at 125)
  //   UV 8+         → very-high tier, shade matters
  //   feels-like 90+→ active-play heat-stroke risk; 50°F → cold penalty start
  pack.push("poop bags");
  if (water !== true) pack.push("water bottle and bowl");
  if (now?.asphalt_temp != null && now.asphalt_temp >= 115) pack.push("paw boots or stick to grass");
  if (now?.uv_index != null && now.uv_index >= 8) pack.push("shade for breaks");
  if (now?.feels_like != null && now.feels_like >= 90) pack.push("cooling towel");
  if (now?.feels_like != null && now.feels_like < 50) pack.push("a jacket for short-coat dogs");
  if (fence === false) pack.push("long line as recall backup");
  pack.push("a favorite ball or tug toy");

  // Activity prompts — dog-park-attuned (not beach sand+surf).
  const activities: string[] = [];
  if (dp?.small_dog_area === true) activities.push("small-dog area if your pup gets overwhelmed");
  if (dp?.large_dog_area === true) activities.push("large-dog area for the romp");
  if (fence === true) activities.push("safe fetch off-leash");
  else activities.push("fetch with the long line in case recall slips");
  if (water === true) activities.push("a fresh-water break at the on-site fountain");
  // v2 amenity additions (Franz 2026-05-27 LATE)
  if (dp?.has_agility === true) activities.push("a turn on the agility course");
  if (dp?.has_water_play === true) activities.push("a splash-pad cool-down");
  if (dp?.has_shade === true
      && ((now?.uv_index ?? 0) >= 6 || (now?.feels_like ?? 0) >= 80))
    activities.push("breaks in the shade between zoomies");
  if (dp?.has_picnic_tables === true) activities.push("a post-romp picnic at the tables");
  activities.push("zoomies with new dog friends");
  if (dp?.lighting === true) activities.push("evening visit under the lights");

  return [
    `You are Scout, a dog-park guide. Voice: warm, conversational, knowledgeable, dog-first. `,
    `Address the human like a friend who knows the place. Lead with the FUN — what's worth doing in the current window — not a forecast recitation.`,
    ``,
    `PARK: ${name}${locationLine ? ` (${locationLine})` : ""}`,
    `FEATURES: ${features.join("; ")}`,
    `HOURS: ${hoursText}`,
    `BEST WINDOW: ${window}`,
    `CURRENT CONDITIONS: ${conditions.length ? conditions.join(", ") : "(no live data)"}`,
    `${description ? `OPERATOR DESCRIPTION (for context, don't quote): ${description}` : ""}`,
    `${dp?.additional_rules ? `POSTED RULES (don't quote, but respect): ${dp.additional_rules}` : ""}`,
    ``,
    `WRITING DIRECTIVES:`,
    `- ONE flowing conversational paragraph, 3-4 sentences. No headers, no bullets.`,
    `- LEAD WITH THE FUN — frame the window as a stoke moment, not a weather report.`,
    `- DO NOT restate the time window (e.g. "3pm-8pm") — it's already in the headline above the paragraph. Describe the vibe / what's happening, not the clock.`,
    `- Weave 2-3 activities naturally from this list — DON'T list them, DON'T invent new ones: ${activities.join(" · ")}.`,
    `- Weave 2-3 pack items concretely from this list — DON'T list them, DON'T invent gear: ${pack.join(" · ")}.`,
    `- Address active cautions with CONCRETE practical guidance (thresholds match v2 scoring bands):`,
    `    • asphalt 115-124°F (warm) → "pavement's warming up — stick to grass on hot afternoons"`,
    `    • asphalt 125°F+ (burn-risk gate) → "DON'T walk on asphalt — booties or carry to grass, paws will burn"`,
    `    • UV 6-7 (high) → "sun's strong — shade breaks every 15 min"`,
    `    • UV 8-10 (very high) → "intense sun — minimize bare-paw pavement time, frequent water"`,
    `    • UV 11+ (extreme, gate) → "extreme UV — keep it brief and seek shade constantly"`,
    `    • wind 13-18mph (nuisance) → "breezy enough to blow papers — secure light gear"`,
    `    • wind 19-24mph → "fresh wind discouraging activity — watch for blowing debris"`,
    `    • wind 25mph+ (gale) → "wind's a real problem — light dogs can get spooked, keep the leash handy"`,
    `    • feels-like ≤50°F → "chilly for short-coat dogs — jacket recommended"`,
    `    • feels-like ≥95°F (caution) → "heat-stroke risk for active play — short sessions, lots of water"`,
    `    • unfenced → "no perimeter fence — keep the long line on until you read the dogs already there"`,
    `- NEVER suggest swimming or sand/surf activity (this is a DOG PARK, not a beach).`,
    `- NEVER suggest off-leash if the park has unfenced=true unless you also mention the long line.`,
    `- Don't reference times that have already passed.`,
    ``,
    `End the paragraph naturally. The UI will render a "Wanna go?" CTA below — don't write one yourself.`,
  ].filter(Boolean).join("\n");
}

async function sha256Hex(s: string): Promise<string> {
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, "0")).join("");
}

// ─── Anthropic call ─────────────────────────────────────────────────────

async function callAnthropic(
  systemPrompt: string,
  history: ConversationTurn[],
  question: string,
): Promise<string> {
  const messages = [
    ...history.map((t) => ({ role: t.role, content: t.content })),
    { role: "user", content: question },
  ];
  const res = await fetch(ANTHROPIC_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({ model: MODEL, max_tokens: MAX_TOKENS, system: systemPrompt, messages }),
  });
  if (!res.ok) throw new Error(`Anthropic ${res.status}: ${await res.text()}`);
  const data = await res.json();
  const text = (data.content ?? [])
    .map((b: { type: string; text?: string }) => b.type === "text" ? (b.text ?? "") : "")
    .filter(Boolean).join("\n");
  if (!text) throw new Error("Anthropic returned empty response");
  return text;
}
