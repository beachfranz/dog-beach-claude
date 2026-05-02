// beach-chat/index.ts
// Supabase Edge Function — conversational assistant for beach conditions.
// Accepts POST { location_id, question, conversation_history }
// Returns { answer: string }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ANTHROPIC_API_KEY    = Deno.env.get("anthropic_api_key")!;
const ANTHROPIC_API_URL    = "https://api.anthropic.com/v1/messages";
const MODEL                = "claude-sonnet-4-20250514";
const MAX_TOKENS           = 1024;

// ─── Entry point ──────────────────────────────────────────────────────────────

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: cors });
  }

  if (req.method !== "POST") {
    return json({ error: "Method not allowed" }, 405);
  }

  // ── Rate limiting: 20 requests per IP per hour ──────────────────────────────
  const forwarded = req.headers.get("x-forwarded-for") ?? "";
  const ip        = forwarded.split(",").at(-1)?.trim() || "unknown";
  const hour = new Date(Math.floor(Date.now() / 3_600_000) * 3_600_000).toISOString();
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data: rateCount } = await supabase.rpc("increment_chat_rate", { p_ip: ip, p_hour: hour });
  if ((rateCount ?? 0) > 20) {
    return json({ answer: "I'm taking a quick break — try again in a little while." }, 429);
  }

  // Occasionally clean up old rate limit rows (older than 24 hours)
  if (Math.random() < 0.1) {
    await supabase.from("chat_rate_limits")
      .delete()
      .lt("hour", new Date(Date.now() - 86_400_000).toISOString());
  }

  let body: { location_id?: string; arena_group_id?: number; question?: string; conversation_history?: ConversationTurn[]; local_date?: string };
  try {
    body = await req.json();
  } catch {
    return json({ error: "Invalid JSON body" }, 400);
  }

  let { location_id, arena_group_id, question, conversation_history = [], local_date } = body;

  if (!question || (!location_id && !arena_group_id)) {
    return json({ error: "question and (location_id or arena_group_id) are required" }, 400);
  }

  // Resolve to location_id (still the slug used by scoring-table queries
  // until those tables drop their location_id PK).
  if (!location_id && arena_group_id) {
    const { data: row } = await supabase
      .from("beaches")
      .select("location_id")
      .eq("arena_group_id", arena_group_id)
      .limit(1);
    location_id = row?.[0]?.location_id ?? undefined;
    if (!location_id) {
      return json({ error: `No legacy slug found for arena_group_id=${arena_group_id}` }, 404);
    }
  }
  try {
    let systemPrompt: string;

    // local_date scopes the chat to a single beach + day (used by detail.html).
    // Comparative-question routing is bypassed: when the user is on a specific
    // day's detail page, they want answers about THAT day, not "go to a
    // different beach."
    if (isComparativeQuestion(question) && !local_date) {
      // ── Cross-beach mode: summary data for all beaches ──────────────
      // All beaches are in California — use Pacific time for "today"
      const todayPacific = localDateForTimezone(new Date(), "America/Los_Angeles");

      // Path 3b-3.3: read beaches_gold + JOIN slug for cross-beach mode.
      // Filter to scoreable so the prompt context is the curated set,
      // not all 763.
      const [{ data: beachesRaw }, { data: allDays }] = await Promise.all([
        supabase
          .from("beaches_gold")
          .select("fid, name, display_name_override, timezone, beaches!inner(location_id)")
          .eq("is_scoreable", true)
          .eq("is_active", true),
        supabase
          .from("beach_day_recommendations")
          .select("location_id, local_date, day_status, best_window_label, best_window_text, avg_temp, avg_wind, avg_uv, avg_tide_height, lowest_tide_height, busyness_category, go_hours_count, caution_hours_count, no_go_hours_count, caution_text, risk_reason_codes, positive_reason_codes, summary_weather, bacteria_risk, precip_72h_mm")
          .gte("local_date", todayPacific)
          .order("local_date", { ascending: true })
          .order("location_id", { ascending: true })
          .limit(50),
      ]);

      // Reshape gold rows into the legacy {location_id, display_name, timezone}
      // shape that buildCrossBeachPrompt expects.
      const beaches = (beachesRaw ?? []).map((g: { fid: number; name: string; display_name_override: string | null; timezone: string; beaches: { location_id: string } | { location_id: string }[] }) => {
        const pb = Array.isArray(g.beaches) ? g.beaches[0] : g.beaches;
        return {
          location_id:  pb?.location_id ?? null,
          display_name: g.display_name_override ?? g.name,
          timezone:     g.timezone ?? "America/Los_Angeles",
        };
      });
      systemPrompt = buildCrossBeachPrompt(beaches, allDays ?? []);

    } else {
      // ── Single-beach mode: full detail for current beach ─────────────
      // Path 3b-3.3: read beaches_gold (identity + scoring metadata) +
      // INNER JOIN public.beaches for legacy slug + curated marketing
      // text (address, website, description) the chat prompt uses.
      const { data: goldRows, error: beachErr } = await supabase
        .from("beaches_gold")
        .select(`
          fid,
          name,
          display_name_override,
          timezone,
          open_time,
          close_time,
          beaches!inner(location_id, address, website, description)
        `)
        .eq("beaches.location_id", location_id)
        .limit(1);

      if (beachErr || !goldRows?.length) {
        return json({ error: `Beach not found: ${location_id}` }, 404);
      }
      const g = goldRows[0] as { fid: number; name: string; display_name_override: string | null;
                                  timezone: string; open_time: string | null; close_time: string | null;
                                  beaches: { location_id: string; address: string | null;
                                             website: string | null; description: string | null }
                                           | { location_id: string; address: string | null;
                                               website: string | null; description: string | null }[] };
      const pb = Array.isArray(g.beaches) ? g.beaches[0] : g.beaches;
      const beach = {
        location_id:    pb.location_id,
        arena_group_id: g.fid,
        display_name:   g.display_name_override ?? g.name,
        timezone:       g.timezone ?? "America/Los_Angeles",
        open_time:      g.open_time,
        close_time:     g.close_time,
        address:        pb.address,
        website:        pb.website,
        description:    pb.description,
      };

      // LLM-extracted policy metadata for this beach (leash rules, dog
      // zones, hours, etc.). Drives Scout's activity advice — Scout must
      // not suggest off-leash play if leash is required, must not
      // suggest sand/wave play if dogs aren't allowed on sand, etc.
      let metadata: Record<string, unknown> | null = null;
      if (beach.arena_group_id) {
        const { data: meta } = await supabase
          .from("arena_beach_metadata")
          .select(
            "dogs_allowed, dogs_leash_required, dogs_off_leash_area, " +
            "dogs_seasonal_restrictions, dogs_time_restrictions, " +
            "dogs_policy_notes, dogs_allowed_areas, hours_text"
          )
          .eq("arena_group_id", beach.arena_group_id)
          .maybeSingle();
        metadata = meta ?? null;
      }

      // Get current local date + hour in the beach's timezone
      const nowUtc = new Date();
      const today  = localDateForTimezone(nowUtc, beach.timezone as string);
      const localParts = new Intl.DateTimeFormat("en-US", {
        timeZone: beach.timezone as string,
        hour: "2-digit", hour12: false,
      }).formatToParts(nowUtc);
      const currentLocalHour = parseInt(
        localParts.find(p => p.type === "hour")?.value ?? "0"
      ) % 24;
      const currentTimeLabel = new Intl.DateTimeFormat("en-US", {
        timeZone: beach.timezone as string,
        hour: "numeric", minute: "2-digit", hour12: true,
      }).format(nowUtc);

      // Scope: if local_date is provided, fetch only that one day. Otherwise
      // fetch the next 7 days starting today.
      const dayQuery = supabase
        .from("beach_day_recommendations")
        .select("*")
        .eq("location_id", location_id);
      const hourQuery = supabase
        .from("beach_day_hourly_scores")
        .select("local_date, local_hour, hour_label, hour_status, hour_score, tide_height, wind_speed, temp_air, precip_chance, uv_index, busyness_category, is_in_best_window, is_candidate_window, tide_status, wind_status, crowd_status, rain_status, temp_status, uv_status")
        .eq("location_id", location_id);

      const [{ data: days, error: daysErr }, { data: hours, error: hoursErr }] = await Promise.all([
        local_date
          ? dayQuery.eq("local_date", local_date)
          : dayQuery.gte("local_date", today).order("local_date", { ascending: true }).limit(7),
        local_date
          ? hourQuery.eq("local_date", local_date).order("local_hour", { ascending: true })
          : hourQuery.gte("local_date", today).order("local_date", { ascending: true }).order("local_hour", { ascending: true }),
      ]);

      if (daysErr) throw new Error(`Failed to load daily data: ${daysErr.message}`);
      if (hoursErr) throw new Error(`Failed to load hourly data: ${hoursErr.message}`);

      // Filter out past hours when the scoped day IS today.
      const filterDate = local_date ?? today;
      const remainingHours = (hours ?? []).filter(h =>
        h.local_date !== filterDate || Number(h.local_hour) >= currentLocalHour
      );

      systemPrompt = buildSystemPrompt(beach, days ?? [], remainingHours, currentTimeLabel, local_date ?? null, metadata);
    }

    const answer = await callAnthropic(systemPrompt, conversation_history, question);
    return json({ answer });

  } catch (err) {
    console.error("beach-chat error:", String(err));
    return json({ error: String(err) }, 500);
  }
});

// ─── Types ────────────────────────────────────────────────────────────────────

interface ConversationTurn {
  role: "user" | "assistant";
  content: string;
}

// ─── Prompt builder ───────────────────────────────────────────────────────────

function buildSystemPrompt(
  beach: Record<string, unknown>,
  days: Record<string, unknown>[],
  hours: Record<string, unknown>[],
  currentTimeLabel?: string,
  scopedDate: string | null = null,
  metadata: Record<string, unknown> | null = null,
): string {
  const hoursByDate = new Map<string, Record<string, unknown>[]>();
  for (const h of hours) {
    const date = h.local_date as string;
    const arr = hoursByDate.get(date) ?? [];
    arr.push(h);
    hoursByDate.set(date, arr);
  }

  const daysContext = days.map((d) => {
    const date = d.local_date as string;
    const dayHours = hoursByDate.get(date) ?? [];

    // is_weekend
    const jsDate = new Date(`${date}T12:00:00`);
    const dayOfWeek = jsDate.toLocaleDateString("en-US", { weekday: "long" });
    const isWeekend = dayOfWeek === "Saturday" || dayOfWeek === "Sunday";

    // feelsLike wind chill
    const avgTemp = d.avg_temp !== null ? parseFloat(String(d.avg_temp)) : null;
    const avgWind = d.avg_wind !== null ? parseFloat(String(d.avg_wind)) : null;
    let feelsLike: number | null = null;
    if (avgTemp !== null && avgWind !== null) {
      if (avgTemp <= 50 || avgWind >= 3) {
        feelsLike = Math.round(
          35.74 + 0.6215 * avgTemp - 35.75 * Math.pow(avgWind, 0.16) + 0.4275 * avgTemp * Math.pow(avgWind, 0.16)
        );
      } else {
        feelsLike = Math.round(avgTemp);
      }
    }

    // tide direction from best window hours
    const bestWindowHours = dayHours.filter((h) => h.is_in_best_window);
    const tideSeries = bestWindowHours
      .map((h) => h.tide_height !== null ? parseFloat(String(h.tide_height)) : null)
      .filter((v): v is number => v !== null);
    let tideDirection = "steady";
    if (tideSeries.length >= 2) {
      const diff = tideSeries[tideSeries.length - 1] - tideSeries[0];
      if (diff > 0.15)       tideDirection = "rising";
      else if (diff < -0.15) tideDirection = "falling";
    }

    // practical tips — dog needs first, human comfort secondary
    const tips: string[] = [];
    const avgUv = d.avg_uv !== null ? parseFloat(String(d.avg_uv)) : null;
    const lowestTide = d.lowest_tide_height !== null ? parseFloat(String(d.lowest_tide_height)) : null;

    // Dog essentials
    tips.push("fresh water and a bowl for the dog");
    if (lowestTide !== null && lowestTide <= 1.0) tips.push("low tide = great fetch/swim — bring a ball and towel for the dog");
    if (avgUv !== null && avgUv >= 6) tips.push(`dog sunscreen for nose/ears (UV ${Math.round(avgUv)})`);
    if (avgTemp !== null && avgTemp >= 80) tips.push("hot sand — dog booties or arrive early before it heats up");
    if (d.busyness_category === "dog_party" || d.busyness_category === "too_crowded") tips.push("long leash for crowded beach");

    // Human comfort (secondary)
    if (feelsLike !== null && feelsLike < 62) tips.push(`layer up — feels like ${feelsLike}°F`);

    // reason codes
    const positives = Array.isArray(d.positive_reason_codes) ? (d.positive_reason_codes as string[]).join(", ") : "";
    const risks     = Array.isArray(d.risk_reason_codes)     ? (d.risk_reason_codes     as string[]).join(", ") : "";

    // all daylight hours with status flags
    const allDaylightHours = dayHours.filter((h) => h.is_candidate_window || h.is_in_best_window);
    const hourLines = allDaylightHours.map((h) => {
      const flags = [
        h.tide_status  !== "go" && h.tide_status  ? `tide:${h.tide_status}`   : null,
        h.wind_status  !== "go" && h.wind_status  ? `wind:${h.wind_status}`   : null,
        h.rain_status  !== "go" && h.rain_status  ? `rain:${h.rain_status}`   : null,
        h.crowd_status !== "go" && h.crowd_status ? `crowd:${h.crowd_status}` : null,
        h.temp_status  !== "go" && h.temp_status  ? `temp:${h.temp_status}`   : null,
        h.uv_status    !== "go" && h.uv_status    ? `uv:${h.uv_status}`       : null,
      ].filter(Boolean).join(", ");
      const marker = h.is_in_best_window ? " ★" : "";
      return `    ${h.hour_label}${marker}: tide=${fmtNum(h.tide_height, "ft")} wind=${fmtNum(h.wind_speed, "mph")} temp=${fmtNum(h.temp_air, "°F")} rain=${fmtNum(h.precip_chance, "%")} crowd=${h.busyness_category ?? "?"} [${h.hour_status}]${flags ? ` flags: ${flags}` : ""}`;
    }).join("\n");

    const bacteriaRisk = d.bacteria_risk ?? "none";
    const bacteriaLine = bacteriaRisk === "high"     ? `  ⚠️ BACTERIA RISK HIGH: ${d.precip_72h_mm ?? 0}mm rain in past 72h — advise against swimming`
                       : bacteriaRisk === "moderate" ? `  ⚠️ BACTERIA ADVISORY: ${d.precip_72h_mm ?? 0}mm rain in past 72h — above 2.5mm advisory threshold`
                       : bacteriaRisk === "low"      ? `  Note: ${d.precip_72h_mm ?? 0}mm rain in past 72h (below advisory threshold)`
                       : "";

    return `
  ${date} ${dayOfWeek.toUpperCase()} (${d.day_status?.toString().toUpperCase()}) ${isWeekend ? "[WEEKEND]" : "[WEEKDAY]"}
  Hours: ${d.go_hours_count ?? 0} go / ${d.caution_hours_count ?? 0} caution / ${d.no_go_hours_count ?? 0} no-go
  Best window: ${d.best_window_label ?? "none"} | Weather: ${d.summary_weather ?? "unknown"} | Tide: ${fmtNum(d.avg_tide_height, "ft")} avg, ${fmtNum(lowestTide, "ft")} low, ${tideDirection} | Wind: ${fmtNum(d.avg_wind, "mph")} | Temp: ${fmtNum(d.avg_temp, "°F")}${feelsLike !== null ? ` (feels ${feelsLike}°F)` : ""} | UV: ${fmtNum(d.avg_uv, "")} | Crowds: ${d.busyness_category ?? "unknown"}
  ${positives ? `Positives: ${positives}` : ""}
  ${risks ? `Risks: ${risks}` : ""}
  ${bacteriaLine}
  ${tips.length ? `Tips: ${tips.join("; ")}` : ""}
  Best window note: ${d.best_window_text ?? "n/a"}
  ${d.caution_text ? `Caution: ${d.caution_text}` : ""}${d.no_go_text ? `No-go reason: ${d.no_go_text}` : ""}
  Hourly breakdown (★ = best window):
${hourLines || "    (none)"}`;
  }).join("\n");

  // ── Dog policy block (extracted via arena_beach_metadata) ───────────
  // Scout MUST respect these rules. They drive activity recommendations:
  // if dogs aren't allowed on sand, don't suggest fetch/wave play; if
  // leash required, don't suggest letting dog run free; for mixed_by_zone,
  // direct advice toward the off-leash zone specifically.
  const dogPolicyLines: string[] = [];
  let dogAdviceConstraints = "";
  if (metadata) {
    const dogsAllowed   = (metadata.dogs_allowed as string | null) || null;
    const leashRequired = (metadata.dogs_leash_required as string | null) || null;
    const offLeashArea  = (metadata.dogs_off_leash_area as string | null) || null;
    const seasonal      = (metadata.dogs_seasonal_restrictions as string | null) || null;
    const timeRules     = (metadata.dogs_time_restrictions as string | null) || null;
    const allowedAreas  = (metadata.dogs_allowed_areas as string | null) || null;
    const policyNotes   = (metadata.dogs_policy_notes as string | null) || null;

    if (dogsAllowed)   dogPolicyLines.push(`- dogs_allowed: ${dogsAllowed}`);
    if (leashRequired) dogPolicyLines.push(`- leash: ${leashRequired}`);
    if (offLeashArea)  dogPolicyLines.push(`- off_leash_area: ${trim(offLeashArea, 200)}`);
    if (allowedAreas)  dogPolicyLines.push(`- allowed_areas: ${trim(allowedAreas, 200)}`);
    if (seasonal)      dogPolicyLines.push(`- seasonal: ${trim(seasonal, 200)}`);
    if (timeRules)     dogPolicyLines.push(`- time_rules: ${trim(timeRules, 200)}`);
    if (policyNotes)   dogPolicyLines.push(`- notes: ${trim(policyNotes, 400)}`);

    // Translate the structured fields into hard activity constraints
    const constraints: string[] = [];
    if (dogsAllowed === "no") {
      constraints.push("Dogs are NOT allowed on the sand at this beach. Do NOT suggest fetch, swim, or any sand/wave activity. If there is an allowed_area (parking lot, multi-use trail), point the user there and make the best of it. If no allowed area exists, gently say this isn't a dog beach today.");
    } else if (leashRequired === "required") {
      constraints.push("Leash is REQUIRED. Never suggest letting the dog run free, off-leash fetch, or unrestrained swimming. Suggest leash-friendly activities: walks along the waterline, on-leash swim, sniff time on the dunes.");
    } else if (leashRequired === "mixed_by_zone") {
      constraints.push("Leash rules vary by zone. Off-leash activity is ONLY OK in the designated zone (see allowed_areas / off_leash_area / notes). Outside that zone the dog must be leashed. Always direct the user to the specific off-leash area when suggesting active play.");
    } else if (leashRequired === "varies_by_time") {
      constraints.push("Leash rules vary by time of day or season. Check seasonal/time_rules carefully before suggesting off-leash activity. When in doubt or outside the off-leash window, default to leashed advice.");
    }
    if (dogsAllowed === "seasonal" || seasonal) {
      constraints.push("Seasonal restrictions apply. Confirm the current date is within the dog-friendly window before recommending off-leash play; if outside the window, treat as leash-required or no-dogs as appropriate.");
    }
    dogAdviceConstraints = constraints.length
      ? `\nDOG POLICY CONSTRAINTS (HARD RULES — never violate):\n${constraints.map(c => `- ${c}`).join("\n")}\n`
      : "";
  }
  const dogPolicyBlock = dogPolicyLines.length
    ? `\nDOG POLICY (extracted from official source):\n${dogPolicyLines.join("\n")}\n`
    : "";

  return `You are Scout — a local surfer who's been bringing your dog to ${beach.display_name} for years. You know every sandbar, every swell window, when the kooks show up, and when it's firing. You text like a surfer — laid back, uses surf/beach slang naturally (swell, glassy, onshore, sectiony, blown out, dawn patrol, dropping in, firing, going off, closeout, mushy, punchy, clean, choppy, overhead, waist-high), first-person, never formal. You're stoked to help but keep it real — if it's blown out, say it's blown out.

BEACH: ${beach.display_name}
${beach.address ? `Address: ${beach.address}` : ""}
${beach.open_time ? `Hours: ${beach.open_time} – ${beach.close_time}` : ""}
${beach.description ? `About: ${beach.description}` : ""}
${beach.website ? `Website: ${beach.website}` : ""}
Timezone: ${beach.timezone}
${dogPolicyBlock}${dogAdviceConstraints}

${currentTimeLabel ? `Current local time: ${currentTimeLabel} — only today's remaining hours are shown in the hourly data below.` : ""}
${scopedDate
  ? `SCOPE: This conversation is about ${scopedDate} ONLY. The data below is for that single day. If the user asks about another date, the weather a different day, or whether they should go on a different day, say you can only speak to ${scopedDate} on this screen and tell them to switch to that day's view.\n\nFORECAST DATA (single day):`
  : `7-DAY FORECAST DATA:`}
${daysContext}

Rules:
- Answer questions about conditions, timing, crowds, tides, weather using the data above
- Reference specific hours, dates, and numbers when relevant — but weave them in naturally, don't just list them
${scopedDate
  ? `- DO NOT mention or recommend any other day. Only ${scopedDate}.`
  : `- If the user asks about a day not in the data, say you only have 7 days ahead`}
- Keep answers to 2 sentences max, 3 only if a third sentence meaningfully adds context to your answer
- Lead with a direct answer to the question — no preamble, no restating the question
- No emojis, no markdown formatting, plain text only
- If conditions are bad, say so honestly — don't sugarcoat it
- Crowd terms: quiet = few people, moderate = getting busy, dog_party = packed with dogs, too_crowded = avoid
- Never mention numeric scores (hour_score, tide_score, etc.) unless the user explicitly asks about them — use the conditions and statuses to inform your language instead
- When giving pack advice, lead with the dog's needs (water, towel, fetch ball, sunscreen, booties, leash) — human comfort items are secondary
- Always assume the user is bringing their dog; frame all advice through that lens
- DOG POLICY is non-negotiable — never suggest activities that violate the leash rule or "no dogs on sand" rule above. If the policy says leash required, the dog stays leashed; if dogs aren't allowed on sand, point the user to the allowed zone (parking lot / multi-use trail) and make the most of that. Don't argue with the policy or hedge — Scout knows the local rules cold and respects them.`;
}

function trim(s: string, n: number): string {
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}

// ─── Comparative question detection ──────────────────────────────────────────

function isComparativeQuestion(question: string): boolean {
  const q = question.toLowerCase();
  return /\b(which beach|what beach|best beach|other beach|all beach|compare|versus|vs\.?|least crowded|most crowded|quietest|busiest|better beach|anywhere else|other option|other spot)\b/.test(q);
}

// ─── Cross-beach prompt builder ───────────────────────────────────────────────

function buildCrossBeachPrompt(
  beaches: Record<string, unknown>[],
  allDays: Record<string, unknown>[],
): string {
  const daysByBeach = new Map<string, Record<string, unknown>[]>();
  for (const d of allDays) {
    const loc = d.location_id as string;
    const arr = daysByBeach.get(loc) ?? [];
    arr.push(d);
    daysByBeach.set(loc, arr);
  }

  const beachContext = beaches.map((b) => {
    const days = daysByBeach.get(b.location_id as string) ?? [];
    const dayLines = days.map((d) => {
      const risks = Array.isArray(d.risk_reason_codes) ? (d.risk_reason_codes as string[]).join(", ") : "";
      return `    ${d.local_date} (${d.day_status?.toString().toUpperCase()}): window=${d.best_window_label ?? "none"} weather=${d.summary_weather ?? "?"} wind=${fmtNum(d.avg_wind, "mph")} temp=${fmtNum(d.avg_temp, "°F")} crowds=${d.busyness_category ?? "?"} go=${d.go_hours_count ?? 0}h${risks ? ` risks=${risks}` : ""}${d.caution_text ? ` caution="${d.caution_text}"` : ""}`;
    }).join("\n");
    return `\n${b.display_name} (${b.location_id}):\n${dayLines || "    (no data)"}`;
  }).join("\n");

  return `You are Scout — a local surfer who knows every dog beach in Southern California. You've scouted all of them and know their differences — which ones get crowded on weekends, which have the best low tides, which get blown out in the afternoon. Casual surfer tone, first-person, no fluff.

ALL BEACHES — 7-DAY SUMMARY:
${beachContext}

Rules:
- Answer cross-beach comparison questions using the data above
- Recommend specific beaches and days with reasons — be direct
- Use descriptive language for conditions, not raw numbers where possible
- Keep answers to 2-3 sentences
- Lead with the direct answer, no preamble
- No emojis, no markdown, plain text only
- Never mention numeric scores unless asked
- Crowd terms: quiet = few people, moderate = getting busy, dog_party = packed with dogs, too_crowded = avoid`;
}

// ─── Anthropic call ───────────────────────────────────────────────────────────

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
      "Content-Type":      "application/json",
      "x-api-key":         ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model:      MODEL,
      max_tokens: MAX_TOKENS,
      system:     systemPrompt,
      messages,
    }),
  });

  if (!res.ok) {
    throw new Error(`Anthropic API error ${res.status}: ${await res.text()}`);
  }

  const data = await res.json();
  const text = (data.content ?? [])
    .map((b: { type: string; text?: string }) => b.type === "text" ? b.text ?? "" : "")
    .filter(Boolean)
    .join("\n");

  if (!text) throw new Error("Anthropic returned empty response");
  return text;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function fmtNum(val: unknown, unit: string): string {
  if (val === null || val === undefined) return "n/a";
  const n = typeof val === "number" ? val : parseFloat(String(val));
  return isNaN(n) ? "n/a" : `${n}${unit}`;
}

function localDateForTimezone(date: Date, timezone: string): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric", month: "2-digit", day: "2-digit",
  }).formatToParts(date);
  const get = (t: string) => parts.find(p => p.type === t)?.value ?? "";
  return `${get("year")}-${get("month")}-${get("day")}`;
}

