// beach-chat/index.ts
// Supabase Edge Function — conversational assistant for beach conditions.
// Accepts POST { location_id, question, conversation_history }
// Returns { answer: string }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ANTHROPIC_API_KEY    = Deno.env.get("anthropic_api_key")!;
const ANTHROPIC_API_URL    = "https://api.anthropic.com/v1/messages";
const MODEL                = "claude-sonnet-4-20250514";
const MAX_TOKENS           = 1024;

const CORS_HEADERS = {
  "Access-Control-Allow-Origin":  "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Content-Type": "application/json",
};

// ─── Entry point ──────────────────────────────────────────────────────────────

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS_HEADERS });
  }

  if (req.method !== "POST") {
    return json({ error: "Method not allowed" }, 405);
  }

  let body: { location_id?: string; question?: string; conversation_history?: ConversationTurn[] };
  try {
    body = await req.json();
  } catch {
    return json({ error: "Invalid JSON body" }, 400);
  }

  const { location_id, question, conversation_history = [] } = body;

  if (!location_id || !question) {
    return json({ error: "location_id and question are required" }, 400);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  try {
    // 1. Fetch beach metadata
    const { data: beach, error: beachErr } = await supabase
      .from("beaches")
      .select("location_id, display_name, address, timezone, open_time, close_time, description, website")
      .eq("location_id", location_id)
      .single();

    if (beachErr || !beach) {
      return json({ error: `Beach not found: ${location_id}` }, 404);
    }

    // 2. Fetch next 7 days of daily recommendations
    const today = new Date().toISOString().slice(0, 10);
    const { data: days, error: daysErr } = await supabase
      .from("beach_day_recommendations")
      .select("*")
      .eq("location_id", location_id)
      .gte("local_date", today)
      .order("local_date", { ascending: true })
      .limit(7);

    if (daysErr) throw new Error(`Failed to load daily data: ${daysErr.message}`);

    // 3. Fetch hourly scores for those days
    const { data: hours, error: hoursErr } = await supabase
      .from("beach_day_hourly_scores")
      .select("local_date, local_hour, hour_label, hour_status, hour_score, tide_height, wind_speed, temp_air, precip_chance, uv_index, busyness_category, is_in_best_window, is_candidate_window")
      .eq("location_id", location_id)
      .gte("local_date", today)
      .order("local_date", { ascending: true })
      .order("local_hour", { ascending: true });

    if (hoursErr) throw new Error(`Failed to load hourly data: ${hoursErr.message}`);

    // 4. Build context and call Anthropic
    const systemPrompt = buildSystemPrompt(beach, days ?? [], hours ?? []);
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
    const candidateHours = dayHours.filter((h) => h.is_candidate_window);

    const hourLines = candidateHours.map((h) =>
      `    ${h.hour_label}: status=${h.hour_status}${h.is_in_best_window ? " (BEST WINDOW)" : ""} tide=${fmtNum(h.tide_height, "ft")} wind=${fmtNum(h.wind_speed, "mph")} temp=${fmtNum(h.temp_air, "°F")} rain=${fmtNum(h.precip_chance, "%")} crowd=${h.busyness_category ?? "unknown"} score=${h.hour_score ?? "n/a"}`
    ).join("\n");

    return `
  ${date} (${d.day_status?.toString().toUpperCase()})
  Best window: ${d.best_window_label ?? "none"} | Weather: ${d.summary_weather ?? "unknown"} | Avg tide: ${fmtNum(d.avg_tide_height, "ft")} | Low tide: ${fmtNum(d.lowest_tide_height, "ft")} | Wind: ${fmtNum(d.avg_wind, "mph")} | Temp: ${fmtNum(d.avg_temp, "°F")} | UV: ${fmtNum(d.avg_uv, "")} | Crowds: ${d.busyness_category ?? "unknown"}
  Summary: ${d.day_text ?? "no summary"}
  Best window note: ${d.best_window_text ?? "n/a"}
  ${d.caution_text ? `Caution: ${d.caution_text}` : ""}${d.no_go_text ? `No-go reason: ${d.no_go_text}` : ""}
  Candidate hours:
${hourLines || "    (none)"}`;
  }).join("\n");

  return `You're a local surfer who's been bringing your dog to ${beach.display_name} for years. You know every sandbar, every swell window, when the kooks show up, and when it's firing. You text like a surfer — laid back, uses surf/beach slang naturally (swell, glassy, onshore, sectiony, blown out, dawn patrol, dropping in, firing, going off, closeout, mushy, punchy, clean, choppy, overhead, waist-high), first-person, never formal. You're stoked to help but keep it real — if it's blown out, say it's blown out.

BEACH: ${beach.display_name}
${beach.address ? `Address: ${beach.address}` : ""}
${beach.open_time ? `Hours: ${beach.open_time} – ${beach.close_time}` : ""}
${beach.description ? `About: ${beach.description}` : ""}
${beach.website ? `Website: ${beach.website}` : ""}
Timezone: ${beach.timezone}

7-DAY FORECAST DATA:
${daysContext}

Rules:
- Answer questions about conditions, timing, crowds, tides, weather using the data above
- Reference specific hours, dates, and numbers when relevant — but weave them in naturally, don't just list them
- If the user asks about a day not in the data, say you only have 7 days ahead
- Keep answers concise — 2-5 sentences unless they ask for detail
- No emojis, no markdown formatting, plain text only
- If conditions are bad, say so honestly — don't sugarcoat it
- Crowd terms: quiet = few people, moderate = getting busy, dog_party = packed with dogs`;
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

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: CORS_HEADERS });
}
