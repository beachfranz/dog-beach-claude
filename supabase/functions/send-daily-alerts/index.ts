// send-daily-alerts/index.ts
// Finds subscribers whose notify_time matches the current UTC hour (in their timezone),
// generates a Claude blurb for today's best window, and sends an SMS via Twilio.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ANTHROPIC_API_KEY    = Deno.env.get("anthropic_api_key")!;
const TWILIO_ACCOUNT_SID   = Deno.env.get("TWILIO_ACCOUNT_SID")!;
const TWILIO_AUTH_TOKEN    = Deno.env.get("TWILIO_AUTH_TOKEN")!;
const TWILIO_FROM_NUMBER   = Deno.env.get("TWILIO_FROM_NUMBER")!;
const SUPABASE_PUBLIC_URL  = "https://ehlzbwtrsxaaukurekau.supabase.co";

const ANTHROPIC_API_URL    = "https://api.anthropic.com/v1/messages";
const MODEL                = "claude-sonnet-4-20250514";

Deno.serve(async (_req: Request) => {
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  const nowUtc   = new Date();

  // Load all active subscribers with at least one location
  const { data: subscribers, error: subErr } = await supabase
    .from("subscribers")
    .select("id, phone_e164, display_name, location_ids, notify_time, timezone")
    .eq("is_active", true)
    .not("location_ids", "eq", "{}");

  if (subErr) {
    console.error("Failed to load subscribers:", subErr.message);
    return json({ error: subErr.message }, 500);
  }

  const results: Record<string, unknown>[] = [];

  for (const sub of subscribers ?? []) {
    // Check if current UTC time matches this subscriber's notify_time in their timezone
    const localNow = new Date(nowUtc.toLocaleString("en-US", { timeZone: sub.timezone }));
    const localHour   = localNow.getHours();
    const localMinute = localNow.getMinutes();
    const [notifyHour, notifyMinute] = (sub.notify_time as string).split(":").map(Number);

    // Only fire within the matching hour (cron runs every hour)
    if (localHour !== notifyHour) continue;
    // Avoid re-sending if cron fires multiple times in the same hour
    if (localMinute > 10) continue;

    const localDate = localNow.toISOString().slice(0, 10);

    for (const location_id of sub.location_ids as string[]) {
      // Skip if already sent today
      const { data: existing } = await supabase
        .from("notification_log")
        .select("id")
        .eq("subscriber_id", sub.id)
        .eq("location_id", location_id)
        .eq("local_date", localDate)
        .eq("status", "sent")
        .maybeSingle();

      if (existing) {
        results.push({ subscriber_id: sub.id, location_id, status: "skipped", reason: "already sent today" });
        continue;
      }

      // Load today's recommendation
      const { data: day, error: dayErr } = await supabase
        .from("beach_day_recommendations")
        .select("best_window_label, best_window_start_ts, best_window_end_ts, best_window_text, day_text, day_status, busyness_category, avg_wind, avg_temp, avg_tide_height, lowest_tide_height, summary_weather")
        .eq("location_id", location_id)
        .eq("local_date", localDate)
        .single();

      if (dayErr || !day) {
        results.push({ subscriber_id: sub.id, location_id, status: "skipped", reason: "no data for today" });
        continue;
      }

      if (!day.best_window_label || day.day_status === "no_go") {
        results.push({ subscriber_id: sub.id, location_id, status: "skipped", reason: `day_status=${day.day_status}` });
        continue;
      }

      // Load beach name
      const { data: beach } = await supabase
        .from("beaches")
        .select("display_name")
        .eq("location_id", location_id)
        .single();

      const beachName = beach?.display_name ?? location_id;

      // Generate Claude blurb
      let blurb = day.best_window_text ?? day.day_text ?? "";
      try {
        blurb = await generateBlurb(beachName, day);
      } catch (err) {
        console.error("Claude blurb failed, falling back to day_text:", String(err));
      }

      // Build calendar link
      const calLink = `${SUPABASE_PUBLIC_URL}/functions/v1/get-calendar-event?location_id=${encodeURIComponent(location_id)}&date=${localDate}`;

      // Compose SMS
      const smsBody = [
        `${beachName} — best window today: ${day.best_window_label}`,
        blurb,
        `Add to calendar: ${calLink}`,
      ].join("\n\n");

      // Send via Twilio
      let status = "sent";
      let twilioSid: string | undefined;
      let errorMessage: string | undefined;

      try {
        twilioSid = await sendSms(sub.phone_e164, smsBody);
      } catch (err) {
        status = "failed";
        errorMessage = String(err);
        console.error(`SMS failed for ${sub.id}:`, errorMessage);
      }

      // Log result
      await supabase.from("notification_log").insert({
        subscriber_id:      sub.id,
        location_id,
        local_date:         localDate,
        status,
        twilio_message_sid: twilioSid,
        error_message:      errorMessage,
        sms_body:           smsBody,
      });

      results.push({ subscriber_id: sub.id, location_id, status, twilio_message_sid: twilioSid });
    }
  }

  return json({ sent: results.filter(r => r.status === "sent").length, results });
});

// ─── Claude blurb ─────────────────────────────────────────────────────────────

async function generateBlurb(beachName: string, day: Record<string, unknown>): Promise<string> {
  const prompt = `You're a local surfer giving a friend a one-sentence heads-up about conditions at ${beachName} today. Best window: ${day.best_window_label}. Weather: ${day.summary_weather ?? "unknown"}. Wind: ${day.avg_wind}mph. Temp: ${day.avg_temp}°F. Tide low: ${day.lowest_tide_height}ft. Crowds: ${day.busyness_category ?? "unknown"}. ${day.best_window_text ?? ""}. One sentence, no emojis, plain text, surfer tone.`;

  const res = await fetch(ANTHROPIC_API_URL, {
    method: "POST",
    headers: {
      "Content-Type":      "application/json",
      "x-api-key":         ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model:      MODEL,
      max_tokens: 100,
      messages:   [{ role: "user", content: prompt }],
    }),
  });

  if (!res.ok) throw new Error(`Anthropic error ${res.status}`);
  const data = await res.json();
  return data.content?.[0]?.text?.trim() ?? "";
}

// ─── Twilio ───────────────────────────────────────────────────────────────────

async function sendSms(to: string, body: string): Promise<string> {
  const url = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Messages.json`;
  const params = new URLSearchParams({ To: to, From: TWILIO_FROM_NUMBER, Body: body });

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type":  "application/x-www-form-urlencoded",
      "Authorization": "Basic " + btoa(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`),
    },
    body: params,
  });

  const data = await res.json();
  if (!res.ok) throw new Error(data.message ?? `Twilio error ${res.status}`);
  return data.sid;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
