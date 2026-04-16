// get-calendar-event/index.ts
// Returns an ICS file for the best window at a given beach on a given date.
// GET ?location_id=huntington-dog-beach&date=2026-04-14

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req: Request) => {
  const cors = corsHeaders(req, "GET, OPTIONS");

  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: cors });
  }

  const url = new URL(req.url);
  const location_id = url.searchParams.get("location_id");
  const date        = url.searchParams.get("date");

  if (!location_id || !date) {
    return text("location_id and date are required", 400);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data: beach, error: beachErr } = await supabase
    .from("beaches")
    .select("display_name, address, timezone")
    .eq("location_id", location_id)
    .single();

  if (beachErr || !beach) {
    return text(`Beach not found: ${location_id}`, 404);
  }

  const { data: day, error: dayErr } = await supabase
    .from("beach_day_recommendations")
    .select("best_window_start_ts, best_window_end_ts, best_window_label, best_window_text, day_text")
    .eq("location_id", location_id)
    .eq("local_date", date)
    .single();

  if (dayErr || !day) {
    return text(`No data for ${location_id} on ${date}`, 404);
  }

  if (!day.best_window_start_ts || !day.best_window_end_ts) {
    return text("No best window available for this day", 404);
  }

  const dtStart  = toIcsDate(day.best_window_start_ts);
  const dtEnd    = toIcsDate(day.best_window_end_ts);
  const now      = toIcsDate(new Date().toISOString());
  const uid      = `${location_id}-${date}@dog-beach`;
  const summary  = `Best window at ${beach.display_name}`;
  const location = beach.address ?? beach.display_name;
  const description = [day.best_window_text, day.day_text].filter(Boolean).join(" ").replace(/\n/g, "\\n");

  const ics = [
    "BEGIN:VCALENDAR",
    "VERSION:2.0",
    "PRODID:-//Dog Beach//Beach Alert//EN",
    "CALSCALE:GREGORIAN",
    "METHOD:PUBLISH",
    "BEGIN:VEVENT",
    `UID:${uid}`,
    `DTSTAMP:${now}`,
    `DTSTART:${dtStart}`,
    `DTEND:${dtEnd}`,
    `SUMMARY:${summary}`,
    `LOCATION:${location}`,
    description ? `DESCRIPTION:${description}` : "",
    "END:VEVENT",
    "END:VCALENDAR",
  ].filter(Boolean).join("\r\n");

  return new Response(ics, {
    status: 200,
    headers: {
      ...cors,
      "Content-Type": "text/calendar; charset=utf-8",
      "Content-Disposition": `attachment; filename="beach-${date}.ics"`,
    },
  });
});

// ─── Helpers ──────────────────────────────────────────────────────────────────

function toIcsDate(isoString: string): string {
  return new Date(isoString).toISOString().replace(/[-:]/g, "").split(".")[0] + "Z";
}

function text(msg: string, status = 200): Response {
  return new Response(msg, { status, headers: { "Content-Type": "text/plain" } });
}
