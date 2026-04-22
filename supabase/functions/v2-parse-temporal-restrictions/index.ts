// v2-parse-temporal-restrictions/index.ts
// Parses existing free-text temporal restrictions into structured fields
// suitable for day/hour scoring. No new web research — just interprets the
// text we already captured.
//
// Reads from:  dogs_time_restrictions, dogs_season_restrictions, dogs_policy_notes
// Writes to:   dogs_seasonal_closures (jsonb), dogs_daily_windows (jsonb),
//              dogs_day_of_week_mask (smallint), dogs_prohibited_reason (text)
//
// Groups by governing_body — all beaches under the same body share the same
// research-generated text, so one Claude call per body is enough.
//
// POST { dry_run?: boolean, body_filter?: string, limit?: number }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import Anthropic from "https://esm.sh/@anthropic-ai/sdk@0.39.0";
import { corsHeaders } from "../_shared/cors.ts";
import { stateCodeFromName } from "../_shared/config.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ANTHROPIC_API_KEY    = Deno.env.get("anthropic_api_key")!;

const MODEL       = "claude-haiku-4-5-20251001";
const CONCURRENCY = 5;

const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

interface Window { start: string; end: string; }
interface Closure { start: string; end: string; reason?: string; }

interface Parsed {
  dogs_seasonal_closures:  Closure[] | null;
  dogs_daily_windows:      Window[] | null;
  dogs_day_of_week_mask:   number   | null;
  dogs_prohibited_reason:  string   | null;
}

function emptyParsed(): Parsed {
  return {
    dogs_seasonal_closures: null,
    dogs_daily_windows:     null,
    dogs_day_of_week_mask:  null,
    dogs_prohibited_reason: null,
  };
}

async function parseText(body: string, timeRes: string | null, seasonRes: string | null, notes: string | null): Promise<Parsed> {
  const hasAnything = (timeRes && timeRes.trim()) || (seasonRes && seasonRes.trim()) || (notes && notes.trim());
  if (!hasAnything) return emptyParsed();

  const prompt = `You convert free-text dog-policy restrictions into structured fields for day/hour scoring.

Governing body: ${body}

Source text fields (any or all may be empty):

dogs_time_restrictions:
${timeRes ?? "(none)"}

dogs_season_restrictions:
${seasonRes ?? "(none)"}

dogs_policy_notes:
${notes ?? "(none)"}

Return a single FLAT JSON object with exactly these keys (no nested sections):

{
  "dogs_seasonal_closures": array or null,
  "dogs_daily_windows": array or null,
  "dogs_day_of_week_mask": integer 0-127 or null,
  "dogs_prohibited_reason": string or null
}

Semantics:

- dogs_seasonal_closures: 0+ blackout windows per year. Each is
    {"start": "MM-DD", "end": "MM-DD", "reason": "short phrase"}
  Example (snowy plover nesting): [{"start":"03-01","end":"09-30","reason":"snowy plover nesting"}]
  If dates aren't in the text, return null. If dogs are ALLOWED year-round with no seasonal closure, return null.

- dogs_daily_windows: 0+ allowed time-of-day windows. Each is
    {"start": "HH:MM", "end": "HH:MM"}  (24-hour, local time)
  Example "before 9am and after 5pm" = [{"start":"00:00","end":"09:00"},{"start":"17:00","end":"23:59"}]
  Use null if dogs are allowed any time of day (no time restriction).
  Use [] (empty array) if dogs are NEVER allowed at any hour — prefer null in that case since the user-level answer comes from dogs_allowed.

- dogs_day_of_week_mask: 7-bit mask of ALLOWED days.
  Bit values: Sun=1, Mon=2, Tue=4, Wed=8, Thu=16, Fri=32, Sat=64.
  Common values:
    null = every day allowed (no day-of-week restriction)
    127 = every day
    62  = weekdays (Mon-Fri) only
    65  = weekends (Sat+Sun) only
  Use null unless the source text explicitly mentions day-of-week rules.

- dogs_prohibited_reason: short phrase explaining why dogs are restricted if any restriction applies ("snowy plover nesting", "lifeguard hours", "wildlife protection", "city ordinance"). null if no restriction or reason unclear.

Rules:
- Prefer null over guessing. Partial info is fine — not every field needs a value.
- Do NOT emit values for rules that are not in the source text.
- Respond with the JSON object only, no markdown fences, no commentary.`;

  try {
    const response = await anthropic.messages.create({
      model: MODEL, max_tokens: 800,
      messages: [{ role: "user", content: prompt }],
    });
    const text  = (response.content[0] as { type: string; text: string }).text.trim();
    const match = text.match(/\{[\s\S]*\}/);
    if (!match) return emptyParsed();
    const p = JSON.parse(match[0]);

    // Validate / sanitise
    const closures = Array.isArray(p.dogs_seasonal_closures)
      ? p.dogs_seasonal_closures.filter((c: unknown) =>
          c && typeof c === "object"
            && typeof (c as Closure).start === "string"
            && typeof (c as Closure).end   === "string"
            && /^\d{2}-\d{2}$/.test((c as Closure).start)
            && /^\d{2}-\d{2}$/.test((c as Closure).end))
      : null;

    const windows = Array.isArray(p.dogs_daily_windows)
      ? p.dogs_daily_windows.filter((w: unknown) =>
          w && typeof w === "object"
            && typeof (w as Window).start === "string"
            && typeof (w as Window).end   === "string"
            && /^\d{2}:\d{2}$/.test((w as Window).start)
            && /^\d{2}:\d{2}$/.test((w as Window).end))
      : null;

    const mask = (typeof p.dogs_day_of_week_mask === "number"
      && Number.isInteger(p.dogs_day_of_week_mask)
      && p.dogs_day_of_week_mask >= 0
      && p.dogs_day_of_week_mask <= 127)
      ? p.dogs_day_of_week_mask
      : null;

    const reason = typeof p.dogs_prohibited_reason === "string" && p.dogs_prohibited_reason.trim()
      ? p.dogs_prohibited_reason.trim()
      : null;

    return {
      dogs_seasonal_closures: closures && closures.length ? closures : null,
      dogs_daily_windows:     windows  && windows.length  ? windows  : null,
      dogs_day_of_week_mask:  mask,
      dogs_prohibited_reason: reason,
    };
  } catch {
    return emptyParsed();
  }
}

async function pLimit<T>(tasks: (() => Promise<T>)[], limit: number): Promise<T[]> {
  const results: T[] = new Array(tasks.length);
  let i = 0;
  async function worker() { while (i < tasks.length) { const n = i++; results[n] = await tasks[n](); } }
  await Promise.all(Array.from({ length: limit }, worker));
  return results;
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (b: unknown, s = 200) => new Response(JSON.stringify(b), { status: s, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { state_code?: string; dry_run?: boolean; body_filter?: string; limit?: number } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const stateCode = body.state_code ?? "CA";
  const supabase  = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // Distinct (governing_body, text inputs). All beaches with the same
  // governing_body share text, so one parse call per body is enough.
  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("governing_body, state, dogs_time_restrictions, dogs_season_restrictions, dogs_policy_notes")
    .eq("review_status", "ready")
    .not("governing_body", "is", null);
  if (error) return json({ error: error.message }, 500);

  // Group by body, filtering to the requested state; skip bodies with nothing to parse
  const stateFiltered = (rows ?? []).filter(r => stateCodeFromName(r.state) === stateCode);
  const byBody = new Map<string, { t: string | null; s: string | null; n: string | null }>();
  for (const r of stateFiltered) {
    if (byBody.has(r.governing_body)) continue;
    const any = (r.dogs_time_restrictions && r.dogs_time_restrictions.trim())
             || (r.dogs_season_restrictions && r.dogs_season_restrictions.trim())
             || (r.dogs_policy_notes && r.dogs_policy_notes.trim());
    if (!any) continue;
    byBody.set(r.governing_body, {
      t: r.dogs_time_restrictions,
      s: r.dogs_season_restrictions,
      n: r.dogs_policy_notes,
    });
  }
  let bodies = [...byBody.keys()];
  if (body.body_filter) bodies = bodies.filter(b => b.toLowerCase().includes(body.body_filter!.toLowerCase()));
  if (body.limit)       bodies = bodies.slice(0, body.limit);

  if (bodies.length === 0) return json({ bodies: 0 });

  const tasks = bodies.map(b => async () => {
    const inp = byBody.get(b)!;
    const parsed = await parseText(b, inp.t, inp.s, inp.n);
    return { body: b, parsed };
  });
  const parsedResults = await pLimit(tasks, CONCURRENCY);

  if (body.dry_run) {
    const nonEmpty = parsedResults.filter(r =>
      r.parsed.dogs_seasonal_closures || r.parsed.dogs_daily_windows
      || r.parsed.dogs_day_of_week_mask !== null || r.parsed.dogs_prohibited_reason);
    return json({
      dry_run:    true,
      bodies:     bodies.length,
      with_rules: nonEmpty.length,
      preview:    nonEmpty.slice(0, 15),
    });
  }

  let bodies_updated = 0;
  const writeErrors: string[] = [];
  for (const r of parsedResults) {
    const { error: uErr } = await supabase
      .from("beaches_staging_new")
      .update({
        dogs_seasonal_closures: r.parsed.dogs_seasonal_closures,
        dogs_daily_windows:     r.parsed.dogs_daily_windows,
        dogs_day_of_week_mask:  r.parsed.dogs_day_of_week_mask,
        dogs_prohibited_reason: r.parsed.dogs_prohibited_reason,
      })
      .eq("governing_body", r.body)
      .eq("review_status", "ready");
    if (uErr) writeErrors.push(`body "${r.body}": ${uErr.message}`);
    else      bodies_updated++;
  }

  return json({
    bodies:         bodies.length,
    bodies_updated,
    summary: {
      with_seasonal:   parsedResults.filter(r => r.parsed.dogs_seasonal_closures).length,
      with_daily:      parsedResults.filter(r => r.parsed.dogs_daily_windows).length,
      with_dow_mask:   parsedResults.filter(r => r.parsed.dogs_day_of_week_mask !== null).length,
      with_reason:     parsedResults.filter(r => r.parsed.dogs_prohibited_reason).length,
    },
    errors: writeErrors.slice(0, 10),
  });
});
