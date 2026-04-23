// admin-re-extract-policy/index.ts
// Fetches a user-supplied URL, passes its text content through the same
// LLM prompt shape that v2-enrich-operational uses, and returns the
// parsed fields WITHOUT writing to the DB. The admin UI shows the
// result in a compare table where the user picks what to keep.
//
// Security model: same as other admin-* functions (obscure URL,
// service-role server-side, no auth layer). See admin-update-beach.
//
// POST { location_id, source_url }                       — existing beach
//  or  { display_name, source_url }                      — new beach (create-mode)
// Returns { extracted: {...}, source_url, fetched_chars }
// On fetch failure: { error, source_url }
//
// The LLM prompt uses beach_name as context — for existing beaches we look
// it up via location_id; for create-mode callers pass display_name directly.

import Anthropic from "https://esm.sh/@anthropic-ai/sdk@0.30.1";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }  from "../_shared/cors.ts";
import { requireAdmin } from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ANTHROPIC_API_KEY    = Deno.env.get("anthropic_api_key")!;  // lowercase — matches existing functions
const MODEL                = "claude-haiku-4-5-20251001";

const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

// Strip HTML tags + scripts/styles. Good enough for gov + municipal sites.
function htmlToText(html: string): string {
  return html
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, " ")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, " ")
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, " ")
    .trim();
}

async function fetchSourceText(url: string): Promise<string> {
  const resp = await fetch(url, {
    headers: {
      "User-Agent":      "Mozilla/5.0 DogBeachAdmin/1.0",
      "Accept":          "text/html,application/xhtml+xml,*/*;q=0.9",
      "Accept-Language": "en-US,en;q=0.9",
    },
    redirect: "follow",
  });
  if (!resp.ok) throw new Error(`HTTP ${resp.status} from ${url}`);
  const ct = resp.headers.get("content-type") || "";
  const raw = await resp.text();
  if (/text\/html|application\/xhtml/i.test(ct)) return htmlToText(raw);
  // Plain-text / JSON / markdown — pass through minimally cleaned
  return raw.replace(/\s+/g, " ").trim();
}

async function extractPolicy(beachName: string, sourceUrl: string, sourceText: string) {
  // Cap source text to keep token cost + LLM attention manageable.
  const MAX_CHARS = 12_000;
  const clipped = sourceText.length > MAX_CHARS ? sourceText.slice(0, MAX_CHARS) + "\n[… truncated …]" : sourceText;

  const prompt = `You are extracting beach metadata from a single authoritative source page.

Beach: ${beachName}
Source URL: ${sourceUrl}

Source text (cleaned from HTML):
---
${clipped}
---

Extract what this specific source says about the beach. Use null for anything the source doesn't explicitly address — do NOT guess from general knowledge.

Respond with a single FLAT JSON object (no nested sections), exactly these keys:

{
  "dogs_allowed": "yes" | "no" | "mixed" | "seasonal" | "unknown",
  "dogs_leash_required": "yes" | "no" | "mixed" | null,  (mixed = rules vary by area or time)
  "dogs_allowed_areas": string or null,
  "dogs_prohibited_areas": string or null,
  "dogs_off_leash_area": string or null (name of off-leash zone, if any),
  "dogs_time_restrictions": string or null (e.g., "before 9am and after 5pm"),
  "dogs_season_restrictions": string or null (e.g., "snowy plover March-Sept"),
  "dogs_policy_notes": "one or two sentences summarizing for a user",
  "has_parking": true | false | null,
  "parking_type": "lot" | "street" | "paid" | "free" | "mixed" | null,
  "parking_notes": string or null,
  "hours_text": string or null (e.g., "sunrise to 10pm" or "24/7"),
  "hours_notes": string or null,
  "has_restrooms": true | false | null,
  "has_showers": true | false | null,
  "has_lifeguards": true | false | null,
  "has_drinking_water": true | false | null,
  "has_disabled_access": true | false | null,
  "has_food": true | false | null,
  "has_fire_pits": true | false | null,
  "has_picnic_area": true | false | null,
  "confidence": "high" | "low"
}

confidence "high" if the source directly states the rule; "low" if inferring or unclear.
Respond with JSON only, no other text.`;

  const response = await anthropic.messages.create({
    model: MODEL, max_tokens: 800,
    messages: [{ role: "user", content: prompt }],
  });
  const text  = (response.content[0] as { type: string; text: string }).text.trim();
  const match = text.match(/\{[\s\S]*\}/);
  if (!match) throw new Error("LLM response not parseable");
  const parsed = JSON.parse(match[0]);

  // Validators matching v2-enrich-operational's shape
  const allowEnum  = ["yes", "no", "mixed", "seasonal", "unknown"];
  const leashEnum  = ["yes", "no", "mixed"];
  const parkEnum   = ["lot", "street", "paid", "free", "mixed"];
  const confEnum   = ["high", "low"];
  const boolOrNull = (v: unknown) => typeof v === "boolean" ? v : null;
  const strOrNull  = (v: unknown) => v == null || v === "" ? null : String(v);
  const enumOrNull = (v: unknown, e: string[]) => e.includes(v as string) ? v : null;

  return {
    dogs_allowed:             allowEnum.includes(parsed.dogs_allowed) ? parsed.dogs_allowed : "unknown",
    dogs_leash_required:      enumOrNull(parsed.dogs_leash_required, leashEnum),
    dogs_allowed_areas:       strOrNull(parsed.dogs_allowed_areas),
    dogs_prohibited_areas:    strOrNull(parsed.dogs_prohibited_areas),
    dogs_off_leash_area:      strOrNull(parsed.dogs_off_leash_area),
    dogs_time_restrictions:   strOrNull(parsed.dogs_time_restrictions),
    dogs_season_restrictions: strOrNull(parsed.dogs_season_restrictions),
    dogs_policy_notes:        strOrNull(parsed.dogs_policy_notes) ?? "",
    has_parking:              boolOrNull(parsed.has_parking),
    parking_type:             enumOrNull(parsed.parking_type, parkEnum),
    parking_notes:            strOrNull(parsed.parking_notes),
    hours_text:               strOrNull(parsed.hours_text),
    hours_notes:              strOrNull(parsed.hours_notes),
    has_restrooms:            boolOrNull(parsed.has_restrooms),
    has_showers:              boolOrNull(parsed.has_showers),
    has_lifeguards:           boolOrNull(parsed.has_lifeguards),
    has_drinking_water:       boolOrNull(parsed.has_drinking_water),
    has_disabled_access:      boolOrNull(parsed.has_disabled_access),
    has_food:                 boolOrNull(parsed.has_food),
    has_fire_pits:            boolOrNull(parsed.has_fire_pits),
    has_picnic_area:          boolOrNull(parsed.has_picnic_area),
    confidence:               confEnum.includes(parsed.confidence) ? parsed.confidence : "low",
  };
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: { location_id?: string; display_name?: string; source_url?: string };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const { location_id, source_url } = body;
  if (!source_url) return json({ error: "source_url required" }, 400);
  try { new URL(source_url); } catch { return json({ error: "Invalid URL" }, 400); }
  if (!location_id && !body.display_name) {
    return json({ error: "location_id or display_name required" }, 400);
  }

  // Resolve the name used for LLM context. Existing beach → lookup by id.
  // Create-mode caller → trust the passed display_name.
  let beachName: string;
  if (location_id) {
    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
    const { data: beach, error } = await supabase
      .from("beaches")
      .select("display_name")
      .eq("location_id", location_id)
      .single();
    if (error || !beach) return json({ error: "Beach not found" }, 404);
    beachName = beach.display_name;
  } else {
    beachName = body.display_name!;
  }

  let sourceText: string;
  try {
    sourceText = await fetchSourceText(source_url);
  } catch (err) {
    return json({ error: `Couldn't fetch URL: ${(err as Error).message}`, source_url }, 502);
  }
  if (!sourceText || sourceText.length < 80) {
    return json({ error: "Source fetched but returned almost no text (JS-rendered page or blocked?)", source_url }, 502);
  }

  let extracted;
  try {
    extracted = await extractPolicy(beachName, source_url, sourceText);
  } catch (err) {
    return json({ error: `LLM extraction failed: ${(err as Error).message}`, source_url }, 500);
  }

  return json({ extracted, source_url, fetched_chars: sourceText.length });
});
