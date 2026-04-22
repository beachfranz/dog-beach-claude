// clean-beaches-staging/index.ts
// Pipeline stage 4: cleans bronze-tier records and promotes them to silver.
//
// Cleaning operations:
//   - Mojibake fix (UTF-8 bytes stored/read as Latin-1, e.g. â€™ → ')
//   - Whitespace normalization (collapse runs, trim)
//   - Governing jurisdiction correction (name keywords vs recorded jurisdiction)
//
// POST { state?: string, dry_run?: boolean }
// Returns { processed, promoted, unchanged, changes, errors }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// ── Fields to apply text cleaning to ─────────────────────────────────────────

const TEXT_FIELDS = [
  "display_name",
  "raw_address",
  "route",
  "city",
  "county",
  "state",
  "governing_body",
  "zone_description",
  "policy_notes",
  "review_notes",
] as const;

// ── Jurisdiction keyword tables (mirrors geocode script) ──────────────────────

const FEDERAL_KEYWORDS = [
  "NATIONAL SEASHORE", "NATIONAL PARK", "NATIONAL RECREATION",
  "NATIONAL MONUMENT", "NATIONAL WILDLIFE", "ARMY CORPS",
];
const STATE_KEYWORDS = [
  "STATE BEACH", "STATE PARK", "STATE RECREATION", "STATE RESERVE",
  "STATE MARINE", "STATE HISTORIC",
];
const COUNTY_KEYWORDS = [
  "COUNTY PARK", "COUNTY BEACH", "REGIONAL PARK", "REGIONAL BEACH",
];

// ── Text cleaners ─────────────────────────────────────────────────────────────

function fixMojibake(s: string): string {
  // UTF-8 bytes misread as Latin-1 (e.g. â€™ → '). Fix by re-encoding as
  // Latin-1 bytes and decoding as UTF-8. If any char is already > U+00FF
  // the string is proper Unicode — return as-is.
  try {
    const bytes = new Uint8Array(s.length);
    for (let i = 0; i < s.length; i++) {
      const code = s.charCodeAt(i);
      if (code > 255) return s;
      bytes[i] = code;
    }
    return new TextDecoder("utf-8").decode(bytes);
  } catch {
    return s;
  }
}

function normalizeWhitespace(s: string): string {
  return s.replace(/\s+/g, " ").trim();
}

function cleanText(s: string | null): string | null {
  if (s == null) return null;
  return normalizeWhitespace(fixMojibake(s));
}

// ── Jurisdiction inference ────────────────────────────────────────────────────

function inferJurisdiction(name: string): string | null {
  const upper = name.toUpperCase();
  for (const kw of FEDERAL_KEYWORDS) if (upper.includes(kw)) return "federal";
  for (const kw of STATE_KEYWORDS)   if (upper.includes(kw)) return "state";
  for (const kw of COUNTY_KEYWORDS)  if (upper.includes(kw)) return "county";
  return null;
}

// ── Per-record cleaning ───────────────────────────────────────────────────────

type Change = { field: string; before: string | null; after: string | null };

function cleanRecord(row: Record<string, unknown>): {
  updates: Record<string, unknown>;
  changes: Change[];
} {
  const updates: Record<string, unknown> = {};
  const changes: Change[] = [];

  // Text field cleaning
  for (const field of TEXT_FIELDS) {
    const before = row[field] as string | null;
    const after  = cleanText(before);
    if (after !== before) {
      updates[field] = after;
      changes.push({ field, before, after });
    }
  }

  // Governing jurisdiction mismatch
  const name    = (updates["display_name"] ?? row["display_name"] ?? "") as string;
  const inferred = inferJurisdiction(name);
  const current  = row["governing_jurisdiction"] as string | null;

  if (inferred && inferred !== current) {
    updates["governing_jurisdiction"] = inferred;
    changes.push({ field: "governing_jurisdiction", before: current, after: inferred });

    const existingNotes = (row["review_notes"] as string | null) ?? "";
    const note = `jurisdiction corrected: name implies ${inferred}, was ${current ?? "null"}`;
    if (!existingNotes.includes("jurisdiction corrected")) {
      const newNotes = existingNotes ? `${existingNotes}; ${note}` : note;
      updates["review_notes"]  = newNotes;
      updates["review_status"] = "Needs Review";
      changes.push({ field: "review_notes", before: existingNotes || null, after: newNotes });
    }
  }

  return { updates, changes };
}

// ── Handler ───────────────────────────────────────────────────────────────────

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { state?: string; dry_run?: boolean } = {};
  try { body = await req.json(); } catch { /* empty body is fine */ }

  const { state, dry_run = false } = body;

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // Fetch all bronze records for the given state (or all states)
  let query = supabase
    .from("beaches_staging")
    .select("*")
    .eq("quality_tier", "bronze")
    .limit(10000);

  if (state) query = query.eq("state", state);

  const { data: records, error: fetchError } = await query;
  if (fetchError) return json({ error: fetchError.message }, 500);

  const results = {
    processed: records?.length ?? 0,
    promoted:  0,
    unchanged: 0,
    changes:   [] as { id: number; display_name: string; changes: Change[] }[],
    errors:    [] as string[],
  };

  const rowsToUpsert: Record<string, unknown>[] = [];

  for (const row of records ?? []) {
    const { updates, changes } = cleanRecord(row as Record<string, unknown>);

    if (changes.length > 0) {
      results.changes.push({ id: row.id, display_name: row.display_name, changes });
    } else {
      results.unchanged++;
    }

    // Merge updates into the full row and promote to silver
    rowsToUpsert.push({ ...row, ...updates, quality_tier: "silver" });
  }

  if (!dry_run && rowsToUpsert.length > 0) {
    const { error } = await supabase
      .from("beaches_staging")
      .upsert(rowsToUpsert, { onConflict: "id" });

    if (error) {
      results.errors.push(error.message);
    } else {
      results.promoted = rowsToUpsert.length;
    }
  } else {
    results.promoted = rowsToUpsert.length;
  }

  return json(results);
});
