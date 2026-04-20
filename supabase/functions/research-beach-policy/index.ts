// research-beach-policy/index.ts
// Full policy research pipeline for a single beaches_staging record.
//
// Pipeline:
//   1. Serper search → find best beach website (governing body first)
//   2. Fetch that URL → extract text, follow ordinance links
//   3. Serper search → "[name] [governing body] dogs policy rules"
//   4. Claude extracts structured policy from each source independently
//   5. Claude compares → agree = promote to gold, disagree = flag for review
//
// POST { staging_id: number }
// Returns { staging_id, status, website, search, agreement, error? }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ANTHROPIC_API_KEY    = Deno.env.get("anthropic_api_key")!;
const SERPER_API_KEY       = Deno.env.get("SERPER_API_KEY")!;

const ANTHROPIC_URL = "https://api.anthropic.com/v1/messages";
const SERPER_URL    = "https://google.serper.dev/search";
const MODEL         = "claude-sonnet-4-20250514";

// ── Serper search ─────────────────────────────────────────────────────────────

async function serperSearch(query: string, num = 5): Promise<Array<{ title: string; link: string; snippet: string }>> {
  const resp = await fetch(SERPER_URL, {
    method:  "POST",
    headers: { "X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json" },
    body:    JSON.stringify({ q: query, num }),
  });
  if (!resp.ok) throw new Error(`Serper error: ${resp.status}`);
  const data = await resp.json();
  return (data.organic ?? []).map((r: Record<string, string>) => ({
    title:   r.title   ?? "",
    link:    r.link    ?? "",
    snippet: r.snippet ?? "",
  }));
}

// ── URL priority scoring ──────────────────────────────────────────────────────
// Lower score = higher priority

const GOV_DOMAINS   = [".gov", ".us", "parks.ca.gov", "nps.gov", "fs.usda.gov"];
const TRAVEL_DOMAINS = ["visitcalifornia", "tripadvisor", "yelp", "expedia", "travelocity"];

function urlPriority(url: string): number {
  const lower = url.toLowerCase();
  if (GOV_DOMAINS.some(d => lower.includes(d)))    return 1; // official gov
  if (lower.includes("park") || lower.includes("beach")) return 2; // beach/park site
  if (TRAVEL_DOMAINS.some(d => lower.includes(d))) return 4; // travel/review
  return 3; // other
}

function bestUrl(results: Array<{ link: string }>): string | null {
  if (results.length === 0) return null;
  return [...results].sort((a, b) => urlPriority(a.link) - urlPriority(b.link))[0].link;
}

// ── Web scraping ──────────────────────────────────────────────────────────────

async function fetchPageText(url: string): Promise<{ text: string; ok: boolean; note?: string }> {
  try {
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; DogBeachBot/1.0)" },
      signal:  AbortSignal.timeout(10_000),
    });

    const contentType = resp.headers.get("content-type") ?? "";

    if (contentType.includes("pdf")) {
      return { text: "", ok: false, note: "PDF — cannot extract text" };
    }
    if (!resp.ok) {
      return { text: "", ok: false, note: `HTTP ${resp.status}` };
    }

    const html = await resp.text();

    // Strip tags and collapse whitespace
    const text = html
      .replace(/<script[\s\S]*?<\/script>/gi, "")
      .replace(/<style[\s\S]*?<\/style>/gi, "")
      .replace(/<[^>]+>/g, " ")
      .replace(/&nbsp;/gi, " ")
      .replace(/&amp;/gi, "&")
      .replace(/&lt;/gi, "<")
      .replace(/&gt;/gi, ">")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 8000); // cap at 8k chars for LLM context

    if (text.length < 100) {
      return { text, ok: false, note: "Page appears to be JS-rendered — minimal text extracted" };
    }

    return { text, ok: true };
  } catch (e) {
    return { text: "", ok: false, note: `Fetch failed: ${(e as Error).message}` };
  }
}

// ── Claude call ───────────────────────────────────────────────────────────────

async function claudeCall(system: string, user: string): Promise<string> {
  const resp = await fetch(ANTHROPIC_URL, {
    method:  "POST",
    headers: {
      "x-api-key":         ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
      "content-type":      "application/json",
    },
    body: JSON.stringify({
      model:      MODEL,
      max_tokens: 1024,
      system,
      messages: [{ role: "user", content: user }],
    }),
  });
  if (!resp.ok) throw new Error(`Anthropic error: ${resp.status}`);
  const data = await resp.json();
  return data.content?.[0]?.text ?? "";
}

// ── Policy extraction ─────────────────────────────────────────────────────────

const EXTRACTION_SYSTEM = `You extract dog access policy information from text about a beach.
Return ONLY a JSON object with these fields (use null if unknown):
{
  "dogs_allowed": true | false | null,
  "access_rule": "off_leash" | "on_leash" | "mixed" | "prohibited" | null,
  "access_scope": "full_beach" | "designated_area" | "partial" | null,
  "zone_description": string | null,
  "seasonal_start": "MM-DD" | null,
  "seasonal_end": "MM-DD" | null,
  "dogs_prohibited_start": "HH:MM" | null,
  "dogs_prohibited_end": "HH:MM" | null,
  "day_restrictions": string | null,
  "allowed_hours_text": string | null,
  "confidence": "high" | "medium" | "low"
}
If dogs are not mentioned at all, return dogs_allowed: null and confidence: "low".`;

async function extractPolicy(text: string, beachName: string, governingBody: string) {
  const prompt = `Beach: ${beachName}\nGoverning body: ${governingBody}\n\nSource text:\n${text}`;
  const raw = await claudeCall(EXTRACTION_SYSTEM, prompt);
  try {
    const match = raw.match(/\{[\s\S]*\}/);
    return match ? JSON.parse(match[0]) : null;
  } catch {
    return null;
  }
}

// ── Policy comparison ─────────────────────────────────────────────────────────

function policiesAgree(a: Record<string, unknown> | null, b: Record<string, unknown> | null): boolean {
  if (!a || !b) return false;
  if (a.dogs_allowed === null || b.dogs_allowed === null) return false;
  if (a.dogs_allowed !== b.dogs_allowed) return false;
  // If both say dogs are allowed, access_rule must also agree (or one is null)
  if (a.dogs_allowed === true) {
    if (a.access_rule && b.access_rule && a.access_rule !== b.access_rule) return false;
  }
  return true;
}

// ── Handler ───────────────────────────────────────────────────────────────────

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { staging_id?: number };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const { staging_id } = body;
  if (!staging_id) return json({ error: "staging_id required" }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // Fetch the staging record
  const { data: rec, error: recErr } = await supabase
    .from("beaches_staging")
    .select("id, display_name, city, county, state, governing_body, governing_jurisdiction, latitude, longitude")
    .eq("id", staging_id)
    .single();

  if (recErr || !rec) return json({ error: recErr?.message ?? "Record not found" }, 404);

  const name    = rec.display_name;
  const govBody = rec.governing_body || rec.county || rec.city || rec.state || "Unknown";
  const location = [rec.city, rec.county, rec.state].filter(Boolean).join(", ");

  const result: Record<string, unknown> = { staging_id, name };

  // ── Step 1: Find beach website ─────────────────────────────────────────────

  let websiteUrl: string | null = null;
  let websiteText = "";
  let websiteNote: string | undefined;

  try {
    const siteResults = await serperSearch(`"${name}" ${govBody} beach rules dogs site hours`, 8);
    websiteUrl = bestUrl(siteResults);
    result.website_url = websiteUrl;
    result.website_search_results = siteResults.slice(0, 3).map(r => ({ title: r.title, link: r.link }));

    if (websiteUrl) {
      const scraped = await fetchPageText(websiteUrl);
      websiteText = scraped.text;
      websiteNote = scraped.note;
      result.website_scraped_ok = scraped.ok;
      result.website_note       = scraped.note;
    }
  } catch (e) {
    result.website_error = (e as Error).message;
  }

  // ── Step 2: Extract policy from website ───────────────────────────────────

  let websitePolicy = null;
  if (websiteText.length > 100) {
    websitePolicy = await extractPolicy(websiteText, name, govBody);
  }
  result.website_policy = websitePolicy;

  // ── Step 3: Serper search for policy ──────────────────────────────────────

  let searchPolicy = null;
  let searchResults: Array<{ title: string; link: string; snippet: string }> = [];

  try {
    searchResults = await serperSearch(
      `${name} ${location} dogs allowed rules leash policy beach`,
      6,
    );
    result.search_results = searchResults.slice(0, 3).map(r => ({ title: r.title, link: r.link, snippet: r.snippet }));

    const searchText = searchResults.map(r => `${r.title}\n${r.snippet}`).join("\n\n").slice(0, 6000);
    if (searchText.length > 50) {
      searchPolicy = await extractPolicy(searchText, name, govBody);
    }
  } catch (e) {
    result.search_error = (e as Error).message;
  }
  result.search_policy = searchPolicy;

  // ── Step 4: Compare and determine outcome ─────────────────────────────────

  const agree  = policiesAgree(websitePolicy, searchPolicy);
  const merged = agree ? (websitePolicy ?? searchPolicy) : null;
  result.agreement = agree;

  // ── Step 5: Save to Supabase ──────────────────────────────────────────────

  // Save website source to beach_policy_research
  if (websiteUrl) {
    await supabase.from("beach_policy_research").insert({
      staging_id,
      source_url:  websiteUrl,
      source_type: urlPriority(websiteUrl) === 1 ? "official_gov" : "website",
      raw_text:    websiteText.slice(0, 5000),
      notes:       websiteNote ?? null,
    });
  }

  // Save search snippets to beach_policy_research
  if (searchResults.length > 0) {
    const snippet = searchResults.map(r => `[${r.title}]\n${r.snippet}`).join("\n\n");
    await supabase.from("beach_policy_research").insert({
      staging_id,
      source_url:  searchResults[0]?.link ?? null,
      source_type: "web_search",
      raw_text:    snippet.slice(0, 5000),
      notes:       `Serper search: ${name} dogs policy`,
    });
  }

  // Update staging record
  if (agree && merged) {
    await supabase.from("beaches_staging").update({
      dogs_allowed:           merged.dogs_allowed,
      access_rule:            merged.access_rule,
      access_scope:           merged.access_scope,
      zone_description:       merged.zone_description,
      seasonal_start:         merged.seasonal_start,
      seasonal_end:           merged.seasonal_end,
      dogs_prohibited_start:  merged.dogs_prohibited_start,
      dogs_prohibited_end:    merged.dogs_prohibited_end,
      day_restrictions:       merged.day_restrictions,
      allowed_hours_text:     merged.allowed_hours_text,
      policy_source_url:      websiteUrl,
      policy_confidence:      merged.confidence ?? "medium",
      policy_verified_date:   new Date().toISOString().slice(0, 10),
      quality_tier:           "gold",
      review_status:          "OK",
    }).eq("id", staging_id);
    result.status = "gold";
  } else {
    const needsWebsite = !websiteText || !websitePolicy;
    await supabase.from("beaches_staging").update({
      review_status: "Needs Review",
      review_notes:  agree ? null
        : needsWebsite ? "Website not found or unscrapable — manual review needed"
        : "Sources disagree on dog policy — manual review needed",
    }).eq("id", staging_id);
    result.status = "needs_review";
  }

  return json(result);
});
