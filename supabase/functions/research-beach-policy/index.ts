// research-beach-policy/index.ts
// Staged policy research pipeline for a single beaches_staging record.
//
// Improvements over v1:
//   - Staged queries: official-domain-first → broad → snippet
//   - State-aware official domain seeding (site: operators)
//   - Negative query terms to suppress hotels/restaurants/review sites
//   - Domain class scoring: official > parks > tourism > aggregator > blog
//   - Beach name gate: skip pages that don't mention the beach
//   - Larger fetch limit (15k chars) + strips nav/header/footer
//   - dog_friendly as a distinct tier above "allowed"
//
// POST { staging_id: number }
// Returns { staging_id, status, website_url, website_policy, search_policy, agreement, error? }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ANTHROPIC_API_KEY    = Deno.env.get("anthropic_api_key")!;
const SERPER_API_KEY       = Deno.env.get("SERPER_API_KEY")!;

const ANTHROPIC_URL    = "https://api.anthropic.com/v1/messages";
const SERPER_URL       = "https://google.serper.dev/search";
const MODEL            = "claude-sonnet-4-20250514";
const FETCH_CHAR_LIMIT = 15_000;
const NEGATIVE_TERMS   = "-hotel -restaurant -apartment -yelp -wedding -resort -rv -airbnb -tripadvisor";

// ── State-aware official domains ──────────────────────────────────────────────

const STATE_OFFICIAL_DOMAINS: Record<string, string[]> = {
  AL: ["alapark.com", "dcnr.alabama.gov"],
  AK: ["dnr.alaska.gov", "parks.alaska.gov"],
  AZ: ["azstateparks.com"],
  AR: ["arkansasstateparks.com"],
  CA: ["parks.ca.gov", "ca.gov"],
  CO: ["cpw.state.co.us"],
  CT: ["portal.ct.gov"],
  DE: ["destateparks.com"],
  FL: ["floridastateparks.org", "dep.state.fl.us", "myfwc.com"],
  GA: ["gastateparks.org"],
  HI: ["dlnr.hawaii.gov"],
  ID: ["parksandrecreation.idaho.gov"],
  IL: ["dnr.illinois.gov"],
  IN: ["dnr.in.gov"],
  LA: ["lastateparks.com"],
  MA: ["mass.gov"],
  MD: ["dnr.maryland.gov"],
  ME: ["maine.gov"],
  MI: ["michigan.gov"],
  MN: ["dnr.state.mn.us"],
  MS: ["mdwfp.com"],
  NC: ["ncparks.gov"],
  NJ: ["dep.nj.gov", "njparksandforests.org"],
  NY: ["parks.ny.gov"],
  OR: ["stateparks.oregon.gov", "oregon.gov"],
  RI: ["dem.ri.gov"],
  SC: ["southcarolinaparks.com"],
  TX: ["tpwd.texas.gov"],
  VA: ["dcr.virginia.gov", "virginiastateparks.gov"],
  WA: ["parks.wa.gov"],
  WI: ["dnr.wisconsin.gov"],
};

const FEDERAL_DOMAINS = ["nps.gov", "fws.gov", "blm.gov", "fs.usda.gov", "recreation.gov"];

// ── Domain classification ─────────────────────────────────────────────────────

type DomainClass = "official" | "parks_authority" | "tourism" | "local_media" | "aggregator" | "blog" | "other";

const DOMAIN_WEIGHTS: Record<DomainClass, number> = {
  official:        1.00,
  parks_authority: 0.95,
  tourism:         0.75,
  local_media:     0.60,
  aggregator:      0.45,
  blog:            0.30,
  other:           0.20,
};

function classifyDomain(url: string): DomainClass {
  try {
    const host = new URL(url).hostname.toLowerCase();
    if (host.endsWith(".gov") || host.endsWith(".us") || host.endsWith(".mil")) return "official";
    if (host.includes("parks") || host.includes("stateparks") || host.includes("recreation")) return "parks_authority";
    if (host.includes("visit") || host.includes("tourism") || host.includes("chamber")) return "tourism";
    if (host.includes("times") || host.includes("tribune") || host.includes("patch") || host.includes("news")) return "local_media";
    if (host.includes("bringfido") || host.includes("tripadvisor") || host.includes("yelp") || host.includes("expedia")) return "aggregator";
    if (host.includes("blog") || host.includes("wordpress")) return "blog";
  } catch { /* bad URL */ }
  return "other";
}

function domainScore(url: string): number {
  return DOMAIN_WEIGHTS[classifyDomain(url)];
}

// ── Official site: operators for a state ─────────────────────────────────────

function officialSiteOps(state: string): string {
  const domains = [...FEDERAL_DOMAINS, ...(STATE_OFFICIAL_DOMAINS[state] ?? [])];
  return domains.map(d => `site:${d}`).join(" OR ");
}

// ── Beach name gate ───────────────────────────────────────────────────────────
// Returns true if enough significant words from the beach name appear in text.

const GENERIC_WORDS = new Set(["beach", "state", "park", "county", "national", "ocean", "lake", "river"]);

function passesNameGate(name: string, text: string): boolean {
  const words = name.toLowerCase().split(/\s+/).filter(w => w.length > 3 && !GENERIC_WORDS.has(w));
  if (words.length === 0) return true;
  const lower = text.toLowerCase();
  const matched = words.filter(w => lower.includes(w)).length;
  return matched / words.length >= 0.5;
}

// ── Serper search ─────────────────────────────────────────────────────────────

type SearchResult = { title: string; link: string; snippet: string };

async function serperSearch(query: string, num = 8): Promise<SearchResult[]> {
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

function rankByDomain(results: SearchResult[]): SearchResult[] {
  return [...results].sort((a, b) => domainScore(b.link) - domainScore(a.link));
}

// ── Web scraping ──────────────────────────────────────────────────────────────

async function fetchPageText(url: string): Promise<{ text: string; ok: boolean; note?: string }> {
  try {
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)" },
      signal:  AbortSignal.timeout(12_000),
    });
    const contentType = resp.headers.get("content-type") ?? "";
    if (contentType.includes("pdf")) return { text: "", ok: false, note: "PDF" };
    if (!resp.ok) return { text: "", ok: false, note: `HTTP ${resp.status}` };

    const html = await resp.text();
    const text = html
      .replace(/<script[\s\S]*?<\/script>/gi, "")
      .replace(/<style[\s\S]*?<\/style>/gi, "")
      .replace(/<nav[\s\S]*?<\/nav>/gi, "")
      .replace(/<header[\s\S]*?<\/header>/gi, "")
      .replace(/<footer[\s\S]*?<\/footer>/gi, "")
      .replace(/<[^>]+>/g, " ")
      .replace(/&nbsp;/gi, " ").replace(/&amp;/gi, "&").replace(/&lt;/gi, "<").replace(/&gt;/gi, ">")
      .replace(/\s+/g, " ").trim()
      .slice(0, FETCH_CHAR_LIMIT);

    if (text.length < 100) return { text, ok: false, note: "JS-rendered or empty" };
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
  "dog_friendly": true | null,
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
Rules:
- dog_friendly = true ONLY when the source explicitly markets the beach as dog-friendly, a dog beach, or dogs welcome — not just "dogs allowed"
- If dogs are not mentioned at all, return dogs_allowed: null and confidence: "low"`;

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
  if (a.dogs_allowed === true) {
    if (a.access_rule && b.access_rule && a.access_rule !== b.access_rule) return false;
  }
  return true;
}

// ── Find best scrapable page from a result list ───────────────────────────────
// Tries top-ranked URLs until one passes the name gate; returns null if none do.

async function findBestPage(
  name: string,
  results: SearchResult[],
  maxAttempts = 3,
): Promise<{ url: string; text: string; note?: string } | null> {
  const ranked = rankByDomain(results);
  for (const r of ranked.slice(0, maxAttempts)) {
    const scraped = await fetchPageText(r.link);
    if (scraped.ok && passesNameGate(name, scraped.text)) {
      return { url: r.link, text: scraped.text, note: scraped.note };
    }
  }
  return null;
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

  const { data: rec, error: recErr } = await supabase
    .from("beaches_staging")
    .select("id, display_name, city, county, state, governing_body, governing_jurisdiction, latitude, longitude")
    .eq("id", staging_id)
    .single();

  if (recErr || !rec) return json({ error: recErr?.message ?? "Record not found" }, 404);

  const name    = rec.display_name;
  const state   = rec.state ?? "";
  const city    = rec.city  ?? "";
  const county  = rec.county ?? "";
  const govBody = rec.governing_body || county || city || state || "Unknown";
  const location = [city, county, state].filter(Boolean).join(", ");

  const result: Record<string, unknown> = { staging_id, name };

  // ── Step 1: Official-first search → find best scrapable page ─────────────────

  let websiteUrl: string | null = null;
  let websiteText = "";
  let websiteNote: string | undefined;

  try {
    const siteOps      = officialSiteOps(state);
    const officialQ    = `"${name}" dogs (${siteOps})`;
    const officialHits = await serperSearch(officialQ, 8);

    let best = await findBestPage(name, officialHits);

    // Fallback: broad search with negative terms
    if (!best) {
      const broadQ    = `"${name}" ${location} dogs ${NEGATIVE_TERMS}`;
      const broadHits = await serperSearch(broadQ, 8);
      best = await findBestPage(name, broadHits);
      result.website_search_results = broadHits.slice(0, 3).map(r => ({ title: r.title, link: r.link }));
    } else {
      result.website_search_results = officialHits.slice(0, 3).map(r => ({ title: r.title, link: r.link }));
    }

    if (best) {
      websiteUrl  = best.url;
      websiteText = best.text;
      websiteNote = best.note;
    }

    result.website_url        = websiteUrl;
    result.website_scraped_ok = websiteText.length > 100;
    result.website_note       = websiteNote;
    result.website_domain_class = websiteUrl ? classifyDomain(websiteUrl) : null;
  } catch (e) {
    result.website_error = (e as Error).message;
  }

  // ── Step 2: Extract policy from page ─────────────────────────────────────────

  let websitePolicy = null;
  if (websiteText.length > 100) {
    websitePolicy = await extractPolicy(websiteText, name, govBody);
  }
  result.website_policy = websitePolicy;

  // ── Step 3: Search-snippet policy ────────────────────────────────────────────

  let searchPolicy = null;
  let searchResults: SearchResult[] = [];

  try {
    const snippetQ = `"${name}" ${city} ${state} dogs allowed leash hours season ${NEGATIVE_TERMS}`;
    searchResults  = await serperSearch(snippetQ, 8);
    result.search_results = searchResults.slice(0, 3).map(r => ({ title: r.title, link: r.link, snippet: r.snippet }));

    const searchText = searchResults.map(r => `${r.title}\n${r.snippet}`).join("\n\n").slice(0, 6000);
    if (searchText.length > 50) {
      searchPolicy = await extractPolicy(searchText, name, govBody);
    }
  } catch (e) {
    result.search_error = (e as Error).message;
  }
  result.search_policy = searchPolicy;

  // ── Step 4: Compare ───────────────────────────────────────────────────────────

  const agree  = policiesAgree(websitePolicy, searchPolicy);
  const merged = agree ? (websitePolicy ?? searchPolicy) : null;
  result.agreement = agree;

  // ── Step 5: Save to Supabase ──────────────────────────────────────────────────

  if (websiteUrl) {
    const dc = classifyDomain(websiteUrl);
    await supabase.from("beach_policy_research").insert({
      staging_id,
      source_url:  websiteUrl,
      source_type: (dc === "official" || dc === "parks_authority") ? "official_gov" : "website",
      raw_text:    websiteText.slice(0, 5000),
      notes:       websiteNote ?? null,
    });
  }

  if (searchResults.length > 0) {
    const snippet = searchResults.map(r => `[${r.title}]\n${r.snippet}`).join("\n\n");
    await supabase.from("beach_policy_research").insert({
      staging_id,
      source_url:  searchResults[0]?.link ?? null,
      source_type: "web_search",
      raw_text:    snippet.slice(0, 5000),
      notes:       `Serper: ${name} dogs policy`,
    });
  }

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
