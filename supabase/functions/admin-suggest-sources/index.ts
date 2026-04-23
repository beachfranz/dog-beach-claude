// admin-suggest-sources/index.ts
// Returns the top 3 candidate source URLs for a beach's dog policy.
// Uses Tavily search (same as v2-*-dog-policy) — just capped at 3 results.
//
// Security model: same as other admin-* functions (obscure URL,
// service-role server-side, no auth layer). See admin-update-beach.
//
// POST { location_id: string }
// Returns { suggestions: [{ url, title, snippet, domain, score }] }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }  from "../_shared/cors.ts";
import { requireAdmin } from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const TAVILY_API_KEY       = Deno.env.get("TAVILY_API_KEY")!;

interface TavilyResult {
  url: string;
  title: string;
  content: string;
  score: number;
}

async function tavilySearch(query: string): Promise<TavilyResult[]> {
  try {
    const resp = await fetch("https://api.tavily.com/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        api_key:      TAVILY_API_KEY,
        query,
        search_depth: "basic",
        max_results:  8,  // grab a few extra, trim to 3 after dedup
      }),
    });
    if (!resp.ok) return [];
    const data = await resp.json();
    return (data?.results ?? []) as TavilyResult[];
  } catch {
    return [];
  }
}

function domainOf(url: string): string {
  try { return new URL(url).hostname.replace(/^www\./, ""); } catch { return ""; }
}

// Matches the SQL _classify_source_url() logic. Kept in sync by hand —
// if the SQL taxonomy changes, update both.
function classifyUrl(url: string | null): string | null {
  if (!url) return null;
  if (/:\/\/[^/]*\.(gov|mil)(\/|:|$)/i.test(url))       return "official";
  if (/:\/\/[^/]*\.gov\.[a-z]{2,3}(\/|:|$)/i.test(url)) return "official";
  if (/:\/\/[^/]*\.[a-z]{2}\.us(\/|:|$)/i.test(url))    return "official";
  if (/:\/\/[^/]*(yelp|tripadvisor|facebook|reddit|wikipedia|wikimedia|instagram|twitter|x\.com|tiktok|pinterest|alltrails|wikiloc|quora|medium|bringfido|rover|huskymutty)\./i.test(url))
    return "community";
  if (/:\/\/[^/]*\.(org|edu)(\/|:|$)/i.test(url))       return "nonprofit";
  return "commercial";
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: { location_id?: string };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const { location_id } = body;
  if (!location_id) return json({ error: "location_id required" }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data: beach, error } = await supabase
    .from("beaches")
    .select("display_name, governing_body, governing_jurisdiction, address")
    .eq("location_id", location_id)
    .single();

  if (error || !beach) return json({ error: "Beach not found" }, 404);

  // Build a search query the same shape the enrichment pipeline uses —
  // beach name plus the governing body steers toward official sources.
  const parts: string[] = [`"${beach.display_name}"`, "dog policy leash rules"];
  if (beach.governing_body) parts.push(beach.governing_body);
  if (beach.address)        parts.push(beach.address.split(",").slice(-2, -1).join("").trim());
  const query = parts.filter(Boolean).join(" ");

  const results = await tavilySearch(query);

  // Dedup by domain, keep highest-scoring result per domain
  const byDomain = new Map<string, TavilyResult>();
  for (const r of results) {
    const d = domainOf(r.url);
    if (!d) continue;
    const existing = byDomain.get(d);
    if (!existing || r.score > existing.score) byDomain.set(d, r);
  }

  const suggestions = [...byDomain.values()]
    .sort((a, b) => b.score - a.score)
    .slice(0, 3)
    .map(r => ({
      url:         r.url,
      title:       r.title,
      snippet:     (r.content || "").slice(0, 200),
      domain:      domainOf(r.url),
      score:       Number(r.score.toFixed(2)),
      source_type: classifyUrl(r.url),
    }));

  return json({ query, suggestions });
});
