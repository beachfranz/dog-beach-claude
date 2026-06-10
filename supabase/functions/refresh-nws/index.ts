// refresh-nws/index.ts
//
// Unified NWS fetcher (renamed from refresh-nws-srf 2026-06-10). Runs hourly.
// Two phases, both writing to public.beach_advisory (different `source`):
//
//   Phase 1 (SRF):  per-WFO Surf Zone Forecast text products → wfo_srf_forecast.
//                   Downstream refresh_rip_current_advisories() emits the
//                   beach_advisory source='rip_current' rows.
//
//   Phase 2 (alerts): per-state api.weather.gov/alerts/active fetch, PIP
//                     against beach polygons via nws_zones, UPSERT
//                     beach_advisory source='nws' rows.
//
//   Phase 3 (sweep):  DELETE source='nws' rows whose fetched_at < this
//                     run's started_at. Watermark-style cleanup so stale
//                     alerts (zone risk dropped, expiration passed) fall
//                     out of the consumer surface.
//
// On first encounter with a zone (SRF or alert), the function lazily warms
// public.nws_zones with the zone's GeoJSON polygon via the NWS API.
//
// Source URL pattern:
//   https://forecast.weather.gov/product.php?site=NWS&issuedby=<WFO>&product=SRF&format=txt
//
// SRF text format (verified for LOX + MHX; expected to hold across coastal
// WFOs — they share the AWIPS SRF template):
//
//   <ZONE_CODES>-<DDHHmm>-
//   <Zone Name>-
//   <Issuance datestamp>
//   ...optional headline alerts...
//   .THIS AFTERNOON THROUGH THURSDAY...
//   Rip Current Risk*.............High.
//   Surf Height...................4 to 7 feet, ...
//   ...
//   .FRIDAY...
//   Rip Current Risk*.............Moderate.
//   ...
//   &&
//   Rip Current Risks: ...
//   $$
//
// Sub-divided zones (e.g. MHX Cape Hatteras):
//   Rip Current Risk*...
//      North of Cape Hatteras...Moderate.
//      South of Cape Hatteras...Low.
//   → take WORST (safety-favoring default).

// deno-lint-ignore-file no-explicit-any
import { createClient, SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ADMIN_SECRET         = Deno.env.get("ADMIN_SECRET") ?? "";

const USER_AGENT = "DogBeachScout/1.0 (https://dogbeachscout.com; franz@franzfunk.com) nws-fetcher";

// Coastal WFOs. Stable list; SRFs only issue from coastal WFOs so this is
// our universe. Inland WFOs return 404 on the SRF URL.
const COASTAL_WFOS = [
  // East coast (north → south)
  "CAR", "GYX", "BOX", "OKX", "PHI", "LWX", "AKQ",
  "MHX", "ILM", "CHS", "JAX", "MLB", "MFL", "KEY", "TBW",
  // Gulf coast
  "TAE", "MOB", "LIX", "LCH", "HGX", "CRP", "BRO",
  // West coast (south → north)
  "SGX", "LOX", "MTR", "EKA", "MFR", "PQR", "SEW",
  // Hawaii + Alaska
  "HFO", "AJK", "AFC", "AFG",
];

// Phase-2 alert scope. Mirror of scripts/load_nws_coastal_zones.py's
// COASTAL_STATES — coastal + Great Lakes (Great Lakes states have inland
// beaches that need AQ / heat alerts even if no rip risk).
const ALERT_STATES = [
  "CA","OR","WA","HI","AK",
  "MA","RI","CT","NY","NJ","DE","MD","VA","NC","SC",
  "GA","FL","AL","MS","LA","TX","ME","NH",
  "MI","WI","MN","IL","IN","OH","PA",
];

const SRF_URL = (wfo: string) =>
  `https://forecast.weather.gov/product.php?site=NWS&issuedby=${wfo}&product=SRF&format=txt`;

const ZONE_GEOJSON_URL = (zone: string) =>
  `https://api.weather.gov/zones/forecast/${zone}`;

const ALERTS_URL = (state: string) =>
  `https://api.weather.gov/alerts/active?area=${state}`;

// Parse zone_id + type from a zones URL like
// https://api.weather.gov/zones/forecast/PZZ565 → ('PZZ565','forecast').
function zoneIdAndTypeFromUrl(zoneUrl: string): [string | null, string | null] {
  const m = zoneUrl.match(/\/zones\/([a-z]+)\/([A-Z0-9_]+)(?:\?|$|\/)/);
  if (!m) return [null, null];
  return [m[2], m[1]];
}

const FETCH_TIMEOUT_MS = 20_000;
const CONCURRENT_FETCHES = 6;

// ── Risk parsing ────────────────────────────────────────────────────────
const RISK_RANK: Record<string, number> = { Low: 1, Moderate: 2, High: 3 };
function worstRisk(...risks: (string | null | undefined)[]): string | null {
  let worst: string | null = null;
  for (const r of risks) {
    if (!r || !(r in RISK_RANK)) continue;
    if (!worst || RISK_RANK[r] > RISK_RANK[worst]) worst = r;
  }
  return worst;
}

interface ParsedZoneForecast {
  zone_ids: string[];           // a header may list multiple zones (CAZ362-364-)
  current_risk: string | null;
  next_risk: string | null;
  conditions_current: string;   // raw period text for archival
  conditions_next: string;
  raw: string;                  // raw zone block
}

// ── Strip the HTML wrapper that forecast.weather.gov returns even with
// format=txt. The actual product text lives in a <pre> block. Normalize
// line endings to LF so regex anchors behave predictably.
function extractPreContent(html: string): string {
  const preMatch = html.match(/<pre[^>]*>([\s\S]*?)<\/pre>/i);
  const raw = preMatch
    ? preMatch[1]
        .replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
        .replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    : html;
  // Normalize CRLF → LF. SRFs are served with CRLF and JS regex /m anchors
  // can behave oddly with \r interleaving — strip them outright.
  return raw.replace(/\r/g, "");
}

// ── Split an SRF text into per-zone blocks. Each zone block in an SRF is
// terminated by a line containing only `$$`. The block before the first
// `$$` includes the SRF preamble (WMO header + product header), which we
// keep as-is — the preamble has no zone-code header line so it filters
// out below.
function splitZoneBlocks(text: string): string[] {
  return text.split(/^\$\$\s*$/m)
    .map(b => b.trim())
    .filter(b => /^[A-Z]{2}Z\d{3}/m.test(b));
}

// ── Parse one zone block: pull zone IDs, find period sections, extract
// "Rip Current Risk" per period. Handles sub-divided risks (take WORST).
function parseZoneBlock(block: string): ParsedZoneForecast | null {
  // Header line: `CAZ362-364-111000-` (one or more zone numbers joined by -)
  const headerMatch = block.match(/^([A-Z]{2}Z\d{3}(?:-\d{3})*)-\d{6}-/m);
  if (!headerMatch) return null;
  const codeBase = headerMatch[1];              // e.g. CAZ362-364
  const statePrefix = codeBase.slice(0, 3);     // CAZ (2 letters + Z marker)
  const ids = codeBase.split("-");
  // First entry is full zone (CAZ362); subsequent are bare numbers (364).
  const zone_ids = [ids[0]];
  for (let i = 1; i < ids.length; i++) {
    zone_ids.push(`${statePrefix}${ids[i]}`);
  }

  // Find period subheaders: lines starting with `.` and ending with `...`
  // Match the WHOLE line including the period text since some have multiple words.
  const periodRegex = /^\.([A-Z][A-Z \-,]+)\.\.\./gm;
  const periodMatches: Array<{ name: string; start: number }> = [];
  let m: RegExpExecArray | null;
  while ((m = periodRegex.exec(block)) !== null) {
    periodMatches.push({ name: m[1].trim(), start: m.index });
  }
  if (periodMatches.length === 0) {
    return { zone_ids, current_risk: null, next_risk: null,
             conditions_current: "", conditions_next: "", raw: block };
  }

  // Extract risk + conditions per period (current = first, next = second).
  function riskFromPeriodText(periodText: string): string | null {
    // Direct line: "Rip Current Risk*..........High."
    const direct = periodText.match(/Rip Current Risk\*?\s*\.+\s*(High|Moderate|Low)\.?/i);
    if (direct) return capitalize(direct[1]);
    // Sub-divided: "Rip Current Risk*..." then sub-area lines below.
    if (/Rip Current Risk\*?\s*\.\.\.\s*$/m.test(periodText)) {
      const subRisks = [...periodText.matchAll(/^\s+[A-Z][^\n]*?\.\.\.\s*(High|Moderate|Low)\.?\s*$/gim)]
        .map(mm => capitalize(mm[1]));
      return worstRisk(...subRisks);
    }
    return null;
  }
  function capitalize(s: string): string {
    return s.charAt(0).toUpperCase() + s.slice(1).toLowerCase();
  }

  // Slice each period to the next period start or block end.
  const periods: Array<{ name: string; text: string }> = [];
  for (let i = 0; i < periodMatches.length; i++) {
    const start = periodMatches[i].start;
    const end = i + 1 < periodMatches.length ? periodMatches[i + 1].start : block.length;
    periods.push({ name: periodMatches[i].name, text: block.slice(start, end) });
  }

  const current = periods[0];
  const next    = periods[1] ?? null;

  return {
    zone_ids,
    current_risk: current ? riskFromPeriodText(current.text) : null,
    next_risk:    next ? riskFromPeriodText(next.text) : null,
    conditions_current: current?.text.trim() ?? "",
    conditions_next:    next?.text.trim() ?? "",
    raw: block,
  };
}

// ── Lazy zone warm: fetch GeoJSON for a zone_id and INSERT into nws_zones
// if absent. Mirrors scripts/poll_nws_alerts.py:get_zone_geom_cached.
async function ensureZoneCached(
  supabase: SupabaseClient, zone_id: string,
): Promise<boolean> {
  // Already cached?
  const { data: existing } = await supabase
    .from("nws_zones").select("zone_id").eq("zone_id", zone_id).maybeSingle();
  if (existing) return false;  // already cached

  try {
    const resp = await fetch(ZONE_GEOJSON_URL(zone_id), {
      headers: { "User-Agent": USER_AGENT, "Accept": "application/geo+json" },
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
    });
    if (!resp.ok) return false;
    const j = await resp.json();
    const props = j.properties ?? {};
    const geomJson = j.geometry;
    if (!geomJson) return false;

    const stateCodes: string[] = props.state ? [props.state] : [];
    const state = stateCodes[0] ?? null;

    const { error } = await supabase
      .from("nws_zones")
      .insert({
        zone_id,
        zone_type: "forecast",
        name: props.name ?? null,
        state,
        // PostGIS auto-coerces GeoJSON via ST_GeomFromGeoJSON when expr is
        // built via SQL. supabase-js .insert accepts text; we cast via RPC
        // — simpler is to round-trip through a stored helper, but inline
        // we use the existing schema (geom geography) which expects WKB.
        // Workaround: write via RPC if available; else rely on a SQL UPSERT.
        geom: null,  // placeholder; geom set via RPC below
        fetched_at: new Date().toISOString(),
        raw: j,
      });
    if (error) return false;
    // Update geom from the GeoJSON via RPC-style SQL (postgrest accepts
    // .rpc on a SQL fn; we don't have one, so use direct UPDATE via
    // pg-rest geom_geojson coercion: NOT supported by PostgREST directly.
    // Pragmatic: write a one-shot UPDATE via a tiny stored fn? — too much
    // for v1. Instead, the trigger doesn't need the WKB form: ST_DWithin
    // can take ST_GeomFromGeoJSON. But the existing geom column is
    // geography type. Alternative: a SQL helper `_nws_zone_upsert_geojson`.
    //
    // For v1 inline: write a temporary helper at SQL apply time, then
    // call via supabase.rpc. See migration 20260610c → defer to a tiny
    // helper added there. Since we didn't add one, fall back to setting
    // geom via raw SQL using PostgREST's `.update` won't work for
    // ST_GeomFromGeoJSON — that's a function. So a one-line SQL UPDATE
    // is the right tool, but we don't have psql here.
    //
    // Workaround: call a small RPC helper. We'll add it inline below by
    // creating a stored fn at function-call time (idempotent CREATE OR
    // REPLACE). Simpler: write geom via SQL inside the cache record by
    // POSTing to a helper RPC `_nws_zone_set_geom` that we ship in this
    // function's prerequisites.
    //
    // To keep this PR self-contained, see the prerequisite SQL helper
    // shipped in 20260610c — it's a thin upsert function.
    await supabase.rpc("_nws_zone_set_geom_from_geojson", {
      p_zone_id: zone_id,
      p_geojson: JSON.stringify(geomJson),
    });
    return true;
  } catch (_e) {
    return false;
  }
}

// ── Phase 2 alert helpers ───────────────────────────────────────────────

interface AlertFeature {
  id?: string;
  geometry?: any;
  properties?: any;
}

async function fetchActiveAlerts(state: string): Promise<AlertFeature[]> {
  try {
    const resp = await fetch(ALERTS_URL(state), {
      headers: { "User-Agent": USER_AGENT, "Accept": "application/geo+json" },
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
    });
    if (!resp.ok) return [];
    const j = await resp.json();
    return Array.isArray(j?.features) ? j.features : [];
  } catch (_e) {
    return [];
  }
}

// PIP an alert's inline geometry against scoreable beaches in a state.
// Helper RPC handles the ST_Intersects + state filter server-side.
async function pipAlertInline(
  supabase: SupabaseClient, geom: any, state: string,
): Promise<number[]> {
  try {
    const { data } = await supabase.rpc("_nws_alert_pip_beaches", {
      p_geom_geojson: JSON.stringify(geom),
      p_state: state,
    });
    if (!Array.isArray(data)) return [];
    return data.map((n: any) => Number(n)).filter((n) => Number.isFinite(n));
  } catch (_e) {
    return [];
  }
}

// PIP an alert against scoreable beaches via a cached zone polygon.
// Does NOT lazy-warm zones — that's the daily bulk-load's job
// (scripts/load_nws_coastal_zones.py). Missing zone → return []; on the
// next bulk-load run the zone is cached and subsequent ticks match.
async function pipAlertViaZone(
  supabase: SupabaseClient, zoneUrl: string, state: string,
): Promise<number[]> {
  const [zone_id] = zoneIdAndTypeFromUrl(zoneUrl);
  if (!zone_id) return [];
  try {
    const { data } = await supabase.rpc("_nws_alert_pip_via_zone", {
      p_zone_id: zone_id,
      p_state: state,
    });
    if (!Array.isArray(data)) return [];
    return data.map((n: any) => Number(n)).filter((n) => Number.isFinite(n));
  } catch (_e) {
    return [];
  }
}

// Build a beach_advisory row from an NWS alert + matched beach_fid.
// Mirrors scripts/poll_nws_alerts.py:upsert_alert (lines 177-220).
function buildAlertRow(
  beach_fid: number, alert: AlertFeature, fetchedAtIso: string,
): Record<string, any> | null {
  const p = alert.properties ?? {};
  const alert_id: string = alert.id ?? p.id ?? "";
  const valid_from: string | null = p.effective ?? p.onset ?? p.sent ?? null;
  const valid_to: string | null = p.ends ?? p.expires ?? null;
  const event_type: string | null = p.event ?? null;
  if (!alert_id || !valid_from || !valid_to || !event_type) return null;

  const nwsSev = String(p.severity ?? "").toLowerCase();
  const severity = ["minor", "moderate", "severe", "extreme"].includes(nwsSev)
    ? nwsSev : "moderate";

  return {
    beach_fid,
    advisory_key: `nws:${alert_id}`,
    source: "nws",
    event_type,
    severity,
    valid_from,
    valid_to,
    label: event_type,
    icon: "⚠️",
    raw_data: {
      certainty:   p.certainty,
      urgency:     p.urgency,
      headline:    p.headline,
      description: p.description,
      instruction: p.instruction,
      sender:      p.sender,
      areaDesc:    p.areaDesc,
    },
    fetched_at: fetchedAtIso,
  };
}

// ── Main ────────────────────────────────────────────────────────────────
Deno.serve(async (req: Request): Promise<Response> => {
  const headers = corsHeaders(req, "POST, OPTIONS");

  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers });
  }

  // Admin-secret gated. Skipped for GET so an orchestrator can ping without
  // payload, but body-bearing calls require the secret.
  if (req.method === "POST") {
    const provided = req.headers.get("x-admin-secret") ?? "";
    if (!ADMIN_SECRET || provided !== ADMIN_SECRET) {
      return new Response(JSON.stringify({ error: "unauthorized" }),
        { status: 401, headers: { ...headers, "Content-Type": "application/json" } });
    }
  }

  let body: any = {};
  try { body = await req.json(); } catch (_e) { body = {}; }
  const wfoFilter: string[] | null = Array.isArray(body.wfos) && body.wfos.length > 0
    ? body.wfos.map((w: string) => w.toUpperCase())
    : null;
  const stateFilter: string | null = body.state ?? null;

  const wfosToFetch = wfoFilter ?? COASTAL_WFOS;
  const debugMode: boolean = !!body.debug;

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    auth: { persistSession: false },
  });

  const t0 = Date.now();
  const startedAtIso = new Date().toISOString();   // watermark anchor for phase 3
  const today = todayLocalDateInPacific();   // SRFs publish in WFO local tz; we anchor on Pacific noon-ish heuristic
  const skipAlerts: boolean = body.skip_alerts === true;
  const stats = {
    wfos_attempted: 0,
    wfos_ok: 0,
    wfos_404: 0,
    wfos_failed: 0,
    zone_rows_upserted: 0,
    zones_warmed: 0,
    alerts_fetched: 0,
    alerts_matched: 0,
    alert_rows_upserted: 0,
    alerts_swept: 0,
    affected_states: new Set<string>(),
    errors: [] as string[],
  };

  // ── Fetch SRFs in parallel batches ─────────────────────────────────────
  const allParsed: Array<{ wfo: string; parsed: ParsedZoneForecast[] }> = [];

  for (let i = 0; i < wfosToFetch.length; i += CONCURRENT_FETCHES) {
    const batch = wfosToFetch.slice(i, i + CONCURRENT_FETCHES);
    const results = await Promise.all(batch.map(async (wfo) => {
      stats.wfos_attempted++;
      try {
        const resp = await fetch(SRF_URL(wfo), {
          headers: { "User-Agent": USER_AGENT },
          signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
        });
        if (resp.status === 404) {
          stats.wfos_404++;
          return { wfo, parsed: [] };
        }
        if (!resp.ok) {
          stats.wfos_failed++;
          stats.errors.push(`${wfo}: HTTP ${resp.status}`);
          return { wfo, parsed: [] };
        }
        const html = await resp.text();
        const text = extractPreContent(html);
        const blocks = splitZoneBlocks(text);
        const parsed: ParsedZoneForecast[] = [];
        for (const block of blocks) {
          const p = parseZoneBlock(block);
          if (p) parsed.push(p);
        }
        stats.wfos_ok++;
        if (debugMode) {
          const ampCount = (text.match(/\$\$/g) ?? []).length;
          const zoneCount = (text.match(/^[A-Z]{2}Z\d{3}/gm) ?? []).length;
          stats.errors.push(`${wfo}: html_len=${html.length} text_len=${text.length} dollars=${ampCount} zone_codes=${zoneCount} blocks=${blocks.length} parsed=${parsed.length}`);
          if (blocks.length === 0) {
            stats.errors.push(`${wfo}: text_head=${text.slice(0, 300).replace(/\n/g, '|')}`);
            stats.errors.push(`${wfo}: text_tail=${text.slice(-300).replace(/\n/g, '|')}`);
          } else if (parsed.length === 0) {
            stats.errors.push(`${wfo}: first_block_head=${blocks[0].slice(0, 200).replace(/\n/g, '|')}`);
          }
        }
        return { wfo, parsed };
      } catch (e) {
        stats.wfos_failed++;
        stats.errors.push(`${wfo}: ${(e as Error).message}`);
        return { wfo, parsed: [] };
      }
    }));
    allParsed.push(...results);
  }

  // ── Warm any unknown zones, then UPSERT forecasts ──────────────────────
  const allZoneIds = new Set<string>();
  for (const { parsed } of allParsed) {
    for (const p of parsed) for (const z of p.zone_ids) allZoneIds.add(z);
  }

  // Find which are NOT already cached. Bulk query.
  let unknownZones: string[] = [];
  if (allZoneIds.size > 0) {
    const { data: known } = await supabase
      .from("nws_zones").select("zone_id")
      .in("zone_id", [...allZoneIds]);
    const knownSet = new Set((known ?? []).map((r: any) => r.zone_id));
    unknownZones = [...allZoneIds].filter(z => !knownSet.has(z));
  }

  // Warm unknown zones in parallel batches.
  for (let i = 0; i < unknownZones.length; i += CONCURRENT_FETCHES) {
    const batch = unknownZones.slice(i, i + CONCURRENT_FETCHES);
    const warmed = await Promise.all(batch.map(z => ensureZoneCached(supabase, z)));
    stats.zones_warmed += warmed.filter(Boolean).length;
  }

  // UPSERT wfo_srf_forecast.
  const rows: any[] = [];
  for (const { wfo, parsed } of allParsed) {
    for (const p of parsed) {
      for (const zone_id of p.zone_ids) {
        rows.push({
          zone_id, wfo, forecast_date: today, period: "current",
          rip_current_risk: p.current_risk,
          conditions: p.conditions_current.slice(0, 4000),
          fetched_at: new Date().toISOString(),
          raw: p.raw.slice(0, 8000),
        });
        rows.push({
          zone_id, wfo, forecast_date: today, period: "next",
          rip_current_risk: p.next_risk,
          conditions: p.conditions_next.slice(0, 4000),
          fetched_at: new Date().toISOString(),
          raw: null,
        });
      }
    }
  }

  if (rows.length > 0) {
    const CHUNK = 500;
    for (let i = 0; i < rows.length; i += CHUNK) {
      const chunk = rows.slice(i, i + CHUNK);
      const { error: upErr } = await supabase
        .from("wfo_srf_forecast")
        .upsert(chunk, { onConflict: "zone_id,forecast_date,period" });
      if (upErr) {
        stats.errors.push(`upsert chunk ${i}: ${upErr.message.slice(0, 200)}`);
      } else {
        stats.zone_rows_upserted += chunk.length;
      }
    }
  }

  // ── Rebind beach.coastal_zone_id NOT triggered inline — the per-row
  //    geography KNN over 3-4k unbound beaches exceeds the PostgREST
  //    statement_timeout when called from the edge fn. Instead, the
  //    rebind runs as a separate daily orch_job depending on this
  //    refresh; see 20260610h_rebind_orch_job.sql.

  // ── Phase 2: NWS active alerts → beach_advisory source='nws' ──────────
  // Per-state fetch from api.weather.gov/alerts/active. For each feature,
  // resolve affected beach fids via inline geometry OR per-affected-zone
  // PIP, then UPSERT one row per (beach_fid, alert_id) tuple.
  const alertRowBuffer: Record<string, any>[] = [];

  if (!skipAlerts) {
    for (let i = 0; i < ALERT_STATES.length; i += CONCURRENT_FETCHES) {
      const batch = ALERT_STATES.slice(i, i + CONCURRENT_FETCHES);
      const fetched = await Promise.all(
        batch.map(async (state) => ({ state, alerts: await fetchActiveAlerts(state) })),
      );
      for (const { state, alerts } of fetched) {
        stats.alerts_fetched += alerts.length;
        for (const alert of alerts) {
          // Two disjoint paths: alerts come either with inline geometry
          // (e.g., County-Warning-Area-bounded products) OR zone-coded
          // (NWS zone-public products). Try whichever shape the alert
          // ships in; never run both — empty result from Path 1 means the
          // alert geometry simply didn't intersect any in-scope beach.
          let matched: number[] = [];
          if (alert.geometry) {
            matched = await pipAlertInline(supabase, alert.geometry, state);
          } else {
            const zoneUrls: string[] = Array.isArray(alert.properties?.affectedZones)
              ? alert.properties.affectedZones : [];
            const perZoneMatches = await Promise.all(
              zoneUrls.map((zurl) => pipAlertViaZone(supabase, zurl, state)),
            );
            const merged = new Set<number>();
            for (const arr of perZoneMatches) for (const fid of arr) merged.add(fid);
            matched = [...merged];
          }
          if (matched.length === 0) continue;
          stats.alerts_matched++;
          for (const beach_fid of matched) {
            const row = buildAlertRow(beach_fid, alert, startedAtIso);
            if (row) alertRowBuffer.push(row);
          }
        }
      }
    }

    if (alertRowBuffer.length > 0) {
      const CHUNK = 500;
      for (let i = 0; i < alertRowBuffer.length; i += CHUNK) {
        const chunk = alertRowBuffer.slice(i, i + CHUNK);
        const { error: upErr } = await supabase
          .from("beach_advisory")
          .upsert(chunk, { onConflict: "beach_fid,advisory_key" });
        if (upErr) {
          stats.errors.push(`alert upsert chunk ${i}: ${upErr.message.slice(0, 200)}`);
        } else {
          stats.alert_rows_upserted += chunk.length;
        }
      }
    }

    // ── Phase 3: watermark sweep ────────────────────────────────────────
    // Drop any source='nws' rows whose fetched_at predates this run.
    // Together with the upsert above this gives "set replacement" semantics:
    // after this function returns, beach_advisory source='nws' reflects
    // exactly the active NWS alert set as of startedAtIso.
    const { count: sweepCount, error: sweepErr } = await supabase
      .from("beach_advisory")
      .delete({ count: "exact" })
      .eq("source", "nws")
      .lt("fetched_at", startedAtIso);
    if (sweepErr) {
      stats.errors.push(`watermark sweep: ${sweepErr.message.slice(0, 200)}`);
    } else {
      stats.alerts_swept = sweepCount ?? 0;
    }
  }

  return new Response(JSON.stringify({
    ok: true,
    today,
    started_at: startedAtIso,
    // Phase 1: SRF
    wfos_attempted: stats.wfos_attempted,
    wfos_ok: stats.wfos_ok,
    wfos_404: stats.wfos_404,
    wfos_failed: stats.wfos_failed,
    zones_warmed: stats.zones_warmed,
    zone_rows_upserted: stats.zone_rows_upserted,
    // Phase 2: alerts
    alerts_fetched: stats.alerts_fetched,
    alerts_matched: stats.alerts_matched,
    alert_rows_upserted: stats.alert_rows_upserted,
    // Phase 3: sweep
    alerts_swept: stats.alerts_swept,
    skip_alerts: skipAlerts,
    elapsed_ms: Date.now() - t0,
    errors_sample: stats.errors.slice(0, 10),
  }), { status: 200, headers: { ...headers, "Content-Type": "application/json" } });
});

// SRFs are issued in WFO local time mid-morning. We anchor forecast_date on
// Pacific date — every WFO has issued its today-block by 14:00 UTC, and the
// "current" period covers Today through Tomorrow in WFO local time. Using
// Pacific as the anchor keeps CA examples lined up with how the rest of
// the app handles "today".
function todayLocalDateInPacific(): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Los_Angeles",
    year: "numeric", month: "2-digit", day: "2-digit",
  }).formatToParts(new Date());
  const get = (t: string) => parts.find(p => p.type === t)?.value ?? "";
  return `${get("year")}-${get("month")}-${get("day")}`;
}
