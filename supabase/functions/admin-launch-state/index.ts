// admin-launch-state/index.ts
//
// Trigger a state-launch pipeline pass from the browser. Idempotent —
// re-running picks up new Overpass results, re-promotes any unpromoted
// arena rows, and re-enqueues any extractions still missing.
//
// Body: { state: 'CA'|'OR'|'WA', max_records?: number, skip_overpass?: boolean }
// Returns per-phase counts so the UI can render a live log.
//
// PAD-US loading is NOT done here (it's a multi-thousand-row ArcGIS REST
// pull better suited to scripts/external_sources.py). NOAA stations are
// loaded via the existing admin-load-noaa-stations endpoint, which the
// UI calls separately so the user sees Phase A.0 distinct from Phase A.2.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }  from "../_shared/cors.ts";
import { requireAdmin } from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const STATE_BBOX: Record<string, [number, number, number, number]> = {
  // [s, w, n, e]
  CA: [32.50, -124.50, 42.00, -114.10],
  OR: [41.99, -124.60, 46.30, -116.45],
  WA: [45.50, -124.80, 49.00, -116.90],
};

const OVERPASS_URL = "https://overpass-api.de/api/interpreter";

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (b: unknown, s = 200) => new Response(JSON.stringify(b), { status: s, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")    return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: { state?: string; max_records?: number; skip_overpass?: boolean } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const state = (body.state ?? "OR").toUpperCase();
  if (!STATE_BBOX[state]) return json({ error: `unknown state: ${state}` }, 400);

  const maxRecords = body.max_records ?? null;
  const skipOverpass = body.skip_overpass === true;

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  const phases: Array<Record<string, unknown>> = [];
  const t0 = Date.now();

  // ── Phase A.2: Overpass ─────────────────────────────────────────────────
  let overpassFetched = 0;
  if (!skipOverpass) {
    const [s, w, n, e] = STATE_BBOX[state];
    const bbox = `${s},${w},${n},${e}`;
    const query = `[out:json][timeout:120];
(
  node["natural"="beach"](${bbox});
  way["natural"="beach"](${bbox});
  relation["natural"="beach"](${bbox});
);
out body geom;`;

    const overpassStart = Date.now();
    let elements: unknown[] = [];
    try {
      const resp = await fetch(OVERPASS_URL, {
        method: "POST",
        headers: {
          "User-Agent":   "dog-beach-scout/1.0 (admin-launch-state)",
          "Accept":       "application/json",
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: `data=${encodeURIComponent(query)}`,
      });
      if (!resp.ok) {
        phases.push({ phase: "A.2 overpass", status: "error", code: resp.status,
                       message: `Overpass HTTP ${resp.status}` });
      } else {
        const payload = await resp.json();
        elements = payload?.elements ?? [];
        if (maxRecords && elements.length > maxRecords) {
          elements = elements.slice(0, maxRecords);
        }
        overpassFetched = elements.length;
      }
    } catch (err) {
      phases.push({ phase: "A.2 overpass", status: "error",
                    message: `Overpass error: ${(err as Error).message}` });
    }

    if (elements.length > 0) {
      const { data: ingestRows, error: ingestErr } = await supabase.rpc(
        "ingest_osm_elements",
        { p_elements: elements, p_state: state },
      );
      if (ingestErr) {
        phases.push({ phase: "A.2 overpass", status: "error", message: ingestErr.message });
      } else {
        const r = (ingestRows as Record<string, number>[])?.[0] ?? {};
        phases.push({
          phase: "A.2 overpass", status: "ok",
          duration_ms: Date.now() - overpassStart,
          fetched: r.fetched ?? 0,
          inserted: r.inserted ?? 0,
          skipped_oos: r.skipped_oos ?? 0,
          skipped_no_geom: r.skipped_no_geom ?? 0,
        });
      }
    } else if (overpassFetched === 0 && phases[phases.length - 1]?.phase !== "A.2 overpass") {
      phases.push({ phase: "A.2 overpass", status: "ok", fetched: 0, inserted: 0 });
    }
  } else {
    phases.push({ phase: "A.2 overpass", status: "skipped" });
  }

  // ── Phase B: promote landing → arena ────────────────────────────────────
  const promoteStart = Date.now();
  const { data: poiRows, error: poiErr } = await supabase.rpc("promote_poi_landing_to_arena");
  const { data: osmRows, error: osmErr } = await supabase.rpc("promote_osm_landing_to_arena");
  const { data: nameRows, error: nameErr } = await supabase.rpc("refresh_arena_names_from_osm_landing");
  if (poiErr || osmErr || nameErr) {
    phases.push({ phase: "B promote", status: "error",
                  message: poiErr?.message || osmErr?.message || nameErr?.message });
  } else {
    const poi  = (poiRows as Record<string, number>[])?.[0] ?? {};
    const osm  = (osmRows as Record<string, number>[])?.[0] ?? {};
    const nam  = (nameRows as Record<string, number>[])?.[0] ?? {};
    phases.push({
      phase: "B promote", status: "ok",
      duration_ms: Date.now() - promoteStart,
      poi_promoted: poi.promoted ?? 0,
      osm_promoted: osm.promoted ?? 0,
      names_refreshed: nam.arena_rows_updated ?? 0,
    });
  }

  // ── Phase B.2 + C: state-scoped pipeline (cluster + promote_to_gold + enqueue) ──
  const pipelineStart = Date.now();
  const { data: pipeRows, error: pipeErr } = await supabase.rpc("run_pipeline_for_state",
    { p_state: state });
  if (pipeErr) {
    phases.push({ phase: "B+C pipeline", status: "error", message: pipeErr.message });
  } else {
    const r = (pipeRows as Record<string, unknown>[])?.[0] ?? {};
    phases.push({
      phase: "B+C pipeline", status: "ok",
      duration_ms: Date.now() - pipelineStart,
      ...r,
    });
  }

  // ── Re-pair NOAA stations on this state's gold rows (cheap, idempotent) ──
  const noaaStart = Date.now();
  const { data: noaaRows, error: noaaErr } = await supabase.rpc("repair_noaa_for_state",
    { p_state: state }).maybeSingle();
  if (noaaErr) {
    // Function may not exist yet — surface as a phase note, not a blocker
    phases.push({ phase: "NOAA re-pair", status: "skip", message: noaaErr.message });
  } else {
    phases.push({
      phase: "NOAA re-pair", status: "ok",
      duration_ms: Date.now() - noaaStart,
      ...((noaaRows as Record<string, unknown>) ?? {}),
    });
  }

  return json({
    state,
    total_ms: Date.now() - t0,
    phases,
  });
});
