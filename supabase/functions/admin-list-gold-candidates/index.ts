// admin-list-gold-candidates/index.ts
//
// Returns gold-set candidates with extractions merged from FOUR sources:
//   1. beach_policy_extractions      (v3 calibration variants — diagnostic)
//   2. park_url_extractions          (CPAD park_url scrapes — production)
//   3. cpad_unit_dogs_policy         (per-CPAD-unit policy — production)
//   4. operator_dogs_policy          (per-operator policy — production)
//
// Plus exception tables for sections[]:
//   5. cpad_unit_policy_exceptions
//   6. operator_policy_exceptions
//
// The curator UI iterates `extractions[field_name]` as a list of variants;
// we synthesize variant rows from production tables mapped to v3 field
// names so each row appears alongside any LLM-extracted variants for the
// same field. Synthetic variants are tagged variant_key='park_url' /
// 'cpad_unit' / 'operator' so the UI can distinguish source.
//
// GET ?set_name=v3   (default 'v3')
// Security: x-admin-secret + per-IP rate limit (requireAdmin).

import { createClient }   from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }    from "../_shared/cors.ts";
import { requireAdmin }   from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// ── Mappers: production-table column values → v3 field values ─────────

function leashFromBool(b: boolean | null | undefined): string {
  if (b === true)  return "on_leash";
  if (b === false) return "off_leash";
  return "unknown";
}
function leashFromText(s: string | null): string {
  if (!s) return "unknown";
  const v = s.toLowerCase();
  if (v.includes("required") || v.includes("must"))    return "on_leash";
  if (v.includes("off-leash") || v.includes("offleash")) return "off_leash";
  if (v.includes("optional"))                           return "leash_optional";
  if (v.includes("varies") || v.includes("time"))       return "varies_by_time";
  return "unknown";
}
function presenceFromBool(b: boolean | null | undefined): string {
  if (b === true)  return "yes";
  if (b === false) return "none";
  return "unknown";
}
function lifeguardFromBool(b: boolean | null | undefined): string {
  if (b === true)  return "full_time";  // lossy — could be seasonal
  if (b === false) return "none";
  return "unknown";
}
function confidenceFromNumeric(n: number | null | undefined): string {
  if (n === null || n === undefined) return "unknown";
  if (n >= 0.7) return "high";
  if (n >= 0.4) return "medium";
  if (n >  0)   return "low";
  return "none";
}
function jsonOrNull(v: any): any { return v ?? null; }

// Build feature_zones object from area_* columns. Returns 7-key object.
function featureZonesFromAreas(row: any): Record<string, string> {
  const norm = (v: string | null) => {
    if (!v) return "unknown";
    const s = v.toLowerCase().trim();
    if (s === "off_leash" || s === "off-leash") return "off_leash";
    if (s === "on_leash" || s === "on-leash" || s === "leashed") return "on_leash";
    if (s === "not_allowed" || s === "prohibited" || s === "no") return "not_allowed";
    if (s === "seasonal") return "seasonal";
    return "unknown";
  };
  return {
    sand:             norm(row.area_sand),
    water_swim:       norm(row.area_water),
    picnic_areas:     norm(row.area_picnic_area),
    parking_lot:      norm(row.area_parking_lot),
    trails_boardwalk: norm(row.area_trails),
    campgrounds:      norm(row.area_campground),
    food_concession:  "unknown",  // not extracted in either production table
  };
}

function makeVariant(opts: {
  variantKey: string;
  parsedValue: string | null;
  evidenceQuote?: string | null;
  sourceUrl?: string | null;
  modelName?: string | null;
  rawObject?: any;          // for structured fields, attach the raw JSON for the UI
  extractedAt?: string | null;
}) {
  return {
    variant_key:    opts.variantKey,
    parsed_value:   opts.parsedValue,
    raw_response:   opts.rawObject ? JSON.stringify(opts.rawObject) : (opts.parsedValue ?? null),
    evidence_quote: opts.evidenceQuote ?? null,
    source_url:     opts.sourceUrl ?? null,
    model_name:     opts.modelName ?? null,
    is_canon:       false,        // production tables aren't "canon" in the calibration sense
    extracted_at:   opts.extractedAt ?? null,
  };
}

// ── Source synthesis ─────────────────────────────────────────────────

// Generic synthesis from any row that uses the park_url_extractions /
// policy_research_extractions column shape (they share dogs_*/has_*/hours_text
// columns). variantTag distinguishes source.
function synthesizeFromParkUrlShape(row: any, variantTag: string, modelTag: string, atField: string): { field: string; variant: any }[] {
  const out: { field: string; variant: any }[] = [];
  const sourceUrl = row.source_url || row.primary_source_url || null;
  const modelName = modelTag;
  const at = row[atField] || row.scraped_at || row.extracted_at;
  const conf = confidenceFromNumeric(row.extraction_confidence);

  if (row.dogs_allowed != null) out.push({ field: "dogs_allowed", variant: makeVariant({
    variantKey: variantTag, parsedValue: String(row.dogs_allowed),
    sourceUrl, modelName, extractedAt: at,
  })});
  if (row.dogs_leash_required != null) out.push({ field: "leash_policy", variant: makeVariant({
    variantKey: variantTag, parsedValue: leashFromText(String(row.dogs_leash_required)),
    sourceUrl, modelName, extractedAt: at,
  })});
  const temporal = [row.dogs_restricted_hours, row.dogs_seasonal_rules].filter(Boolean).join(" / ");
  if (temporal) out.push({ field: "temporal_restrictions", variant: makeVariant({
    variantKey: variantTag, parsedValue: temporal, sourceUrl, modelName, extractedAt: at,
  })});
  if (row.dogs_zone_description) out.push({ field: "dogs_off_leash_area", variant: makeVariant({
    variantKey: variantTag, parsedValue: row.dogs_zone_description, sourceUrl, modelName, extractedAt: at,
  })});
  if (row.dogs_policy_notes) out.push({ field: "dogs_policy_notes", variant: makeVariant({
    variantKey: variantTag, parsedValue: row.dogs_policy_notes, sourceUrl, modelName, extractedAt: at,
  })});
  if (row.hours_text) out.push({ field: "hours_text", variant: makeVariant({
    variantKey: variantTag, parsedValue: row.hours_text, sourceUrl, modelName, extractedAt: at,
  })});
  if (row.parking_type) out.push({ field: "parking_type", variant: makeVariant({
    variantKey: variantTag, parsedValue: row.parking_type, sourceUrl, modelName, extractedAt: at,
  })});
  if (row.has_restrooms != null) out.push({ field: "restrooms", variant: makeVariant({
    variantKey: variantTag, parsedValue: presenceFromBool(row.has_restrooms),
    sourceUrl, modelName, extractedAt: at,
  })});
  if (row.has_showers != null) out.push({ field: "outdoor_showers", variant: makeVariant({
    variantKey: variantTag, parsedValue: presenceFromBool(row.has_showers),
    sourceUrl, modelName, extractedAt: at,
  })});
  if (row.has_lifeguards != null) out.push({ field: "lifeguard", variant: makeVariant({
    variantKey: variantTag, parsedValue: lifeguardFromBool(row.has_lifeguards),
    sourceUrl, modelName, extractedAt: at,
  })});
  if (row.extraction_confidence != null) out.push({ field: "confidence", variant: makeVariant({
    variantKey: variantTag, parsedValue: conf,
    sourceUrl, modelName, extractedAt: at,
  })});
  return out;
}

const synthesizeFromParkUrl = (row: any) =>
  synthesizeFromParkUrlShape(row, "park_url", "park_url_v1", "scraped_at");
const synthesizeFromPolicyResearch = (row: any) =>
  synthesizeFromParkUrlShape(row, "research", `research:${row.origin || "unknown"}`, "extracted_at");

function synthesizeFromCpadUnit(row: any): { field: string; variant: any }[] {
  const out: { field: string; variant: any }[] = [];
  const sourceUrl = row.url_used;
  const modelName = `cpad_unit:${row.extraction_model || ""}`;
  const at = row.scraped_at;

  if (row.default_rule || row.dogs_allowed) out.push({ field: "dogs_policy_notes", variant: makeVariant({
    variantKey: "cpad_unit",
    parsedValue: `${row.default_rule || row.dogs_allowed} (CPAD: ${row.unit_name})`,
    evidenceQuote: row.source_quote ?? null,
    sourceUrl, modelName, extractedAt: at,
  })});
  if (row.leash_required != null) out.push({ field: "leash_policy", variant: makeVariant({
    variantKey: "cpad_unit", parsedValue: leashFromBool(row.leash_required),
    sourceUrl, modelName, extractedAt: at,
  })});
  // Always emit feature_zones from area_* (even if all unknown)
  const fz = featureZonesFromAreas(row);
  out.push({ field: "feature_zones", variant: makeVariant({
    variantKey: "cpad_unit", parsedValue: JSON.stringify(fz),
    rawObject: fz, sourceUrl, modelName, extractedAt: at,
    evidenceQuote: row.source_quote ?? null,
  })});
  // Time/seasonal → temporal_restrictions
  const tw = row.time_windows;
  const sr = row.seasonal_rules;
  const parts: string[] = [];
  if (tw && Object.keys(tw).length) parts.push(`time_windows: ${JSON.stringify(tw)}`);
  if (sr && Object.keys(sr).length) parts.push(`seasonal: ${JSON.stringify(sr)}`);
  if (parts.length) out.push({ field: "temporal_restrictions", variant: makeVariant({
    variantKey: "cpad_unit", parsedValue: parts.join(" / "), sourceUrl, modelName, extractedAt: at,
  })});
  if (row.designated_dog_zones) out.push({ field: "dogs_off_leash_area", variant: makeVariant({
    variantKey: "cpad_unit", parsedValue: row.designated_dog_zones, sourceUrl, modelName, extractedAt: at,
  })});
  if (row.source_quote) out.push({ field: "evidence_quote", variant: makeVariant({
    variantKey: "cpad_unit", parsedValue: row.source_quote, sourceUrl, modelName, extractedAt: at,
  })});
  if (row.extraction_confidence != null) out.push({ field: "confidence", variant: makeVariant({
    variantKey: "cpad_unit", parsedValue: confidenceFromNumeric(Number(row.extraction_confidence)),
    sourceUrl, modelName, extractedAt: at,
  })});
  return out;
}

function synthesizeFromOperator(row: any, operatorName: string | null): { field: string; variant: any }[] {
  const out: { field: string; variant: any }[] = [];
  const sourceUrl = row.source_url;
  const modelName = `operator:${operatorName || row.operator_id}`;
  const at = row.updated_at || row.created_at;
  const evidenceQuote = (row.pass_a_quotes && row.pass_a_quotes.length) ? row.pass_a_quotes[0] : null;

  if (row.summary) out.push({ field: "dogs_policy_notes", variant: makeVariant({
    variantKey: "operator", parsedValue: row.summary,
    evidenceQuote, sourceUrl, modelName, extractedAt: at,
  })});
  if (row.default_rule) out.push({ field: "dogs_allowed", variant: makeVariant({
    variantKey: "operator", parsedValue: row.default_rule,
    evidenceQuote, sourceUrl, modelName, extractedAt: at,
  })});
  if (row.leash_required != null) out.push({ field: "leash_policy", variant: makeVariant({
    variantKey: "operator", parsedValue: leashFromBool(row.leash_required),
    evidenceQuote, sourceUrl, modelName, extractedAt: at,
  })});
  const fz = featureZonesFromAreas(row);
  out.push({ field: "feature_zones", variant: makeVariant({
    variantKey: "operator", parsedValue: JSON.stringify(fz),
    rawObject: fz, sourceUrl, modelName, extractedAt: at,
    evidenceQuote,
  })});
  const tw = row.time_windows, sc = row.seasonal_closures;
  const parts: string[] = [];
  if (tw && Object.keys(tw).length) parts.push(`time_windows: ${JSON.stringify(tw)}`);
  if (sc && Object.keys(sc).length) parts.push(`seasonal_closures: ${JSON.stringify(sc)}`);
  if (parts.length) out.push({ field: "temporal_restrictions", variant: makeVariant({
    variantKey: "operator", parsedValue: parts.join(" / "), sourceUrl, modelName, extractedAt: at,
  })});
  if (row.designated_dog_zones) out.push({ field: "dogs_off_leash_area", variant: makeVariant({
    variantKey: "operator", parsedValue: row.designated_dog_zones, sourceUrl, modelName, extractedAt: at,
  })});
  if (evidenceQuote) out.push({ field: "evidence_quote", variant: makeVariant({
    variantKey: "operator", parsedValue: evidenceQuote, sourceUrl, modelName, extractedAt: at,
  })});
  const conf = row.pass_c_confidence ?? row.pass_a_confidence;
  if (conf != null) out.push({ field: "confidence", variant: makeVariant({
    variantKey: "operator", parsedValue: confidenceFromNumeric(Number(conf)),
    sourceUrl, modelName, extractedAt: at,
  })});
  return out;
}

function synthesizeFromException(row: any, kind: "cpad_unit" | "operator"): { field: string; variant: any }[] {
  // Each exception row contributes one suggested section to the sections[] field.
  const sourceUrl = row.source_url;
  const modelName = `${kind}_exception`;
  const at = row.updated_at || row.created_at;
  const section = {
    name: row.beach_name || "(unnamed exception)",
    geographic_descriptor: "",
    leash_policy: leashFromText(row.rule || ""),
    temporal_restrictions: "",
    feature_zones: { sand: "unknown", water_swim: "unknown", picnic_areas: "unknown",
                     parking_lot: "unknown", trails_boardwalk: "unknown",
                     campgrounds: "unknown", food_concession: "unknown" },
    evidence_quote: row.source_quote || "",
  };
  return [{ field: "sections", variant: makeVariant({
    variantKey: kind === "cpad_unit" ? "cpad_unit_exc" : "operator_exc",
    parsedValue: row.beach_name || row.rule || "(exception)",
    rawObject: { sections: [section] },
    sourceUrl,
    modelName,
    extractedAt: at,
    evidenceQuote: row.source_quote || null,
  })}];
}

// ── Main handler ────────────────────────────────────────────────────

Deno.serve(async (req) => {
  const cors = corsHeaders(req, ["GET", "OPTIONS"]);
  if (req.method === "OPTIONS") return new Response(null, { headers: cors });

  const fail = await requireAdmin(req, cors);
  if (fail) return fail;

  const supa = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  const url = new URL(req.url);
  const setName = url.searchParams.get("set_name") ?? "v3";

  // 1. Members
  const { data: members, error: mErr } = await supa
    .from("gold_set_membership")
    .select("fid, archetype, added_at, excluded, notes")
    .eq("set_name", setName)
    .eq("excluded", false)
    .order("archetype").order("fid");
  if (mErr) return new Response(JSON.stringify({ error: mErr.message }), { status: 500, headers: cors });

  const fids = (members ?? []).map((m: any) => m.fid);
  if (!fids.length) {
    return new Response(JSON.stringify({ set_name: setName, count: 0, candidates: [] }), { headers: cors });
  }

  // 2. beaches_gold identity
  const { data: golds } = await supa
    .from("beaches_gold")
    .select("fid, name, display_name_override, county_name, state, location_id, lat, lon, park_name, cpad_unit_id")
    .in("fid", fids);
  const goldByFid: Record<number, any> = {};
  for (const g of (golds ?? [])) goldByFid[g.fid] = g;

  // Resolve operator_id per beach via cpad_units→operators (name match).
  // Also collect cpad_unit_ids for cpad_unit_dogs_policy lookup.
  const cpadUnitIds = Array.from(new Set(
    (golds ?? []).map((g: any) => g.cpad_unit_id).filter((x: any) => x != null)
  )) as number[];
  let cpadUnitsByUnitId: Record<number, any> = {};
  let mngAgncyByFid: Record<number, string> = {};
  if (cpadUnitIds.length) {
    const { data: cpads } = await supa
      .from("cpad_units")
      .select("unit_id, mng_agncy")
      .in("unit_id", cpadUnitIds);
    for (const c of (cpads ?? [])) cpadUnitsByUnitId[c.unit_id] = c;
    for (const g of (golds ?? [])) {
      const cu = cpadUnitsByUnitId[g.cpad_unit_id];
      if (cu?.mng_agncy) mngAgncyByFid[g.fid] = cu.mng_agncy;
    }
  }
  const distinctMngAgncy = Array.from(new Set(Object.values(mngAgncyByFid)));
  const operatorByMngAgncy: Record<string, any> = {};
  if (distinctMngAgncy.length) {
    const { data: ops } = await supa
      .from("operators")
      .select("id, canonical_name, cpad_agncy_name, level, slug")
      .in("cpad_agncy_name", distinctMngAgncy);
    for (const o of (ops ?? [])) operatorByMngAgncy[o.cpad_agncy_name] = o;
  }
  const operatorIdByFid: Record<number, number> = {};
  const operatorNameByFid: Record<number, string> = {};
  for (const fid of fids) {
    const ag = mngAgncyByFid[fid];
    const op = ag ? operatorByMngAgncy[ag] : null;
    if (op) { operatorIdByFid[fid] = op.id; operatorNameByFid[fid] = op.canonical_name; }
  }

  // 3. beach_policy_extractions (v3 calibration variants)
  const { data: bpe } = await supa
    .from("beach_policy_extractions")
    .select("id, arena_group_id, field_name, variant_key, model_name, parsed_value, raw_response, evidence_quote, source_id, source_type, run_id, extracted_at")
    .in("arena_group_id", fids)
    .order("extracted_at", { ascending: false });

  // 4. park_url_extractions
  const { data: pue } = await supa
    .from("park_url_extractions")
    .select("*")
    .in("arena_group_id", fids)
    .order("scraped_at", { ascending: false });

  // 4b. policy_research_extractions
  const { data: pre } = await supa
    .from("policy_research_extractions")
    .select("*")
    .in("arena_group_id", fids)
    .order("extracted_at", { ascending: false });

  // 5. cpad_unit_dogs_policy
  let cpadPolicy: any[] = [];
  if (cpadUnitIds.length) {
    const { data } = await supa
      .from("cpad_unit_dogs_policy")
      .select("*")
      .in("cpad_unit_id", cpadUnitIds);
    cpadPolicy = data ?? [];
  }
  const cpadPolicyByUnit: Record<number, any> = {};
  for (const r of cpadPolicy) cpadPolicyByUnit[r.cpad_unit_id] = r;

  // 6. operator_dogs_policy
  const operatorIds = Array.from(new Set(Object.values(operatorIdByFid))) as number[];
  let opPolicy: any[] = [];
  if (operatorIds.length) {
    const { data } = await supa
      .from("operator_dogs_policy")
      .select("*")
      .in("operator_id", operatorIds);
    opPolicy = data ?? [];
  }
  const opPolicyByOpId: Record<number, any> = {};
  for (const r of opPolicy) opPolicyByOpId[r.operator_id] = r;

  // 7. exception tables (sections candidates)
  let cpadExceptions: any[] = [];
  if (cpadUnitIds.length) {
    const { data } = await supa
      .from("cpad_unit_policy_exceptions")
      .select("*")
      .in("cpad_unit_id", cpadUnitIds);
    cpadExceptions = data ?? [];
  }
  let opExceptions: any[] = [];
  if (operatorIds.length) {
    const { data } = await supa
      .from("operator_policy_exceptions")
      .select("*")
      .in("operator_id", operatorIds);
    opExceptions = data ?? [];
  }

  // 8. Existing truth values
  const { data: truths } = await supa
    .from("beach_policy_gold_set")
    .select("arena_group_id, fid, field_name, verified_value, truth_value_json, source_url, notes, verified_by, verified_at, curator_confidence")
    .in("arena_group_id", fids);

  // 9. Source URLs for beach_policy_extractions (via city_policy_sources)
  const sourceIds = Array.from(new Set(
    (bpe ?? []).map((e: any) => e.source_id).filter((x: any) => x != null)
  )) as number[];
  const sourceUrlById: Record<number, string> = {};
  if (sourceIds.length) {
    const { data: srcs } = await supa
      .from("city_policy_sources")
      .select("id, url, source_type, title")
      .in("id", sourceIds);
    for (const s of (srcs ?? [])) sourceUrlById[s.id] = s.url;
  }

  // 10. is_canon flags from extraction_prompt_variants — also used as an
  //     allow-list to filter out beach_policy_extractions rows whose variant
  //     has since been deactivated (e.g. raw_address structured_json).
  const { data: variants } = await supa
    .from("extraction_prompt_variants")
    .select("field_name, variant_key, is_canon, active")
    .eq("active", true);
  const canonKey = (fn: string, vk: string) => `${fn}::${vk}`;
  const canonMap: Record<string, boolean> = {};
  const activeVariantSet = new Set<string>();
  for (const v of (variants ?? [])) {
    canonMap[canonKey(v.field_name, v.variant_key)] = !!v.is_canon;
    activeVariantSet.add(canonKey(v.field_name, v.variant_key));
  }

  // ── Build extByFid: {fid: {field_name: [variant, ...]}} ────────────
  const extByFid: Record<number, Record<string, any[]>> = {};
  const push = (fid: number, field: string, variant: any) => {
    if (!extByFid[fid]) extByFid[fid] = {};
    if (!extByFid[fid][field]) extByFid[fid][field] = [];
    extByFid[fid][field].push(variant);
  };

  // beach_policy_extractions (v3 LLM calibration variants).
  // Skip rows whose variant has been deactivated — historical extractions
  // from retired prompts still live in the table but shouldn't show in the
  // curator (e.g. raw_address structured_json was deactivated 2026-05-02).
  for (const e of (bpe ?? [])) {
    if (!activeVariantSet.has(canonKey(e.field_name, e.variant_key))) continue;
    push(e.arena_group_id, e.field_name, {
      ...e,
      source_url: e.source_id ? (sourceUrlById[e.source_id] ?? null) : null,
      is_canon:   canonMap[canonKey(e.field_name, e.variant_key)] ?? false,
    });
  }

  // park_url_extractions (per-fid)
  for (const r of (pue ?? [])) {
    const synthed = synthesizeFromParkUrl(r);
    for (const { field, variant } of synthed) push(r.arena_group_id, field, variant);
  }

  // policy_research_extractions (per-fid)
  for (const r of (pre ?? [])) {
    const synthed = synthesizeFromPolicyResearch(r);
    for (const { field, variant } of synthed) push(r.arena_group_id, field, variant);
  }

  // cpad_unit_dogs_policy: applies to every fid whose cpad_unit_id matches
  for (const fid of fids) {
    const g = goldByFid[fid];
    if (!g?.cpad_unit_id) continue;
    const cp = cpadPolicyByUnit[g.cpad_unit_id];
    if (!cp) continue;
    const synthed = synthesizeFromCpadUnit(cp);
    for (const { field, variant } of synthed) push(fid, field, variant);
  }

  // operator_dogs_policy: applies to every fid whose operator_id matches
  for (const fid of fids) {
    const opId = operatorIdByFid[fid];
    if (opId == null) continue;
    const op = opPolicyByOpId[opId];
    if (!op) continue;
    const synthed = synthesizeFromOperator(op, operatorNameByFid[fid]);
    for (const { field, variant } of synthed) push(fid, field, variant);
  }

  // Exceptions → sections candidates
  // CPAD-unit exceptions: applies to any fid whose cpad_unit_id matches
  const cpadExcByUnit: Record<number, any[]> = {};
  for (const r of cpadExceptions) {
    if (!cpadExcByUnit[r.cpad_unit_id]) cpadExcByUnit[r.cpad_unit_id] = [];
    cpadExcByUnit[r.cpad_unit_id].push(r);
  }
  for (const fid of fids) {
    const g = goldByFid[fid];
    if (!g?.cpad_unit_id) continue;
    for (const exc of (cpadExcByUnit[g.cpad_unit_id] ?? [])) {
      const synthed = synthesizeFromException(exc, "cpad_unit");
      for (const { field, variant } of synthed) push(fid, field, variant);
    }
  }
  // Operator exceptions: applies to any fid whose operator_id matches
  const opExcByOpId: Record<number, any[]> = {};
  for (const r of opExceptions) {
    if (!opExcByOpId[r.operator_id]) opExcByOpId[r.operator_id] = [];
    opExcByOpId[r.operator_id].push(r);
  }
  for (const fid of fids) {
    const opId = operatorIdByFid[fid];
    if (opId == null) continue;
    for (const exc of (opExcByOpId[opId] ?? [])) {
      const synthed = synthesizeFromException(exc, "operator");
      for (const { field, variant } of synthed) push(fid, field, variant);
    }
  }

  // Truth values
  const truthByFid: Record<number, Record<string, any>> = {};
  for (const t of (truths ?? [])) {
    const f = (t.arena_group_id ?? t.fid) as number;
    if (!truthByFid[f]) truthByFid[f] = {};
    truthByFid[f][t.field_name] = t;
  }

  // Build candidates
  function uniqueSources(rows: any[]): { source_url: string; kind: string }[] {
    const seen = new Set<string>();
    const out: { source_url: string; kind: string }[] = [];
    for (const r of rows) {
      if (!r.source_url || seen.has(r.source_url)) continue;
      seen.add(r.source_url);
      out.push({ source_url: r.source_url, kind: r.source_type ?? r.variant_key ?? "extraction_source" });
    }
    return out;
  }

  const candidates = (members ?? []).map((m: any) => {
    const fid = m.fid;
    const ext = extByFid[fid] ?? {};
    const allRows = ([] as any[]).concat(...Object.values(ext));
    const g = goldByFid[fid] ?? {};
    return {
      fid,
      arena_group_id: fid,
      name: g.name ?? null,
      display_name: g.display_name_override ?? g.name ?? null,
      county: g.county_name ?? null,
      state: g.state ?? null,
      location_id: g.location_id ?? null,
      lat: g.lat ?? null,
      lon: g.lon ?? null,
      park_name: g.park_name ?? null,
      cpad_unit_id: g.cpad_unit_id ?? null,
      operator_name: operatorNameByFid[fid] ?? null,
      mng_agncy: mngAgncyByFid[fid] ?? null,
      archetype: m.archetype,
      member_notes: m.notes ?? null,
      sources: uniqueSources(allRows),
      extractions: ext,
      truth: truthByFid[fid] ?? {},
    };
  });

  return new Response(JSON.stringify({
    set_name: setName,
    count: candidates.length,
    candidates,
  }), { headers: cors });
});
