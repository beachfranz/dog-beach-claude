// admin-curate-beach/index.ts
//
// GET ?fid=N
//   -> { beach, photos[], default_match_quality } — per-photo data
//      including ccc access_point_name, attribution, etc.
//
// POST { fid, photos: [{id, sort_order, keep}], match_quality?, notes? }
//   -> { ok: true, kept_count, deleted_count }
//   - Updates sort_order on each kept photo
//   - DELETEs photos with keep=false
//   - Upserts beach_curation_status with match_quality + curated_at = now()
//
// Auth: requires x-admin-secret header.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import { requireAdmin } from "../_shared/admin-auth.ts";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "GET, POST, OPTIONS"), "Content-Type": "application/json" };
  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

  if (req.method === "GET") {
    const url = new URL(req.url);
    const fid = parseInt(url.searchParams.get("fid") || "", 10);
    if (!Number.isFinite(fid)) {
      return new Response(JSON.stringify({ error: "fid required" }),
        { status: 400, headers: cors });
    }

    const { data: beach } = await supabase
      .from("beaches_gold")
      .select("fid, name, display_name_override, county_name, state, lat, lon")
      .eq("fid", fid).maybeSingle();
    if (!beach) {
      return new Response(JSON.stringify({ error: "Beach not found" }),
        { status: 404, headers: cors });
    }

    // Default ordering minimizes manual drag burden on the curator:
    //   1. Curator-touched photos (sort_order < 1000, set by prior saves)
    //      keep their explicit order — never reorder a curator's intent.
    //   2. Untouched photos (sort_order >= 1000 from loaders) come in by
    //      model predicted_keep_prob DESC — best-first per the Tier 2
    //      photo classifier. Curator drags only when the model is wrong.
    const { data: photos } = await supabase
      .from("beach_photos")
      .select("id, source, image_url, thumb_url, attribution, license, " +
              "page_url, distance_m, sort_order, source_meta, match_quality, " +
              "curated_at, lat, lng")
      .eq("arena_group_id", fid)
      .is("curated_at", null);   // hide already-decided photos; curator only
                                  // sees what they still need to decide.
                                  // Propagated decisions from sibling beaches
                                  // mark photos curated_at — they then drop
                                  // out of the grid here, so a Marin beach
                                  // whose 30 gallery photos were already
                                  // decided at sister-beach-1 shows empty.
    (photos ?? []).sort((a, b) => {
      const aTouched = (a.sort_order ?? 9999) < 1000;
      const bTouched = (b.sort_order ?? 9999) < 1000;
      if (aTouched !== bTouched) return aTouched ? -1 : 1;
      if (aTouched) return (a.sort_order ?? 0) - (b.sort_order ?? 0);
      const aProb = Number((a.source_meta as Record<string, unknown> | null)?.predicted_keep_prob ?? 0);
      const bProb = Number((b.source_meta as Record<string, unknown> | null)?.predicted_keep_prob ?? 0);
      if (aProb !== bProb) return bProb - aProb;
      return (a.sort_order ?? 0) - (b.sort_order ?? 0);
    });

    const { data: status } = await supabase
      .from("beach_curation_status")
      .select("curated_at, curated_by, notes, match_quality")
      .eq("arena_group_id", fid).maybeSingle();

    // Default match-quality per photo: word-overlap heuristic between
    // beach name and source's "access_point_name" (CCC) or alt text.
    const beachName = beach.display_name_override ?? beach.name ?? "";
    const enriched = (photos ?? []).map(p => {
      const meta = (p.source_meta as Record<string, unknown>) ?? {};
      const accessName = (meta.access_point_name as string)
                         ?? (meta.alt_description as string)
                         ?? (meta.alt as string)
                         ?? "";
      const def = defaultMatchQuality(beachName, accessName);
      return {
        ...p,
        access_point_name: accessName,
        default_match_quality: def,
        match_quality: p.match_quality ?? def,  // saved value or default
      };
    });

    return new Response(JSON.stringify({
      beach: {
        fid: beach.fid,
        display_name: beach.display_name_override ?? beach.name,
        county_name: beach.county_name,
        state: beach.state,
        lat: beach.lat,
        lng: beach.lon,
      },
      photos: enriched,
      curation_status: status,
    }), { status: 200, headers: cors });
  }

  if (req.method === "POST") {
    let body: {
      fid?: number;
      photos?: Array<{ id: number; sort_order: number; keep: boolean; match_quality?: string }>;
      match_quality?: string;
      notes?: string;
      curated_by?: string;
    };
    try {
      body = await req.json();
    } catch {
      return new Response(JSON.stringify({ error: "Invalid JSON" }), { status: 400, headers: cors });
    }

    const fid = body.fid;
    if (!Number.isFinite(fid)) {
      return new Response(JSON.stringify({ error: "fid required" }), { status: 400, headers: cors });
    }

    const photos = body.photos ?? [];
    const toDelete = photos.filter(p => !p.keep).map(p => p.id);
    const toKeep   = photos.filter(p => p.keep);

    let deleted = 0;
    if (toDelete.length) {
      // BEFORE deleting, write tombstones so loaders don't re-fetch these
      // photos next run. Capture metadata fields (title/artist/scores) at
      // trash time so the Tier 2 photo-quality model can train on
      // feature-parity labels (was: leakage via missingness — Polyzotis
      // 2018). Failure to tombstone is non-fatal — log and proceed.
      //
      // SIBLING PROPAGATION: a single trash decision on photo X (source,
      // external_id) cascades to ALL beach_photos rows sharing that
      // identity. CDPR/NPS gallery photos are fanned out to many sub-
      // beaches; one curator click should not require seeing the same
      // image at 5 different beaches. Expand toDelete to include all
      // siblings before tombstoning + deleting.
      const { data: seedRows } = await supabase
        .from("beach_photos")
        .select("source, external_id")
        .in("id", toDelete);
      const seedKeys = new Set(
        (seedRows ?? [])
          .filter(r => r.external_id != null)
          .map(r => `${r.source}|${r.external_id}`)
      );
      if (seedKeys.size) {
        const { data: allSiblings } = await supabase
          .from("beach_photos")
          .select("id, source, external_id")
          .in("source", Array.from(new Set((seedRows ?? []).map(r => r.source))));
        const expandedIds = new Set<number>(toDelete);
        for (const s of allSiblings ?? []) {
          if (s.external_id != null && seedKeys.has(`${s.source}|${s.external_id}`)) {
            expandedIds.add(s.id as number);
          }
        }
        if (expandedIds.size > toDelete.length) {
          console.log(`sibling propagation: trash expanded ${toDelete.length} -> ${expandedIds.size} rows`);
          toDelete.length = 0;
          for (const id of expandedIds) toDelete.push(id);
        }
      }

      const { data: doomed } = await supabase
        .from("beach_photos")
        .select("arena_group_id, source, external_id, distance_m, license, source_meta")
        .in("id", toDelete);
      const tombstones = (doomed ?? [])
        .filter(d => d.external_id != null)
        .map(d => {
          const sm = (d.source_meta ?? {}) as Record<string, unknown>;
          const num = (k: string) => {
            const v = sm[k];
            return v == null ? null : Number(v);
          };
          return {
            arena_group_id:   d.arena_group_id,
            source:           d.source,
            external_id:      d.external_id,
            rejected_by:      body.curated_by ?? null,
            title:            (sm.title as string) ?? null,
            artist:           (sm.artist as string) ?? null,
            distance_m:       d.distance_m ?? null,
            relevance_score:  num("relevance_score"),
            name_match_score: num("name_match_score"),
            composite_score:  num("composite_score"),
            license:          d.license ?? null,
            // Vision tags from load_photo_vision_tags.py — capture at trash
            // time so the rejected side has feature parity with the kept
            // side. Lets the photo-quality classifier train on vision
            // features without the tombstone-missingness leak.
            vision_tags:      (sm.vision as Record<string, unknown>) ?? null,
          };
        });
      if (tombstones.length) {
        const { error: tombErr } = await supabase
          .from("beach_photo_rejected")
          .upsert(tombstones, { onConflict: "arena_group_id,source,external_id" });
        if (tombErr) console.error("tombstone write failed:", tombErr.message);
      }

      const { error: delErr, count } = await supabase
        .from("beach_photos")
        .delete({ count: "exact" })
        .in("id", toDelete);
      if (delErr) return new Response(JSON.stringify({ error: delErr.message }),
        { status: 500, headers: cors });
      deleted = count ?? 0;
    }

    // Update sort_order + match_quality on kept photos.
    // SIBLING PROPAGATION: a keep decision on photo X (source, external_id)
    // also marks all sibling rows curated_at + match_quality. sort_order is
    // local per-beach so it stays per-row (the explicit p.sort_order is for
    // the beach the curator was on; siblings keep their own positions).
    for (const p of toKeep) {
      const update: Record<string, unknown> = {
        sort_order: p.sort_order,
        curated_at: new Date().toISOString(),
        curated_by: body.curated_by ?? null,
      };
      if (p.match_quality) update.match_quality = p.match_quality;
      const { error: upErr } = await supabase
        .from("beach_photos")
        .update(update)
        .eq("id", p.id);
      if (upErr) return new Response(JSON.stringify({ error: upErr.message }),
        { status: 500, headers: cors });

      // Find siblings sharing (source, external_id) at OTHER beaches
      const { data: meta } = await supabase
        .from("beach_photos")
        .select("source, external_id")
        .eq("id", p.id).single();
      if (!meta?.external_id) continue;
      const siblingUpdate: Record<string, unknown> = {
        curated_at: update.curated_at,
        curated_by: update.curated_by,
      };
      if (p.match_quality) siblingUpdate.match_quality = p.match_quality;
      const { error: sibErr, count: sibCount } = await supabase
        .from("beach_photos")
        .update(siblingUpdate, { count: "exact" })
        .eq("source", meta.source)
        .eq("external_id", meta.external_id)
        .neq("id", p.id)
        .is("curated_at", null);
      if (sibErr) console.warn("sibling keep propagation:", sibErr.message);
      else if (sibCount) console.log(`keep propagated to ${sibCount} sibling rows`);
    }

    // Upsert curation status (mark beach as done)
    await supabase.from("beach_curation_status").upsert({
      arena_group_id: fid!,
      curated_at: new Date().toISOString(),
      curated_by: body.curated_by ?? null,
      notes: body.notes ?? null,
      match_quality: body.match_quality ?? null,
    });

    return new Response(JSON.stringify({
      ok: true, kept_count: toKeep.length, deleted_count: deleted,
    }), { status: 200, headers: cors });
  }

  return new Response(JSON.stringify({ error: "Method not allowed" }),
    { status: 405, headers: cors });
});

// ─── Match-quality default heuristic ─────────────────────────────────────
function defaultMatchQuality(beachName: string, otherName: string): string {
  if (!beachName || !otherName) return "wtf";
  const a = beachName.toLowerCase();
  const b = otherName.toLowerCase();
  if (a === b) return "exact";
  // Word-set overlap (Jaccard)
  const norm = (s: string) => s.replace(/[^a-z0-9 ]+/g, " ").split(/\s+/)
                               .filter(w => w.length > 2 && !STOP.has(w));
  const wa = new Set(norm(a)); const wb = new Set(norm(b));
  if (wa.size === 0 || wb.size === 0) return "wtf";
  const inter = [...wa].filter(w => wb.has(w)).length;
  const union = new Set([...wa, ...wb]).size;
  const j = inter / union;
  if (j >= 0.85) return "exact";
  if (j >= 0.55) return "very_likely";
  if (j >= 0.30) return "somewhat_likely";
  if (j >= 0.10) return "unlikely";
  return "wtf";
}
const STOP = new Set([
  "the","beach","park","state","city","of","and","at","to","north","south","east","west",
  "main","public","point","ca","san","los","new","county",
]);
