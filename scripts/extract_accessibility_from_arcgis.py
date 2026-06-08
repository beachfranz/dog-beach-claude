"""Extract ADA accessibility from state-agency ArcGIS REST services.

Fourth ADA source class (after park_url, state_directory, operator_url,
ccc_directory): zero-LLM, authoritative agency tags. Query state DNR /
DCNR ArcGIS MapServer feature layers for ADA-flagged POIs (restrooms,
fishing piers, boat ramps, picnic areas), spatial-join to beaches_gold
within a small radius, write BEP rows with source='arcgis_<state>'.

The GIS_SOURCES dict at top registers per-state services with their
layer ID, RuleID → claimed_values map, and spatial-match radius. Future
states with ArcGIS POI services (WI / MN / PA / NY all confirmed to
publish similar layers) plug in here without script changes.

Why a separate script: HTML+Haiku extractors don't fit JSON+spatial
sources. Different cost (zero LLM), different latency (one bulk query),
different confidence tier (0.90 — agency authoritative).

Usage:
  python scripts/extract_accessibility_from_arcgis.py --state OH               # preview
  python scripts/extract_accessibility_from_arcgis.py --state OH --apply       # write
  python scripts/extract_accessibility_from_arcgis.py --state OH --skip-if-fresh-within 14
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.db import connect

ROOT = Path(__file__).resolve().parent.parent
TMP_DIR = ROOT / "tmp"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# ArcGIS RuleID → BEP claimed_values payload. Boolean fields union; notes
# semicolon-concat across multiple POIs per fid. Confidence 0.90 (highest).
#
# accessibility_features schema (per beach_amenities):
#   accessible_parking, accessible_restrooms, path_to_sand, beach_mat,
#   wheelchair_rental, accessible_viewing, notes
# Other ADA-friendly POI types (boat ramp, fishing pier, picnic, etc.)
# don't map to a direct field; they get encoded into notes.
RULE_MAP_OHIODNR = {
    189: {"notes": "ADA-Friendly Fishing Pier (Ohio DNR)"},
    190: {"accessible_restrooms": True, "notes": "ADA-Friendly Restroom (Ohio DNR)"},
    192: {"notes": "ADA-Friendly Courtesy Boat Dock (Ohio DNR)"},
    195: {"notes": "ADA-Friendly Day Lodge (Ohio DNR)"},
    198: {"notes": "ADA-Friendly Boat Ramp (Ohio DNR)"},
    218: {"notes": "ADA-Friendly Paddle Dock (Ohio DNR)"},
    221: {"notes": "ADA-Friendly Fishing Access (Ohio DNR)"},
    236: {"notes": "ADA-Friendly Picnic Area (Ohio DNR)"},
}

GIS_SOURCES: dict[str, dict] = {
    "OH": {
        "base_url": "https://gis2.ohiodnr.gov/arcgis/rest/services/OIT_Services/Ohio_POI/MapServer",
        "layer_id": 0,
        "rule_field": "RuleID",
        "rule_map": RULE_MAP_OHIODNR,
        "park_name_field": "PARK_NAME",
        "radius_m": 1000,  # 1km — OH state-park beaches are wide; tighter risks misses
        "notes": "Ohio DNR Points of Interest. 4 ADA-tagged features (Dillon, Lake Hope, Mohican state parks) as of 2026-06-08.",
    },
    # Future states: add similar entries. Each state's RuleID map differs
    # (state agencies don't share a schema); inspect the target MapServer
    # endpoint and write a RULE_MAP_<state>.
}


# ───────────────────────── ArcGIS query ─────────────────────────

def query_arcgis_features(base_url: str, layer_id: int, rule_field: str,
                          rule_ids: list[int], park_name_field: str) -> list[dict]:
    """Fetch all features matching the rule_ids from the ArcGIS layer.

    Returns list of {rule_id, park_name, lat, lon} dicts. WGS84 output.

    Uses OR-joined where clause; the IN syntax timed out against the
    Ohio DNR endpoint (~60s+ for 8-element IN, vs ~40s for OR-joined).
    """
    where = " OR ".join(f"{rule_field}={r}" for r in rule_ids)
    params = {
        "where": where,
        "outFields": f"{rule_field},{park_name_field}",
        "f": "json",
        "outSR": "4326",
        "returnGeometry": "true",
        "resultRecordCount": "1000",  # ArcGIS default cap
    }
    url = f"{base_url}/{layer_id}/query?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as r:
        data = json.loads(r.read())
    features = []
    for f in data.get("features", []):
        attrs = f.get("attributes", {})
        geom = f.get("geometry", {})
        x, y = geom.get("x"), geom.get("y")
        if x is None or y is None:
            continue
        features.append({
            "rule_id": int(attrs.get(rule_field)),
            "park_name": attrs.get(park_name_field) or "",
            "lon": float(x),
            "lat": float(y),
        })
    return features


# ───────────────────────── Spatial join ─────────────────────────

def spatial_join_to_gold(features: list[dict], state: str, radius_m: int) -> dict[int, list[dict]]:
    """For each feature, find the NEAREST gold beach within radius_m.
    Returns fid → list of matched features (one fid may receive multiple POIs).
    """
    fid_to_features: dict[int, list[dict]] = defaultdict(list)
    with connect() as c, c.cursor() as cur:
        for f in features:
            cur.execute("""
                SELECT fid, name,
                       round(ST_Distance(geom::geography,
                                         ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography)::numeric, 1) AS dist_m
                  FROM public.beaches_gold
                 WHERE state = %s AND is_active
                   AND ST_DWithin(geom::geography,
                                  ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                                  %s)
                 ORDER BY geom::geography <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                 LIMIT 1
            """, (f["lon"], f["lat"], state, f["lon"], f["lat"], radius_m, f["lon"], f["lat"]))
            r = cur.fetchone()
            if r:
                fid_to_features[int(r[0])].append({
                    **f,
                    "matched_fid": int(r[0]),
                    "matched_name": r[1],
                    "distance_m": float(r[2]),
                })
    return fid_to_features


# ───────────────────────── Payload merge ─────────────────────────

def merge_claimed_values(features: list[dict], rule_map: dict) -> dict:
    """Merge multiple POIs for one fid into a single claimed_values dict.
    Booleans union; notes semicolon-concat (dedup)."""
    merged: dict = {}
    note_parts: list[str] = []
    for f in features:
        payload = rule_map.get(f["rule_id"], {})
        for k, v in payload.items():
            if k == "notes":
                if v and v not in note_parts:
                    note_parts.append(v)
            elif isinstance(v, bool):
                merged[k] = bool(merged.get(k)) or v
            elif v is not None:
                merged[k] = v
    if note_parts:
        merged["notes"] = "; ".join(note_parts)
    return merged


# ───────────────────────── Freshness ─────────────────────────

def _state_is_fresh(state_code: str, days: int) -> bool:
    source = f"arcgis_{state_code.lower()}"
    with connect() as c, c.cursor() as cur:
        cur.execute("""
            SELECT EXISTS(
              SELECT 1
                FROM public.beach_enrichment_provenance bep
                JOIN public.beaches_gold g ON g.fid = bep.gold_fid
               WHERE bep.field_group = 'accessibility'
                 AND bep.source = %s
                 AND g.state = %s
                 AND bep.updated_at > now() - (%s || ' days')::interval
            )
        """, (source, state_code, days))
        return bool(cur.fetchone()[0])


# ───────────────────────── Modes ─────────────────────────

def run_preview(state: str, cfg: dict, manifest_path: Path) -> int:
    rule_ids = list(cfg["rule_map"].keys())
    print(f"  state={state}  base={cfg['base_url']}  layer={cfg['layer_id']}")
    print(f"  querying RuleIDs {rule_ids} ...", flush=True)
    t0 = time.time()
    features = query_arcgis_features(cfg["base_url"], cfg["layer_id"],
                                     cfg["rule_field"], rule_ids, cfg["park_name_field"])
    print(f"  ArcGIS returned {len(features)} features in {time.time()-t0:.1f}s", flush=True)
    if not features:
        print("  no features matched. manifest still written for audit.")

    fid_to_features = spatial_join_to_gold(features, state, cfg["radius_m"])
    print(f"  spatial join (radius={cfg['radius_m']}m): {len(fid_to_features)} distinct fids matched", flush=True)

    entries = []
    for fid, fs in sorted(fid_to_features.items()):
        cv = merge_claimed_values(fs, cfg["rule_map"])
        if not cv:
            continue
        entries.append({
            "fid": fid,
            "matched_name": fs[0]["matched_name"],
            "n_pois": len(fs),
            "min_distance_m": min(f["distance_m"] for f in fs),
            "park_names": sorted({f["park_name"] for f in fs if f["park_name"]}),
            "rule_ids": sorted({f["rule_id"] for f in fs}),
            "claimed_values": cv,
        })

    unmatched_features = [f for f in features if not any(
        f["lon"] == ff["lon"] and f["lat"] == ff["lat"]
        for fs in fid_to_features.values() for ff in fs
    )]
    manifest = {
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "state": state,
        "source_url": cfg["base_url"],
        "layer_id": cfg["layer_id"],
        "radius_m": cfg["radius_m"],
        "total_features": len(features),
        "matched_fids": len(entries),
        "unmatched_features": len(unmatched_features),
        "entries": entries,
        "unmatched": [{"rule_id": f["rule_id"], "park_name": f["park_name"],
                       "lat": f["lat"], "lon": f["lon"]} for f in unmatched_features],
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\n  manifest written: {manifest_path}")
    print(f"  matched fids: {len(entries)} | unmatched POIs: {len(unmatched_features)}")
    for e in entries:
        print(f"    fid={e['fid']:7d}  dist={e['min_distance_m']:6.1f}m  parks={e['park_names']!r}  rules={e['rule_ids']}  -> name={e['matched_name']!r}")
    if unmatched_features:
        print(f"\n  Unmatched POIs (no gold beach within {cfg['radius_m']}m):")
        for f in unmatched_features:
            print(f"    rule={f['rule_id']:4d}  park={f['park_name']!r:30s}  ({f['lat']:.4f}, {f['lon']:.4f})")
    return 0


def run_apply(state: str, cfg: dict, manifest_path: Path) -> int:
    if not manifest_path.exists():
        sys.exit(f"ERROR: manifest not found at {manifest_path}. Run preview first.")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    source = f"arcgis_{state.lower()}"
    source_url = cfg["base_url"]
    entries = manifest.get("entries", [])

    print(f"  applying source={source!r}  {len(entries)} fids")

    inserted = updated = 0
    with connect() as c, c.cursor() as cur:
        for e in entries:
            cur.execute("""
                INSERT INTO public.beach_enrichment_provenance
                  (gold_fid, field_group, source, source_url, claimed_values,
                   confidence, is_canonical, extraction_type, updated_at)
                VALUES (%s, 'accessibility', %s, %s, %s::jsonb,
                        0.90, false, 'derived_url_crawl', now())
                ON CONFLICT (gold_fid, field_group, source) DO UPDATE
                  SET claimed_values = excluded.claimed_values,
                      confidence     = excluded.confidence,
                      source_url     = excluded.source_url,
                      updated_at     = now()
                  WHERE beach_enrichment_provenance.claimed_values IS DISTINCT FROM excluded.claimed_values
                RETURNING (xmax = 0) AS inserted
            """, (e["fid"], source, source_url, json.dumps(e["claimed_values"])))
            r = cur.fetchone()
            if r is None: continue
            if r[0]: inserted += 1
            else: updated += 1
        c.commit()
    print(f"  BEP done. inserted={inserted} updated={updated}")

    # Fire resolver + promote sweep per [[non-dogs-bep-needs-manual-resolver]]
    print("  Firing resolver + promote sweep ...")
    with connect() as c, c.cursor() as cur:
        cur.execute("SELECT public._resolve_field_group_gold('accessibility', NULL)")
        flipped = cur.fetchone()[0]
        cur.execute("SELECT rows_inserted, rows_updated FROM public.promote_canonical_accessibility_to_beach_amenities(NULL)")
        ins, upd = cur.fetchone()
        c.commit()
    print(f"  resolver: {flipped} canonical | promote: ins={ins} upd={upd}")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True, help="state code; must be in GIS_SOURCES")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--manifest", type=Path, default=None,
                    help="manifest path override (default: tmp/<state>_arcgis_matches.json)")
    ap.add_argument("--skip-if-fresh-within", type=int, default=0, metavar="DAYS",
                    help="skip if any arcgis_<state> BEP row updated within N days")
    args = ap.parse_args()

    state = args.state.upper()
    cfg = GIS_SOURCES.get(state)
    if not cfg:
        print(f"  [{state}] no GIS_SOURCES entry — skip")
        return 0
    manifest_path = args.manifest or (TMP_DIR / f"{state.lower()}_arcgis_matches.json")

    if args.skip_if_fresh_within > 0 and _state_is_fresh(state, args.skip_if_fresh_within):
        print(f"  [{state}] SKIP — arcgis_{state.lower()} BEP rows fresh within {args.skip_if_fresh_within}d")
        return 0

    if args.apply:
        return run_apply(state, cfg, manifest_path)
    return run_preview(state, cfg, manifest_path)


if __name__ == "__main__":
    sys.exit(main())
