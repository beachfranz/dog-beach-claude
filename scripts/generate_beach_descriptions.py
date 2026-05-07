"""Generate warm, factual beach descriptions via Sonnet for beach.html / detail.html.

Per beach:
  1. Pull zones (zone_rules), CPAD parent unit, beach metadata from DB
  2. Hit Overpass for verified physical features (cliff/pier/jetty/breakwater/
     groyne/lighthouse/marina/stream/river) within 300m of centroid
  3. Build the input bundle, hash it, skip if cached
  4. Call Sonnet with the integrated activity+environment prompt
  5. Upsert into beach_descriptions

Usage:
  python scripts/generate_beach_descriptions.py --fids 6212,6202,8337,218
  python scripts/generate_beach_descriptions.py --pilot 10
  python scripts/generate_beach_descriptions.py --full
  python scripts/generate_beach_descriptions.py --refresh --fids 6212  # force regen
  python scripts/generate_beach_descriptions.py --dry-run --pilot 5    # build inputs only

Caching: input_hash on beach_descriptions row; we skip when current hash matches
(input bundle hasn't changed). Pass --refresh to force regen regardless.

Cost: ~$0.003/beach via Sonnet 4.5. ~$2.40 to backfill ~800 active beaches.
"""

from __future__ import annotations
import argparse
import hashlib
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
ENV  = ROOT / "scripts" / "pipeline" / ".env"
load_dotenv(ENV)

SUPABASE_URL    = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY     = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_KEY   = os.environ["ANTHROPIC_API_KEY"]
MODEL           = "claude-sonnet-4-5-20250929"
OVERPASS_URL    = "https://overpass-api.de/api/interpreter"
OVERPASS_DELAY  = 3.0     # seconds between Overpass requests (be polite)
OVERPASS_RADIUS = 300     # meters around beach centroid

PROMPT = """You write 2-3 sentence beach descriptions for a dog-owner-focused app.

The reader is planning a visit with their dog. They want: what can my dog and I do here, and what's the place like?

REQUIRED:
1. Lead with the activities derived from `zones` -> `sections`. Map sections to actions:
   - sand off-leash -> "let your dog play off-leash on the sand"
   - sand on-leash -> "walk your dog along the beach"
   - water_swim -> "splash in the water" / "let them swim"
   - trails -> "take a walk on the trails"
   - picnic_area on_leash -> "share a picnic with your pooch"
   - sections marked NOT ALLOWED -> "stay clear of [section]"
2. If `verified_physical_features` is non-empty, weave the facts in naturally — open or close with a short clause (e.g. "at the mouth of Aliso Creek", "backed by coastal bluffs"). Don't make it its own sentence unless natural.
3. Mention time-windows / seasonal restrictions concretely when present.
4. Mention parking ONCE somewhere natural in the prose, using `parking.type` and `parking.cost` when set:
   - type=lot, cost=free  -> "with free parking"
   - type=lot, cost=paid  -> "with paid parking" / "for a small fee at the lot"
   - type=street, cost=free -> "with free street parking"
   - type=street, cost=paid -> "with metered street parking"
   - type=street, cost=mixed -> "with limited metered and free street parking"
   - type=mixed, cost=paid -> "with paid lot and street parking"
   - type set, cost=null -> just describe type ("with a parking lot", "with street parking")
   - parking.type=null -> skip parking entirely
5. Use 2nd-person imperative voice ("Let your dog...", "Enjoy a walk...", "Pack a lunch").

VERIFIED PHYSICAL FEATURE -> CLAUSE:
- natural=cliff (any count) -> "backed by coastal bluffs"
- waterway=stream/river named -> "at the mouth of {name}"
- waterway=stream/river unnamed -> "near a creek mouth"
- man_made=pier named -> "next to {name}"
- man_made=breakwater/jetty/groyne -> "tucked beside a {kind}"
- leisure=marina -> "harbor-adjacent"

FORBIDDEN:
- Generic beach imagery not specific to THIS beach ("rolling waves", "salty breeze").
- Inventing physical features not in `verified_physical_features`. If empty, don't describe terrain at all — just lead with activities.
- Superlatives ("best", "famous", "treasured", "beloved", "gem", "pristine").
- Crowd / popularity claims.
- Restating amenity flags as a list inventory.

VOICE: warm, second-person imperative, activity-led. 2-3 sentences. Output ONLY the description.

INPUTS
%s
"""


# ─── Supabase REST helpers ────────────────────────────────────────────────

def supa(path: str, *, method: str = "GET", body: dict | None = None,
         params: dict | None = None) -> object:
    """Hit Supabase REST/RPC with the service key."""
    qs = ("?" + urllib.parse.urlencode(params)) if params else ""
    req = urllib.request.Request(
        f"{SUPABASE_URL}{path}{qs}", method=method,
        data=(json.dumps(body).encode() if body is not None else None),
        headers={
            "apikey": SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read()
        if not raw:
            return None
        return json.loads(raw)


def select_targets(args) -> list[int]:
    if args.fids:
        return [int(s) for s in args.fids.split(",")]
    if args.pilot:
        rows = supa("/rest/v1/beaches_gold",
                    params={"select": "fid",
                            "is_active": "eq.true",
                            "is_scoreable": "eq.true",
                            "order": "fid.asc",
                            "limit": str(int(args.pilot))})
        return [r["fid"] for r in rows]
    if args.full:
        rows = supa("/rest/v1/beaches_gold",
                    params={"select": "fid",
                            "is_active": "eq.true",
                            "is_scoreable": "eq.true",
                            "order": "fid.asc"})
        return [r["fid"] for r in rows]
    print("ERROR: provide --fids, --pilot N, or --full", file=sys.stderr)
    sys.exit(1)


# ─── Input-bundle assembly ────────────────────────────────────────────────

def fetch_zones_summary(zone_rules: dict) -> list[dict]:
    """Compress the verbose zone_rules into a small summary the LLM can read."""
    if not zone_rules: return []
    seasons = (zone_rules.get("seasons") or
               [{"name": None, "regions": zone_rules.get("regions") or []}])
    out = []
    for s in seasons:
        for reg in (s.get("regions") or []):
            sections = reg.get("sections") or {}
            if not sections: continue
            sec_summary = {}
            for name, sec in sections.items():
                rule = sec.get("rule") or "unknown"
                tw = sec.get("time_windows") or []
                if tw:
                    win_strs = []
                    for w in tw:
                        win_strs.append(f"{w.get('start')}-{w.get('end')}: {w.get('rule', rule)}")
                    sec_summary[name] = f"{rule} (time-windows: {', '.join(win_strs)})"
                else:
                    sec_summary[name] = rule
            out.append({
                "season": s.get("name") or "All year",
                "zone":   reg.get("name") or "Whole beach",
                "sections": sec_summary,
            })
    return out


def fetch_overpass_features(lat: float, lng: float) -> list[dict]:
    """Pull verified physical features within 300m of the centroid."""
    q = (
        f'[out:json][timeout:30];'
        f'('
        f'way(around:{OVERPASS_RADIUS},{lat},{lng})["natural"~"^(cliff|reef|peninsula|cape|bay)$"];'
        f'way(around:{OVERPASS_RADIUS},{lat},{lng})["man_made"~"^(pier|jetty|breakwater|groyne|lighthouse)$"];'
        f'node(around:{OVERPASS_RADIUS},{lat},{lng})["man_made"~"^(pier|jetty|lighthouse)$"];'
        f'way(around:{OVERPASS_RADIUS},{lat},{lng})["waterway"~"^(stream|river)$"];'
        f'way(around:{OVERPASS_RADIUS},{lat},{lng})["leisure"="marina"];'
        f');out tags;'
    )
    data = urllib.parse.urlencode({"data": q}).encode()
    req = urllib.request.Request(OVERPASS_URL, method="POST",
        data=data, headers={"User-Agent": "DogBeachScout/1.0 (franz@franzfunk.com)"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            resp = json.loads(r.read())
    except Exception as e:
        print(f"  overpass error: {e}", file=sys.stderr)
        return []
    feats = []
    for e in resp.get("elements", []):
        t = e.get("tags") or {}
        kind = (t.get("natural") or t.get("man_made")
                or t.get("waterway") or t.get("leisure"))
        if not kind or kind == "coastline": continue
        feats.append({"kind": kind, "name": t.get("name") or None})
    return feats


def build_inputs(fid: int) -> dict | None:
    """Assemble the full input bundle for one beach."""
    rows = supa("/rest/v1/beaches_gold", params={
        "select": "fid,location_id,name,display_name_override,county_name,state,group_id,cpad_unit_id,geom",
        "fid": f"eq.{fid}", "is_active": "eq.true", "limit": "1",
    })
    if not rows:
        return None
    g = rows[0]

    bdp_rows = supa("/rest/v1/beach_dog_policy", params={
        "select": "zone_rules,dogs_allowed",
        "arena_group_id": f"eq.{g['group_id']}", "limit": "1",
    })
    bdp = bdp_rows[0] if bdp_rows else {}

    # Parking signal from beach_amenities. parking_cost is not yet in
    # the schema (queued as a future extraction pass) so we only have
    # parking_type. cost stays null until we add it.
    amen_rows = supa("/rest/v1/beach_amenities", params={
        "select": "parking_type,parking_notes",
        "arena_group_id": f"eq.{g['group_id']}", "limit": "1",
    })
    amen = amen_rows[0] if amen_rows else {}
    # Treat 'metered' parking_type as paid; otherwise leave cost null
    p_type = amen.get("parking_type")
    parking = None
    if p_type:
        parking = {
            "type": p_type,
            "cost": "paid" if p_type == "metered" else None,
        }

    cpad_unit = None
    if g.get("cpad_unit_id"):
        cpad_rows = supa("/rest/v1/cpad_units", params={
            "select": "unit_name", "unit_id": f"eq.{g['cpad_unit_id']}", "limit": "1",
        })
        if cpad_rows:
            cpad_unit = {"name": cpad_rows[0]["unit_name"]}

    # Fetch lat/lng via the get_beach_info RPC (it computes from geom).
    info = supa("/rest/v1/rpc/get_beach_info", method="POST", body={"p_fid": fid})
    lat = info.get("beach", {}).get("lat") if info else None
    lng = info.get("beach", {}).get("lng") if info else None

    physical = []
    if lat is not None and lng is not None:
        physical = fetch_overpass_features(lat, lng)
        time.sleep(OVERPASS_DELAY)

    # cpad_unit is intentionally NOT in the prompt right now — CPAD links
    # are geographic-overlap based and conflict with operator reality on
    # carve-out beaches (e.g., HB Dog Beach falls inside Bolsa Chica State
    # Beach's polygon but is City-managed, not CDPR-managed). Re-introduce
    # once we have an operator-reconciled "parent unit" attribution.
    _ = cpad_unit  # kept for audit, not sent
    return {
        "name": g.get("display_name_override") or g["name"],
        "location": ", ".join(filter(None, [g.get("county_name"), g.get("state")])),
        "zones": fetch_zones_summary(bdp.get("zone_rules") or {}),
        "parking": parking,
        "verified_physical_features": physical,
    }


def input_hash(inputs: dict) -> str:
    return hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()[:16]


# ─── Sonnet call ──────────────────────────────────────────────────────────

def call_sonnet(inputs: dict) -> tuple[str, dict]:
    body = {
        "model": MODEL, "max_tokens": 350,
        "messages": [{"role": "user",
                      "content": PROMPT % json.dumps(inputs, indent=2)}],
    }
    req = urllib.request.Request("https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode(), method="POST",
        headers={"x-api-key": ANTHROPIC_KEY,
                 "anthropic-version": "2023-06-01",
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        resp = json.loads(r.read())
    text = "".join(p["text"] for p in resp["content"] if p["type"] == "text").strip()
    return text, resp.get("usage", {})


# ─── Cache + write ────────────────────────────────────────────────────────

def get_existing(fid: int) -> dict | None:
    rows = supa("/rest/v1/beach_descriptions",
                params={"arena_group_id": f"eq.{fid}", "limit": "1",
                        "select": "input_hash,source"})
    return rows[0] if rows else None


def upsert_description(fid: int, description: str, ihash: str) -> None:
    body = [{
        "arena_group_id": fid,
        "description":    description,
        "source":         "sonnet",
        "model":          MODEL,
        "input_hash":     ihash,
        "generated_at":   "now()",   # PostgREST resolves
        "updated_at":     "now()",
    }]
    # PostgREST upsert via Prefer: resolution=merge-duplicates
    req = urllib.request.Request(
        f"{SUPABASE_URL}/rest/v1/beach_descriptions",
        method="POST", data=json.dumps(body).encode(),
        headers={
            "apikey": SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
        },
    )
    urllib.request.urlopen(req, timeout=30).close()


# ─── Main ─────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--fids",  help="comma-separated list of fids")
    grp.add_argument("--pilot", type=int, help="run on first N beaches")
    grp.add_argument("--full",  action="store_true", help="run on all active scoreable beaches")
    ap.add_argument("--refresh", action="store_true",
                    help="regenerate even when input_hash matches cache")
    ap.add_argument("--dry-run", action="store_true",
                    help="build inputs and print, don't call Sonnet")
    args = ap.parse_args()

    fids = select_targets(args)
    print(f"Targets: {len(fids)} beaches  (cost ~${len(fids)*0.003:.2f} via {MODEL})")

    cost_in = cost_out = 0
    cache_hits = generated = errors = 0

    for i, fid in enumerate(fids, 1):
        try:
            inputs = build_inputs(fid)
            if not inputs:
                print(f"  [{i}/{len(fids)}] fid={fid}  not found, skip")
                errors += 1
                continue

            ih = input_hash(inputs)
            cached = get_existing(fid)
            if cached and cached.get("input_hash") == ih and not args.refresh:
                cache_hits += 1
                print(f"  [{i}/{len(fids)}] fid={fid}  {inputs['name'][:40]:40s}  cache-hit")
                continue
            if cached and cached.get("source") == "manual":
                print(f"  [{i}/{len(fids)}] fid={fid}  {inputs['name'][:40]:40s}  manual-protected, skip")
                continue

            if args.dry_run:
                print(f"  [{i}/{len(fids)}] fid={fid}  {inputs['name'][:40]:40s}")
                print(f"    inputs: {json.dumps(inputs, indent=2)[:400]}...")
                continue

            text, usage = call_sonnet(inputs)
            cost_in  += usage.get("input_tokens", 0)
            cost_out += usage.get("output_tokens", 0)

            upsert_description(fid, text, ih)
            generated += 1
            print(f"  [{i}/{len(fids)}] fid={fid}  {inputs['name'][:40]:40s}  ok")
            print(f"      {text}")
        except Exception as e:
            print(f"  [{i}/{len(fids)}] fid={fid}  ERROR: {e}", file=sys.stderr)
            errors += 1

    cost = cost_in * 3 / 1e6 + cost_out * 15 / 1e6
    print(f"\n=== TOTALS ===")
    print(f"  generated:   {generated}")
    print(f"  cache hits:  {cache_hits}")
    print(f"  errors:      {errors}")
    print(f"  tokens:      {cost_in} in / {cost_out} out")
    print(f"  est cost:    ${cost:.3f}")


if __name__ == "__main__":
    sys.exit(main())
