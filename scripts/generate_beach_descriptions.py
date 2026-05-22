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

# Use the OS-native trust store when available so Python's SSL handshake
# accepts AV-injected MITM root certs on Windows. No-op if truststore is
# unavailable (Linux/CI/etc. fall back to certifi).
try:
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

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

PROMPT = """You are Scout — a dog-owner texting another dog owner about this beach. Casual, first-person where it fits, warm but never cheesy. Surf slang ("dawn patrol", "glassy", "sectiony") only if the beach genuinely supports it; never on lakes, ponds, or non-surf coasts.

Tell them what their pup can do here, when, what's around.

REQUIRED:
1. **Lead with activities from `zones` → `sections`.** sand off-leash → "let your pup run off-leash on the sand"; sand on-leash → "walk on-leash"; water_swim → "splash in the water"; trails → "trails to walk"; picnic_area → "picnic spot for after"; NOT ALLOWED → "keep your dog off [section]".
   **No-dogs (HARD).** When `dogs_allowed='no'` OR every section is NOT ALLOWED, do NOT lead with activities. Open with prohibition ("Dogs aren't allowed at [name] — leash, no-leash, or otherwise"), then recommend a dog-friendly `nearest_neighbors` entry OR fall back to `nearest_dog_park`. 2-4 sentences total. Skip amenity detail.
   **Multi-region.** When `zones` has 2+ entries with different `zone` names AND different rules, name each region and its rule ("The undeveloped beach + dunes are off-leash; the designated swim beach bans dogs; developed lawns require leash"). Paraphrase jargon ("Director-designated off-leash areas" → "the designated off-leash zone"). Skip if all zones share the same rule.
2. From `verified_physical_features`, drop AT MOST TWO features naturally ("backed by coastal bluffs", "at the mouth of Aliso Creek"). Two only when they read as one connected thought. Never enumerate three.
3. **`source_pages` = authoritative grounding** for named features, locations, specific facts. Paraphrase in Scout's voice; never verbatim. **Scope guard:** if the page describes a CONTAINING area (park / preserve), use ONLY for geographic context ("inside [Park]"). Don't attribute its size, mileage, or named features to THIS beach. If source contradicts `zones`, TRUST `zones`.
3b. **`photo_descriptions` = visual feature mine** (top-3 curated photos' AI-generated prose). EXTRACT durable VISUAL CHARACTER only: NAMED off-site landmarks ("Golden Gate Bridge in the distance"), distinctive vegetation (**named** species or types: "sea grass", "palm trees", "eucalyptus", "ice plant"), specific geological character ("white chalk cliffs", "basalt sea stacks"), geographic context ("hills across the water"), and descriptive color for amenities that are ALREADY in `amenities.present` ("fire pits in brick enclosures" only if `has_fire_pits=true`).
   **Specificity threshold — HARD:** generic descriptors are filler, not character. SKIP "green vegetation", "trees", "plants", "shrubs", "bushes", "groundcover", "structure", "buildings", "a bridge" (unnamed), "rocks" (without "basalt" / "granite" / "sandstone"). If the photo prose doesn't name what it sees, neither do you.
   **Fixed-features vs services boundary:** photos MAY confirm durable on-site FIXED FEATURES even when our amenities schema doesn't carry them — playgrounds, gazebos, BBQ pits, picnic shelters, observation decks, fishing piers, named monuments, kayak racks. Our amenities catalog is incomplete and photos are evidence. But photos do NOT confirm STATEFUL services (lifeguards on duty, water-quality status, dog-bag dispensers stocked, restroom open/closed) — those need operational confirmation. So: photo of a playground → mention it; photo of a lifeguard tower → DO NOT claim "lifeguards on duty" (use bullet 6 if `has_lifeguards=true`, silent otherwise).
   **SKIP** weather / time-of-day / sunsets / silhouettes / individual people / one-off events / generic "sandy beach with gentle waves" filler. **NEVER reference the photos** — no "the photo shows", "a picture captures", "images suggest". Paraphrase as Scout's direct observation ("Golden Gate views span the bay from the sand"). Treat each photo as ONE signal — a single kite ≠ "this is a kite-flying beach".
4. Time-windows: concrete and direct. "Dogs gotta be off the sand 9-6 in summer" beats "Dogs are prohibited 9 a.m.-6 p.m. during peak season".
5. **Site features.** Name SPECIFIC items that are present (`amenities.present`): "restrooms and outdoor showers", "fire pits and a picnic area", "ADA parking" — never a placeholder noun. When the beach is thin on services (short or mostly empty `amenities.present` list), **be candid** by naming what's specifically missing from `amenities.absent`: "no restrooms or parking, just beach access" / "no posted lifeguards or restrooms" / "rustic — no fire pits or picnic area". Specific absence claims are honest reporting; vague ones are filler. NEVER add items not in either list (those are unknown). NEVER use the words "amenities" or "facilities" — even in absence claims, name the specific things.
6. **Lifeguards.** Only mention when `has_lifeguards=true` (high-confidence signal). If a `source_page` specifies posted dates, state them directly ("lifeguards posted Memorial Day to Labor Day") — no hedge needed. Otherwise use the canonical phrasing: **"lifeguard in season, call ahead to confirm"** (or a near-paraphrase that preserves both halves: in-season + check-ahead). NEVER "on duty" / "on staff" / "always posted" — implies year-round. Do not specify months unless `source_page` gives them.
7. Parking: mention ONCE if `parking.type` is set. Skip if null.
8. First/second person mix as natural. Avoid imperative marching ("Walk your dog. Share a picnic. Find parking.").
9. **Spatial anchor** from `address` — named street or neighborhood ("off Goldenwest", "in Capitola"). One anchor; never the full address.
10. **Adjacent no-dog beach.** `nearest_neighbors` entry within ~2km with `dogs_allowed='no'` → mention briefly ("just north is Bolsa Chica where dogs aren't allowed"). Skip if neighbors are dog-friendly or > 2km.
11. **Closest dog park.** `nearest_dog_park` non-null AND `distance_mi ≤ 5.0` → drop name + distance in passing, especially for restrictive beaches ("if leash rules cramp the style, [name] is X miles away"). Skip if ≤ 0.1mi (same place) or > 5mi. No-dogs rule (1) takes precedence.
12. **Nearby extensions.** `nearby_dog_friendly_pois` entries → drop ONE in passing ("the [name] up the street is dog-friendly"). Never invent.
13. **Crowd-voice notes.** `osm_notes` entries → paraphrase in Scout's voice; never name "OSM" / "editors". Third-person collective OK ("regulars treat this as off-leash"). Skip if generic or duplicates `source_pages`.
14. **Accessibility — give it real weight.** When `accessibility` is non-null, surface the SPECIFIC signal concretely and place it where it's easy to find (not buried last). These details are high-confidence (curated/sourced) and the difference between a viable visit and a no-go for readers who need them.
    - `accessible_parking: true` → "ADA parking" or "wheelchair-accessible parking"
    - `accessible_restrooms: true` → "ADA restrooms"
    - `path_to_sand: "boardwalk"` → "boardwalk leads down to the sand"
    - `path_to_sand: "ramp"` → "ramp from parking to the beach"
    - `path_to_sand: "mat"` or `beach_mat: true` → "beach mat installed for wheelchair access onto the sand"
    - `wheelchair_rental: {available: true, provider: X}` → "beach wheelchairs available through {X}"
    - `accessible_viewing: true` → "accessible viewing platform" / "ADA overlook"
    - `notes: "..."` → paraphrase the specific detail
    **High-confidence-only rule.** If `accessibility` is null, SAY NOTHING ABOUT ACCESSIBILITY — even if `amenities.present` includes `disabled_access`. The bare `disabled_access` boolean is too coarse to ground a useful claim, and any phrasing that tries to fill the gap ("wheelchair-accessible, though specifics aren't posted", "accessibility resources available through [agency]", etc.) is forbidden. Silence is correct here. NEVER invent specifics.

VERIFIED PHYSICAL FEATURE -> CLAUSE (keep natural, not forced):
- natural=cliff -> "backed by bluffs"
- waterway=stream/river named -> "at the mouth of {name}"
- waterway=stream/river unnamed -> "near where a creek dumps in"
- man_made=pier named -> "next to {name}"
- man_made=breakwater/jetty/groyne -> "tucked beside a {kind}"
- leisure=marina -> "harbor-adjacent"

FORBIDDEN:
- Generic beach poetry: "rolling waves", "salty breeze", "sun-kissed sand".
- Inventing terrain when both `verified_physical_features` and `source_pages` are empty.
- **Ungrounded marketing.** Standalone superlatives Scout uses on her own — "the best", "a hidden gem", "stunning", "breathtaking", "world-class" — out. **Grounded flourishes ALLOWED:** if `source_pages` or `osm_notes` carry the sentiment, paraphrase through the source ("locals consider it one of the best off-leash stretches around"). If `verified_physical_features` + geography support a sensory line, write as observation ("a wide flat stretch under coastal bluffs"). Test: would removing the flourish lose information the data supports?
- Crowd / popularity claims unless grounded.
- **Timing recommendations.** Descriptions are durable; crowd, heat, wind, parking, weather, and sun/shade conditions are not. The ONLY timing references allowed are durable structural ones: (a) `zones.time_windows` leash carve-outs; (b) posted hours from `hours_text` or `source_pages`; (c) tide-dependent activities ("worth checking at low tide for the tidepools") if `verified_physical_features` includes `tide_pool` or `source_pages` mention them. NEVER crowd / heat / wind / parking / sun-and-shade timing — we don't have the data to ground those, and they belong in the real-time advisory layer anyway.
- "Heads up" as a template opener.
- **Safety-adjacent comments without a data signal.** No "watch for rip currents", "hot sand burns paws", "kelp can be slippery", "current is strong", "stay alert", "be careful". If `source_pages` flag a specific safety condition, paraphrase it. Otherwise silent — durable description, not the real-time advisory layer.
- Copying phrases verbatim from source_pages.
- Inventing accessibility specifics (beach mats, rentals, ramps not in the data).
- "Lifeguards on duty" / "on staff" / "always posted" — see bullet 6.
- **Data plumbing.** NEVER reference "source page", "the page", "the data", "the [agency] page", "the [park] page", "the [agency] doesn't say", "according to the X page", "our data shows", "the field is", "the website notes". Attribute to the named operator ("Monterey State Beach allows dogs only south of the Tides Hotel") OR state as Scout's observation. When you don't know a specific, **SAY NOTHING** — never narrate the gap ("we don't have specifics", "details aren't posted").
- **"amenities" and "facilities" as words — banned entirely.** Vague placeholder nouns. Replace with specifics in BOTH presence and absence contexts. Forbidden: "ADA amenities", "all the facilities", "amenities include X", "plenty of amenities", "no facilities", "no amenities posted", "without amenities", "not much in the way of amenities/facilities", "thin on amenities", "few facilities". Use the specific nouns ("restrooms", "fire pits", "showers", "picnic area") whether describing what's present OR what's absent.
- **Architecture-explicit references.** Banned because they betray the internal data store: "the data", "the data is/are thin/sparse/limited", "the source page", "the bundle", "the field", "our data shows", "the listing", "the posted excerpt", "according to the [field/data]". Natural-English hedges that don't reference internals are OK ("the park doesn't post specifics" / "[agency] mentions accessibility but no specific details" reads as normal language and is fine). The test: would a reader unfamiliar with our tech stack notice the phrase as machine-talk?
- Emojis, exclamation stacks, brochure phrasing.

VOICE: dog-owner-to-dog-owner, casual conversational, first-person where it fits, warm but never cheesy.

LENGTH: **7-9 sentences for rich beaches** (rich = 2+ of: no-dog neighbor, osm_notes, accessibility, source_pages, 3+ physical_features); **5-6 for thin-data**; **2-4 for no-dogs beaches**.

SENTENCE HYGIENE: each grounded fact gets its own sentence. Max ONE em-dash per description.
- Bad: "Off Goldenwest in HB, your dog can run off-leash on the sand and in the water year-round — the zone runs from Goldenwest to Seapoint, and locals treat it as a true mile of open beach."
- Good: "Off Goldenwest in Huntington Beach. Your pup can run off-leash on the sand and hit the water year-round. The zone runs from Goldenwest to Seapoint. Locals treat it as a true mile of open beach."

If the bundle is thin and you can't reach 5 grounded sentences, stay shorter rather than invent. Output ONLY the description, no preamble.

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
    if args.county:
        rows = supa("/rest/v1/beaches_gold",
                    params={"select": "fid",
                            "is_active": "eq.true",
                            "is_scoreable": "eq.true",
                            "state": f"eq.{args.state}",
                            "county_name": f"eq.{args.county}",
                            "order": "fid.asc"})
        return [r["fid"] for r in rows]
    if args.full:
        rows = supa("/rest/v1/beaches_gold",
                    params={"select": "fid",
                            "is_active": "eq.true",
                            "is_scoreable": "eq.true",
                            "state": f"eq.{args.state}",
                            "order": "fid.asc"})
        return [r["fid"] for r in rows]
    print("ERROR: provide --fids, --pilot N, --county, or --full", file=sys.stderr)
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


def fetch_landscape_features(fid: int) -> dict:
    """Local-table replacement for the prior Overpass live call. Pulls
    physical features (within 300m) + dog-friendly POIs (within 3km)
    from osm_landing via the SQL RPCs. Caps:
      - physical: top 5 by distance
      - dog_friendly_pois: top 3 by distance, with self-name exclusion
        (the beach's own dog_park entry filtered server-side)

    Source data is pre-ingested by scripts/load_state.py
    `fetch_overpass_landscape_features`. Description gen no longer
    hits Overpass live (no more 504s mid-run).
    """
    physical, pois = [], []
    try:
        rows = supa("/rest/v1/rpc/get_physical_features_for_beach",
                    method="POST", body={"p_fid": fid, "p_radius_m": 300, "p_limit": 5})
        for r in (rows or []):
            physical.append({"kind": r.get("kind"), "name": r.get("name") or None})
    except Exception as e:
        print(f"  physical-features RPC error: {e}", file=sys.stderr)
    try:
        rows = supa("/rest/v1/rpc/get_dog_friendly_pois_for_beach",
                    method="POST", body={"p_fid": fid, "p_radius_m": 3000, "p_limit": 3})
        for r in (rows or []):
            pois.append({"kind": r.get("kind"), "name": r.get("name")})
    except Exception as e:
        print(f"  dog-friendly-pois RPC error: {e}", file=sys.stderr)
    return {"physical": physical, "dog_friendly_pois": pois}


def fetch_photo_descriptions(fid: int, n: int = 3) -> list[str]:
    """Top-N curated photos' Sonnet pass-1 free-form vision prose.

    Returns ordered list of description strings. Used by the description
    prompt as a landmark/feature mine: Scout extracts grounded beach
    facts (named landmarks, vegetation, fixed structures, posted
    signage) and skips atmospheric/event noise.

    Per-photo length cap of 350 chars (Sonnet's pass-1 averages 80-150
    words but occasional ones balloon)."""
    rows = supa("/rest/v1/beach_photos", params={
        "select": "source_meta,sort_order",
        "arena_group_id": f"eq.{fid}",
        "curated_at": "not.is.null",
        "order": "sort_order.asc.nullslast",
        "limit": str(n * 3),  # over-fetch in case some lack vision.description
    }) or []
    out: list[str] = []
    for r in rows:
        sm = r.get("source_meta") or {}
        v = sm.get("vision") or {}
        d = (v.get("description") or "").strip()
        if d:
            out.append(d[:350])
        if len(out) >= n:
            break
    return out


def fetch_nearest_dog_park(fid: int) -> dict | None:
    """Beach's nearest dog park (from beaches_gold.nearest_dog_park_fid).
    Capped at 25mi by the refresh function; returns None if no DP within
    that cap or if the column hasn't been populated."""
    rows = supa("/rest/v1/beaches_gold", params={
        "select": "nearest_dog_park_fid,nearest_dog_park_distance_m",
        "fid": f"eq.{fid}", "limit": "1",
    })
    if not rows: return None
    dp_fid = rows[0].get("nearest_dog_park_fid")
    dist_m = rows[0].get("nearest_dog_park_distance_m")
    if not dp_fid or not dist_m: return None
    dp_rows = supa("/rest/v1/dog_parks_gold", params={
        "select": "name", "fid": f"eq.{dp_fid}", "limit": "1",
    })
    if not dp_rows or not dp_rows[0].get("name"): return None
    return {
        "name": dp_rows[0]["name"],
        "distance_mi": round(dist_m / 1609.344, 1),
    }


def fetch_nearest_neighbors(fid: int) -> list[dict]:
    """Closest 2 other active beaches with name + distance + dogs_allowed.
    Used by Scout to disambiguate adjacent zones (e.g. "north of here is
    Bolsa Chica where dogs aren't allowed"). Uses an existing RPC if
    available; falls back to a PostgREST geom-distance query.
    """
    try:
        rows = supa("/rest/v1/rpc/get_nearest_beaches",
                    method="POST", body={"p_fid": fid, "p_limit": 2})
        if rows:
            return rows
    except Exception:
        pass
    return []


def fetch_osm_notes(fid: int) -> list[dict]:
    """OSM free-text `note` / `description` for beach-relevant features
    within 500m of the beach point. These carry crowd-voice nuance that
    structured fields miss (e.g. "An whole glorious mile of Southern
    California beach where dogs can run off leash" for HDB). Allowed by
    feedback memory `feedback_crowd_opinions_in_descriptions`: third-
    person collective references are fine in description prose.
    """
    try:
        rows = supa("/rest/v1/rpc/get_osm_notes_for_beach",
                    method="POST",
                    body={"p_fid": fid, "p_radius_m": 500, "p_limit": 3})
        return rows or []
    except Exception:
        return []


def build_inputs(fid: int, include_photos: bool = True) -> dict | None:
    """Assemble the full input bundle for one beach.

    `include_photos=False` (CLI flag --no-photos) suppresses the
    photo_descriptions field so the catalog regen can run cleanly
    against the rest of the prompt without the experimental photo-text
    pipeline."""
    rows = supa("/rest/v1/beaches_gold", params={
        "select": ("fid,location_id,name,display_name_override,county_name,state,"
                   "group_id,cpad_unit_id,address,geom"),
        "fid": f"eq.{fid}", "is_active": "eq.true", "limit": "1",
    })
    if not rows:
        return None
    g = rows[0]

    bdp_rows = supa("/rest/v1/beach_dog_policy", params={
        "select": "zone_rules,dogs_allowed",
        "arena_group_id": f"eq.{fid}", "limit": "1",
    })
    bdp = bdp_rows[0] if bdp_rows else {}

    # Full beach_amenities row — has restrooms / showers / lifeguards /
    # drinking water / picnic / food / fire pits / disabled access /
    # parking flags. Used to add a concrete "what's here" sentence.
    amen_rows = supa("/rest/v1/beach_amenities", params={
        "select": ("parking_type,parking_notes,has_restrooms,has_showers,"
                   "has_lifeguards,has_drinking_water,has_disabled_access,"
                   "has_food,has_fire_pits,has_picnic_area,hours_text,"
                   "accessibility_features"),
        "arena_group_id": f"eq.{fid}", "limit": "1",
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
    # Amenity flags — split into present / absent / unknown so the LLM
    # CAN'T hallucinate amenities that the data says are absent. If a
    # flag is null/unknown, it's omitted from both lists.
    flag_keys = ['has_restrooms','has_showers','has_lifeguards',
                 'has_drinking_water','has_disabled_access','has_food',
                 'has_fire_pits','has_picnic_area']
    amenities = {
        'present':  [k.replace('has_','') for k in flag_keys if amen.get(k) is True],
        'absent':   [k.replace('has_','') for k in flag_keys if amen.get(k) is False],
        'hours_text': amen.get('hours_text'),
    }
    # Multi-dimensional accessibility from beach_amenities.accessibility_features.
    # Strip internal fields (_source, _updated_at) before passing to Scout.
    af_raw = amen.get('accessibility_features') or {}
    accessibility = {k: v for k, v in af_raw.items() if not k.startswith('_')}
    if not accessibility:
        accessibility = None

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

    # Landscape features pulled from osm_landing locally (was Overpass live).
    op = fetch_landscape_features(fid)
    physical          = op.get("physical", [])
    dog_friendly_pois = op.get("dog_friendly_pois", [])

    # Spatial neighbors — closest 2 active beaches with dog policy.
    # Lets Scout disambiguate adjacent zones (e.g. "north of here is
    # Bolsa Chica where dogs aren't allowed").
    nearest_neighbors = fetch_nearest_neighbors(fid)

    # Closest dog park within 25mi cap. Pre-materialized via
    # beaches_gold.nearest_dog_park_fid (refresh_nearest_dog_park.py).
    # Lets Scout suggest a fallback when this beach doesn't allow dogs
    # OR augment activity options on adjacent days.
    nearest_dog_park = fetch_nearest_dog_park(fid)

    # Top-3 curated photos' Sonnet pass-1 free-form vision prose.
    # Used by the description prompt to mine for landmark / vegetation /
    # structural facts grounded in the photos. Skip if no curated
    # tagged photos exist (description prompt skips this rule on null).
    photo_descriptions = fetch_photo_descriptions(fid, n=3) if include_photos else []

    # OSM crowd-voice notes (tags->note / tags->description) on nearby
    # beach-relevant features. Sparse (~3% of beaches have any hit) but
    # high-quality nuance when present.
    osm_notes = fetch_osm_notes(fid)

    # cpad_unit is intentionally NOT in the prompt right now — CPAD links
    # are geographic-overlap based and conflict with operator reality on
    # carve-out beaches (e.g., HB Dog Beach falls inside Bolsa Chica State
    # Beach's polygon but is City-managed, not CDPR-managed). Re-introduce
    # once we have an operator-reconciled "parent unit" attribution.
    _ = cpad_unit  # kept for audit, not sent

    # Source-page content already cached during dog-policy extraction.
    # Reused here as authoritative grounding for the description (same
    # pages, same trust hierarchy). park_url_extractions has a
    # dog-focused description column AND raw_text; we pass both.
    pue = supa("/rest/v1/park_url_extractions", params={
        "select": "source_url,description,raw_text",
        "arena_group_id": f"eq.{fid}",
        "order": "scraped_at.desc", "limit": "3",
    }) or []
    source_pages = [{
        "url": p.get("source_url"),
        "extracted_description": p.get("description"),
        "raw_text_excerpt": (p.get("raw_text") or "")[:3000],
    } for p in pue if (p.get("description") or p.get("raw_text"))]

    return {
        "name": g.get("display_name_override") or g["name"],
        "location": ", ".join(filter(None, [g.get("county_name"), g.get("state")])),
        "address": g.get("address"),
        "zones": fetch_zones_summary(bdp.get("zone_rules") or {}),
        "parking": parking,
        "amenities": amenities,
        "accessibility": accessibility,
        "verified_physical_features": physical,
        "nearest_neighbors": nearest_neighbors,
        "nearest_dog_park": nearest_dog_park,
        "nearby_dog_friendly_pois": dog_friendly_pois,
        "osm_notes": osm_notes,
        "source_pages": source_pages,
        "photo_descriptions": photo_descriptions,
    }


_PROMPT_SHA = hashlib.sha256(PROMPT.encode()).hexdigest()[:8]


def input_hash(inputs: dict) -> str:
    """Prompt-aware cache key. Hashes inputs AND prompt+model so that
    description regen fires automatically when the PROMPT or MODEL
    changes — not only when the input bundle changes. On a one-shot
    prompt edit, this triggers a full-catalog regen on next pipeline
    run (1045 MVP+ beaches × ~$0.018 = ~$19); subsequent runs cache
    normally."""
    payload = {
        "inputs":     inputs,
        "prompt_sha": _PROMPT_SHA,
        "model":      MODEL,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:16]


# ─── Sonnet call ──────────────────────────────────────────────────────────

def call_sonnet(inputs: dict) -> tuple[str, dict]:
    body = {
        "model": MODEL, "max_tokens": 600,
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
    grp.add_argument("--county", help="run on all active+scoreable beaches in this county (e.g. 'Orange')")
    ap.add_argument("--state", default="CA", help="state filter for --county / --full (default CA)")
    ap.add_argument("--refresh", action="store_true",
                    help="regenerate even when input_hash matches cache")
    ap.add_argument("--dry-run", action="store_true",
                    help="build inputs and print, don't call Sonnet")
    ap.add_argument("--no-photos", action="store_true",
                    help="omit photo_descriptions from input bundle (experimental photo-text pipeline off)")
    args = ap.parse_args()

    fids = select_targets(args)
    print(f"Targets: {len(fids)} beaches  (cost ~${len(fids)*0.003:.2f} via {MODEL})")

    cost_in = cost_out = 0
    cache_hits = generated = errors = 0

    for i, fid in enumerate(fids, 1):
        try:
            inputs = build_inputs(fid, include_photos=not args.no_photos)
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
