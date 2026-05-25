"""Beach content_type module for the harvest framework.

Defines schema + prompt + URL hints + inventory comparison for
extracting BEACHES from operator websites (LA County Beaches & Harbors,
City of San Diego, NPS coastal park pages, EBRPD shoreline, etc.).

Inventory target: public.arena — the master beach inventory consolidated
from OSM beach polys + POI beach points + CCC access points. Beaches the
operator publishes that don't appear in arena are real discovery gaps.
"""

CONTENT_TYPE = "beach"

ATTRIBUTES_FIELDS = [
    "hours",
    "dogs_allowed",
    "dogs_seasonal",
    "leash_policy",
    "has_lifeguards",
    "lifeguard_season",
    "has_restrooms",
    "has_showers",
    "has_parking",
    "parking_fee",
    "has_food",
    "swimming_allowed",
    "surf_friendly",
    "fishing_allowed",
    "bonfires_allowed",
    "wheelchair_accessible",
    "amenity_notes",
]

SIBLING_URL_HINTS = [
    "/beaches",
    "/our-beaches",
    "/coastal-parks",
    "/parks/beaches",
    "/visit/beaches",
    "/beach-and-bay",
    "/shoreline",
    "/beach-finder",
    "/find-a-beach",
    "/park-finder/beaches",
]

INVENTORY_TABLE = "arena"

EXTRACTION_PROMPT = """\
You extract a list of BEACHES from an operator's web page.

A beach is a sand/shoreline access point that the operator manages — either
ocean, bay, lake, or river beach. INCLUDE:
  - Named beaches (e.g. "Zuma Beach", "Ocean Beach", "Black's Beach")
  - Coastal access points with a beach designation (e.g. "Crystal Cove State Beach")
  - Operator-managed lake / river beaches (e.g. "Crown Memorial State Beach")

EXCLUDE:
  - Dog parks, regular parks, trails, campgrounds with no beach
  - Marinas, harbors, piers (without an associated beach)
  - Sub-features of a beach (volleyball courts, snack bars) — extract the
    parent beach instead
  - Aggregate / category links

Output strict JSON; no prose. "locations" is an array. For single-beach
pages, return one element. For rules-only pages with no enumeration,
return {"locations": []}.

{
  "locations": [
    {
      "name":              "<exact name as listed>",
      "address":           "<street + city if listed>" or null,
      "address_city":      "<city if separate>" or null,
      "address_state":     "<2-letter>" or null,
      "subpage_url":       "<URL of per-beach page if linked>" or null,
      "attributes": {
        "hours":                "<as stated, e.g. 'dawn to dusk' or '6am-10pm'>" or null,
        "dogs_allowed":         "yes" | "no" | "leashed" | "seasonal" | "off-leash-area-only" | null,
        "dogs_seasonal":        "<seasonal restriction text, if applicable>" or null,
        "leash_policy":         "<verbatim from the page if mentioned>" or null,
        "has_lifeguards":       true|false|null,
        "lifeguard_season":     "<e.g. 'Memorial Day to Labor Day'>" or null,
        "has_restrooms":        true|false|null,
        "has_showers":          true|false|null,
        "has_parking":          true|false|null,
        "parking_fee":          "<e.g. '$15/day' or 'free'>" or null,
        "has_food":             true|false|null,
        "swimming_allowed":     true|false|null,
        "surf_friendly":        true|false|null,
        "fishing_allowed":      true|false|null,
        "bonfires_allowed":     true|false|null,
        "wheelchair_accessible":true|false|null,
        "amenity_notes":        "<acreage + special features + closures>" or null
      }
    }
  ]
}
"""


# Generic beach names that don't uniquely identify a specific beach.
# When the closest arena candidate has one of these names, lean harder
# on proximity.
GENERIC_ARENA_NAMES = frozenset({
    "",
    "beach",
    "the beach",
    "city beach",
    "public beach",
    "state beach",
    "county beach",
    "ocean beach",  # multiple "Ocean Beach" exist across CA (SF, SD, etc.)
})

NULL_NAME_MATCH_RADIUS_M = 500
GENERIC_NAME_MATCH_RADIUS_M = 800


def _normalize_arena_name(name: str | None) -> str:
    if name is None:
        return ""
    return name.lower().strip()


def _proximity_score(dist_m: float, full_score_until_m: float = 100,
                     zero_score_from_m: float = 1500) -> float:
    """Linear decay. Beaches are larger features than dog parks so the
    "same beach" radius is a bit wider — operator-listed lat/lng might
    be the parking lot or the access point, not the sand."""
    if dist_m <= full_score_until_m:
        return 1.0
    if dist_m >= zero_score_from_m:
        return 0.0
    return (zero_score_from_m - dist_m) / (zero_score_from_m - full_score_until_m)


def inventory_match_query(cursor, extracted_row: dict) -> dict:
    """For one extracted beach, find best match in public.arena.

    Modes (same family as dog_park):
      1. arena name NULL/empty + close → match by proximity alone
      2. arena name generic + close → weight proximity heavily
      3. arena name specific → standard weighted scoring
      4. specific + far + low trigram → gap
    """
    lat = extracted_row.get("lat")
    lng = extracted_row.get("lng")
    name = extracted_row.get("name") or ""

    if lat is None or lng is None:
        return {"status": "no_geocode", "match_id": None, "match_score": None,
                "components": None}

    cursor.execute("""
        SELECT a.fid,
               a.name,
               similarity(lower(%s), lower(coalesce(a.name, ''))) AS trigram,
               ST_Distance(
                 ST_SetSRID(ST_MakePoint(%s::float8, %s::float8), 4326)::geography,
                 a.geom::geography
               ) AS dist_m
          FROM public.arena a
         WHERE a.is_active = true
           AND ST_DWithin(
                 ST_SetSRID(ST_MakePoint(%s::float8, %s::float8), 4326)::geography,
                 a.geom::geography, 3000)
         ORDER BY similarity(lower(%s), lower(coalesce(a.name, ''))) DESC,
                  ST_Distance(
                    ST_SetSRID(ST_MakePoint(%s::float8, %s::float8), 4326)::geography,
                    a.geom::geography
                  ) ASC
         LIMIT 1
    """, (name, lng, lat, lng, lat, name, lng, lat))
    row = cursor.fetchone()
    if not row:
        return {"status": "gap", "match_id": None, "match_score": None,
                "components": {"reason": "no_arena_beach_within_3km"}}
    arena_id, arena_name, trigram, dist_m = row
    trigram = float(trigram)
    dist_m = float(dist_m)
    arena_name_norm = _normalize_arena_name(arena_name)

    if arena_name_norm == "":
        mode = "null_name"
        if dist_m > NULL_NAME_MATCH_RADIUS_M:
            return {"status": "gap", "match_id": None, "match_score": None,
                    "components": {"reason": "null_name_arena_too_far",
                                    "arena_id_candidate": arena_id,
                                    "dist_m": round(dist_m, 1)}}
        score = _proximity_score(dist_m, full_score_until_m=100,
                                  zero_score_from_m=NULL_NAME_MATCH_RADIUS_M)
    elif arena_name_norm in GENERIC_ARENA_NAMES:
        mode = "generic_name"
        if dist_m > GENERIC_NAME_MATCH_RADIUS_M:
            return {"status": "gap", "match_id": None, "match_score": None,
                    "components": {"reason": "generic_name_arena_too_far",
                                    "arena_name": arena_name,
                                    "arena_id_candidate": arena_id,
                                    "dist_m": round(dist_m, 1)}}
        prox = _proximity_score(dist_m, full_score_until_m=100,
                                 zero_score_from_m=GENERIC_NAME_MATCH_RADIUS_M)
        score = 0.2 * trigram + 0.8 * prox
    else:
        mode = "specific_name"
        prox = _proximity_score(dist_m)
        score = 0.6 * trigram + 0.4 * prox
        if prox == 0 and trigram < 0.85:
            return {"status": "gap", "match_id": None, "match_score": None,
                    "components": {"reason": "specific_name_distant_low_trigram",
                                    "arena_id_candidate": arena_id,
                                    "arena_name": arena_name,
                                    "trigram": round(trigram, 3),
                                    "dist_m": round(dist_m, 1),
                                    "mode": mode}}

    status = "matched" if score >= 0.55 else "weak_match"
    return {
        "status": status,
        "match_id": arena_id,
        "match_score": round(score, 3),
        "components": {
            "trigram": round(trigram, 3),
            "dist_m": round(dist_m, 1),
            "arena_name": arena_name,
            "mode": mode,
        },
    }
