"""Dog-friendly park content_type for the harvest framework.

A dog-friendly park is a regular park / state beach / natural area that
ALLOWS DOGS — not a fenced dog-park facility (that's content_type=
'dog_park'). Operators like CDPR publish "where dogs can go" pages that
enumerate this category specifically. Examples:

  - Asilomar State Beach (CDPR /Dogs page) — beach that allows leashed dogs
  - Henry Cowell Redwoods SP — forest that allows dogs
  - Mount Diablo SP — natural area with dog access

Inventory target: public.cpad_units (CDPR-managed land) for the CDPR case.
Other operators don't have a clean canonical inventory target today; the
match is informational ("which CPAD unit is this?") rather than gap-detective.
"""

CONTENT_TYPE = "dog_friendly_park"

ATTRIBUTES_FIELDS = [
    "dogs_allowed",
    "leash_policy",
    "dogs_seasonal",
    "dog_specific_areas",
    "waste_stations",
    "water_for_dogs",
    "amenity_notes",
]

SIBLING_URL_HINTS = [
    "/Dogs",
    "/dogs",
    "/pets",
    "/pet-policy",
    "/dog-friendly",
    "/dog-friendly-parks",
    "/pets-in-parks",
    "/dogs-in-parks",
]

INVENTORY_TABLE = "cpad_units"

EXTRACTION_PROMPT = """\
You extract a list of DOG-FRIENDLY PARKS from an operator's web page.

A dog-friendly park is a regular park / beach / natural area where dogs
are EXPLICITLY ALLOWED (typically on-leash; sometimes with off-leash zones
inside the park). It is NOT a fenced dog-park facility — those are a
separate content type.

INCLUDE:
  - Any park / beach / preserve named where dogs are allowed
  - State beaches that allow dogs (extract them here; the entity-type
    distinction is downstream)
  - Parks with conditional dog rules (e.g. "leashed only", "off-leash
    in designated areas")

EXCLUDE:
  - Parks where dogs are PROHIBITED — don't include
  - Fenced dog-park facilities (those are content_type='dog_park')
  - Generic rule pages with no per-park enumeration

Output strict JSON; no prose. "locations" is an array. For rules-only
pages with no per-park enumeration, return {"locations": []}.

{
  "locations": [
    {
      "name":              "<exact park name as listed (e.g. 'Asilomar SB' or 'Asilomar State Beach')>",
      "address":           "<street if listed>" or null,
      "address_city":      "<city if listed>" or null,
      "address_state":     "<2-letter>" or null,
      "subpage_url":       "<URL of per-park page if linked>" or null,
      "attributes": {
        "dogs_allowed":         "yes" | "leashed" | "off-leash-area-only" | "seasonal" | null,
        "leash_policy":         "<verbatim leash rule from page>" or null,
        "dogs_seasonal":        "<seasonal restriction text>" or null,
        "dog_specific_areas":   "<off-leash zone within park, swim area, etc.>" or null,
        "waste_stations":       true|false|null,
        "water_for_dogs":       true|false|null,
        "amenity_notes":        "<other dog-relevant notes>" or null
      }
    }
  ]
}
"""


GENERIC_CPAD_NAMES = frozenset({
    "",
    "state park",
    "state beach",
    "park",
    "beach",
})

NULL_NAME_MATCH_RADIUS_M = 500
GENERIC_NAME_MATCH_RADIUS_M = 800


def _proximity_score(dist_m: float, full_score_until_m: float = 200,
                     zero_score_from_m: float = 3000) -> float:
    """Linear decay. CPAD units are POLYGONS so dist_m here is
    distance-to-polygon-boundary, which is 0 if the point falls inside
    the polygon. 3km cap allows for points that geocoded to a parking
    lot or trailhead just outside the formal unit boundary."""
    if dist_m <= full_score_until_m:
        return 1.0
    if dist_m >= zero_score_from_m:
        return 0.0
    return (zero_score_from_m - dist_m) / (zero_score_from_m - full_score_until_m)


def inventory_match_query(cursor, extracted_row: dict) -> dict:
    """Match against public.cpad_units — closest CPAD unit to the
    geocoded point.

    Unlike dog_park / beach, the 'gap' semantics here are less crisp —
    every state park IS in CPAD by definition. This match is more about
    LINKAGE (which CPAD unit is this?) than gap detection.
    """
    lat = extracted_row.get("lat")
    lng = extracted_row.get("lng")
    name = extracted_row.get("name") or ""

    if lat is None or lng is None:
        return {"status": "no_geocode", "match_id": None, "match_score": None,
                "components": None}

    cursor.execute("""
        SELECT cu.unit_id,
               cu.unit_name,
               cu.mng_agncy,
               similarity(lower(%s), lower(coalesce(cu.unit_name, ''))) AS trigram,
               ST_Distance(
                 ST_SetSRID(ST_MakePoint(%s::float8, %s::float8), 4326)::geography,
                 cu.geom::geography
               ) AS dist_m
          FROM public.cpad_units cu
         WHERE ST_DWithin(
                 ST_SetSRID(ST_MakePoint(%s::float8, %s::float8), 4326)::geography,
                 cu.geom::geography, 5000)
         ORDER BY similarity(lower(%s), lower(coalesce(cu.unit_name, ''))) DESC,
                  ST_Distance(
                    ST_SetSRID(ST_MakePoint(%s::float8, %s::float8), 4326)::geography,
                    cu.geom::geography
                  ) ASC
         LIMIT 1
    """, (name, lng, lat, lng, lat, name, lng, lat))
    row = cursor.fetchone()
    if not row:
        return {"status": "gap", "match_id": None, "match_score": None,
                "components": {"reason": "no_cpad_unit_within_5km"}}
    unit_id, unit_name, mng_agncy, trigram, dist_m = row
    trigram = float(trigram)
    dist_m = float(dist_m)
    unit_name_norm = (unit_name or "").lower().strip()

    if unit_name_norm == "":
        mode = "null_name"
        if dist_m > NULL_NAME_MATCH_RADIUS_M:
            return {"status": "gap", "match_id": None, "match_score": None,
                    "components": {"reason": "null_name_cpad_too_far",
                                    "dist_m": round(dist_m, 1)}}
        score = _proximity_score(dist_m, full_score_until_m=50,
                                  zero_score_from_m=NULL_NAME_MATCH_RADIUS_M)
    elif unit_name_norm in GENERIC_CPAD_NAMES:
        mode = "generic_name"
        if dist_m > GENERIC_NAME_MATCH_RADIUS_M:
            return {"status": "gap", "match_id": None, "match_score": None,
                    "components": {"reason": "generic_name_cpad_too_far",
                                    "unit_name": unit_name,
                                    "dist_m": round(dist_m, 1)}}
        prox = _proximity_score(dist_m)
        score = 0.2 * trigram + 0.8 * prox
    else:
        mode = "specific_name"
        prox = _proximity_score(dist_m)
        score = 0.6 * trigram + 0.4 * prox
        if prox == 0 and trigram < 0.85:
            return {"status": "gap", "match_id": None, "match_score": None,
                    "components": {"reason": "specific_name_distant_low_trigram",
                                    "cpad_unit_id": unit_id,
                                    "unit_name": unit_name,
                                    "trigram": round(trigram, 3),
                                    "dist_m": round(dist_m, 1)}}

    status = "matched" if score >= 0.55 else "weak_match"
    return {
        "status": status,
        "match_id": unit_id,
        "match_score": round(score, 3),
        "components": {
            "trigram": round(trigram, 3),
            "dist_m": round(dist_m, 1),
            "unit_name": unit_name,
            "mng_agncy": mng_agncy,
            "mode": mode,
        },
    }
