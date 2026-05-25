"""Dog park content_type module for the harvest framework.

Defines schema + prompt + URL hints + inventory comparison for
extracting dog parks from operator websites.
"""

CONTENT_TYPE = "dog_park"

ATTRIBUTES_FIELDS = [
    "hours",
    "has_small_dog_area",
    "has_water",
    "has_double_gate",
    "surface",
    "has_lighting",
    "amenity_notes",
]

# Candidate sub-paths to try when searching an operator's site for a
# dog-park listing page. Used by discover_listing_urls.py against
# operator.web_url. Order matters — earlier matches win.
SIBLING_URL_HINTS = [
    "/dog-parks",
    "/dogparks",
    "/dog-park",
    "/parks/dog-parks",
    "/parks/dog-park",
    "/park-and-recreation/dogs",
    "/park-and-recreation/parks/dogs/leashfree",
    "/recreation/dog-parks",
    "/animal-services/dog-parks",
    "/specialty-parks/dog-parks",
]

INVENTORY_TABLE = "osm_dog_parks"

EXTRACTION_PROMPT = """\
You extract a list of DOG PARKS from an operator's web page.

A dog park is a designated off-leash facility (typically fenced) where dogs
are explicitly permitted off-leash. EXCLUDE:
  - beaches with dog rules (even if dogs allowed off-leash there)
  - trails or regular parks that just permit dogs on-leash
  - aggregate links ("All Dog Parks") or category navigation
  - generic rules pages without per-park enumeration

Output strict JSON; no prose. "parks" is an array. For single-park pages,
return one element. For rules-only pages with NO park enumeration, return
[]. When in doubt about whether something is a dog park, include it if the
page treats it as one.

{
  "parks": [
    {
      "name":              "<exact name as listed>",
      "address":           "<street + city if listed>" or null,
      "address_city":      "<city if separate from address>" or null,
      "address_state":     "<2-letter>" or null,
      "subpage_url":       "<URL of per-park page if linked>" or null,
      "attributes": {
        "hours":               "<as stated, e.g. 'dawn to dusk' or '6am-10pm'>" or null,
        "has_small_dog_area":  true|false|null,
        "has_water":           true|false|null,
        "has_double_gate":     true|false|null,
        "surface":             "grass" | "decomposed_granite" | "natural" | "concrete" | "synthetic" | null,
        "has_lighting":        true|false|null,
        "amenity_notes":       "<acreage + special features + closures>" or null
      }
    }
  ]
}
"""


def inventory_match_query(cursor, extracted_row: dict) -> dict:
    """For one extracted dog-park row, find best match in osm_dog_parks.

    Returns dict with keys:
        status        'matched' | 'weak_match' | 'gap' | 'no_geocode'
        match_id      osm_dog_parks.id of best match (or None)
        match_score   weighted score 0-1
        components    {trigram, dist_m, ...}
    """
    lat = extracted_row.get("lat")
    lng = extracted_row.get("lng")
    name = extracted_row.get("name") or ""

    if lat is None or lng is None:
        return {"status": "no_geocode", "match_id": None, "match_score": None,
                "components": None}

    cursor.execute("""
        SELECT odp.id,
               odp.name,
               similarity(lower(%s), lower(coalesce(odp.name, ''))) AS trigram,
               ST_Distance(
                 ST_SetSRID(ST_MakePoint(%s::float8, %s::float8), 4326)::geography,
                 odp.geom::geography
               ) AS dist_m
          FROM public.osm_dog_parks odp
         WHERE ST_DWithin(
                 ST_SetSRID(ST_MakePoint(%s::float8, %s::float8), 4326)::geography,
                 odp.geom::geography, 2000)
         ORDER BY similarity(lower(%s), lower(coalesce(odp.name, ''))) DESC,
                  ST_Distance(
                    ST_SetSRID(ST_MakePoint(%s::float8, %s::float8), 4326)::geography,
                    odp.geom::geography
                  ) ASC
         LIMIT 1
    """, (name, lng, lat, lng, lat, name, lng, lat))
    row = cursor.fetchone()
    if not row:
        return {"status": "gap", "match_id": None, "match_score": None,
                "components": {"reason": "no_osm_dog_park_within_2km"}}
    osm_id, osm_name, trigram, dist_m = row
    trigram = float(trigram)
    dist_m = float(dist_m)
    # Weighted score: name dominates, proximity provides tiebreaking.
    proximity_score = max(0.0, (1000 - dist_m) / 1000) if dist_m < 1000 else 0.0
    score = 0.6 * trigram + 0.4 * proximity_score
    status = "matched" if score >= 0.55 else "weak_match"
    return {
        "status": status,
        "match_id": osm_id,
        "match_score": round(score, 3),
        "components": {
            "trigram": round(trigram, 3),
            "dist_m": round(dist_m, 1),
            "osm_name": osm_name,
        },
    }
