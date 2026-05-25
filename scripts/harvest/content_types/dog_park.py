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


# OSM names that don't actually identify a specific park — they're
# generic enough that trigram match against an operator's specific
# name gives misleadingly low scores. When OSM uses one of these,
# we lean harder on proximity.
GENERIC_OSM_NAMES = frozenset({
    "",
    "dog park",
    "dog parks",
    "dog play area",
    "dog play areas",
    "dog run",
    "dog area",
    "off-leash area",
    "off-leash dog area",
    "off leash dog area",
    "off-leash zone",
    "off leash zone",
    "dog park entrance",
})

# Beyond this radius, even a null/generic OSM polygon shouldn't be
# treated as "the same park" — too easy to pick up an unrelated
# nearby off-leash zone.
NULL_NAME_MATCH_RADIUS_M = 400
GENERIC_NAME_MATCH_RADIUS_M = 600


def _normalize_osm_name(name: str | None) -> str:
    if name is None:
        return ""
    return name.lower().strip()


def _proximity_score(dist_m: float, full_score_until_m: float = 100,
                     zero_score_from_m: float = 1000) -> float:
    """Linear decay: 1.0 at <=full_score_until_m, 0.0 at >=zero_score_from_m."""
    if dist_m <= full_score_until_m:
        return 1.0
    if dist_m >= zero_score_from_m:
        return 0.0
    return (zero_score_from_m - dist_m) / (zero_score_from_m - full_score_until_m)


def inventory_match_query(cursor, extracted_row: dict) -> dict:
    """For one extracted dog-park row, find best match in osm_dog_parks.

    Scoring has three modes:

    1. **OSM name is NULL/empty**: name signal unusable. Score by
       proximity alone, but cap the search radius to NULL_NAME_MATCH_RADIUS_M
       (default 400m). Beyond that, an unnamed OSM polygon is more likely
       a different park than the same one.

    2. **OSM name is generic** ("Dog Play Area", "Off-leash Dog Area", etc.):
       name signal is weak (could describe many parks). Weight proximity
       heavily and shorten the effective match radius.

    3. **OSM name is specific**: standard weighted scoring
       (0.6 * trigram + 0.4 * proximity).

    Returns dict with keys:
        status        'matched' | 'weak_match' | 'gap' | 'no_geocode'
        match_id, match_score, components
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
    osm_name_norm = _normalize_osm_name(osm_name)

    if osm_name_norm == "":
        # NULL/empty OSM name: rely on proximity alone, capped radius.
        mode = "null_name"
        if dist_m > NULL_NAME_MATCH_RADIUS_M:
            # Too far to confidently match a null-name polygon
            return {"status": "gap", "match_id": None, "match_score": None,
                    "components": {"reason": "null_name_osm_too_far",
                                    "osm_id_candidate": osm_id,
                                    "dist_m": round(dist_m, 1)}}
        score = _proximity_score(dist_m, full_score_until_m=50,
                                  zero_score_from_m=NULL_NAME_MATCH_RADIUS_M)
    elif osm_name_norm in GENERIC_OSM_NAMES:
        # Generic OSM name: weight proximity heavily, capped radius.
        mode = "generic_name"
        if dist_m > GENERIC_NAME_MATCH_RADIUS_M:
            return {"status": "gap", "match_id": None, "match_score": None,
                    "components": {"reason": "generic_name_osm_too_far",
                                    "osm_name": osm_name,
                                    "osm_id_candidate": osm_id,
                                    "dist_m": round(dist_m, 1)}}
        prox = _proximity_score(dist_m, full_score_until_m=100,
                                 zero_score_from_m=GENERIC_NAME_MATCH_RADIUS_M)
        score = 0.2 * trigram + 0.8 * prox
    else:
        # Specific OSM name: standard weighted scoring.
        mode = "specific_name"
        prox = _proximity_score(dist_m)
        score = 0.6 * trigram + 0.4 * prox

        # Tightening (Franz 2026-05-24 LATE): when the OSM neighbor
        # has a specific name AND proximity is 0 (>=1km away) AND
        # trigram is below the strong-match threshold (0.85), it's
        # almost certainly a different park — flag as gap, not weak.
        # Example: Upper Noe Dog Play Area matched to Upper Douglass
        # at 1054m with trigram 0.65. Franz confirmed different parks.
        if prox == 0 and trigram < 0.85:
            return {"status": "gap", "match_id": None, "match_score": None,
                    "components": {"reason": "specific_name_distant_low_trigram",
                                    "osm_id_candidate": osm_id,
                                    "osm_name": osm_name,
                                    "trigram": round(trigram, 3),
                                    "dist_m": round(dist_m, 1),
                                    "mode": mode}}

    status = "matched" if score >= 0.55 else "weak_match"
    return {
        "status": status,
        "match_id": osm_id,
        "match_score": round(score, 3),
        "components": {
            "trigram": round(trigram, 3),
            "dist_m": round(dist_m, 1),
            "osm_name": osm_name,
            "mode": mode,
        },
    }
