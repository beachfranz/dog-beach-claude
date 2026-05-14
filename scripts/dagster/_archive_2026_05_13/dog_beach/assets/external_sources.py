"""External data origins — files on disk and remote APIs that feed the
805 spine ingest scripts.

These are lineage-only AssetSpecs (non-executable). Their materialization
is "someone downloaded a file or scraped an API"; Dagster doesn't manage
that. They appear as upstream nodes so the lineage shows where each
spine source's raw data originally came from.

Lineage:
  external/us_beaches_csv          ─→ us_beach_points_run    ─→ public/us_beach_points
  external/cpad_shapefile          ─→ cpad_units_run         ─→ public/cpad_units
  external/cpad_arcgis_featureserver ─→ cpad_units_run       (alt source)
  external/osm_overpass            ─→ public/osm_features    (no Python loader; manual sync)
  external/ccc_data_portal         ─→ public/ccc_access_points (no Python loader; SQL function)
"""
from dagster import AssetSpec, AssetKey


us_beaches_csv = AssetSpec(
    key=AssetKey(["external", "us_beaches_csv"]),
    description="share/Dog_Beaches/US_beaches_with_state.csv (8,041 rows). "
                "Schema: WKT, fid, COUNTRY, NAME, ADDR1..5, CAT_MOD. The "
                "CAT_MOD values (e.g. travel_and_leisure.beach) match the "
                "Geoapify Places taxonomy, suggesting the raw CSV (US_beaches.csv) "
                "came from Geoapify's worldwide POI dataset, but the original "
                "fetch path isn't documented in the repo. STATE column was "
                "added 2026-04-23 by scripts/add_state_to_csv.py. Loaded into "
                "public.us_beach_points by scripts/load_us_beach_points.py.",
    group_name="external_sources",
    kinds={"csv", "file"},
)

cpad_shapefile = AssetSpec(
    key=AssetKey(["external", "cpad_shapefile"]),
    description="CPAD_2025b_Units.shp from CNRA (California Natural Resources "
                "Agency). 17,239 protected-area Unit polygons in NAD83 CA Teale "
                "Albers. Reprojected to WGS84 by scripts/load_cpad_shapefile.py.",
    group_name="external_sources",
    kinds={"shapefile", "geospatial"},
)

cpad_arcgis_featureserver = AssetSpec(
    key=AssetKey(["external", "cpad_arcgis_featureserver"]),
    description="https://gis.cnra.ca.gov/arcgis/rest/services/Boundaries/"
                "CPAD_AgencyLevel/MapServer/0 — paginated FeatureServer endpoint "
                "for CPAD AgencyLevel (~160K features). Alternative to the "
                "shapefile, used by scripts/load_cpad.py when full state load "
                "is needed (the shapefile loader is preferred).",
    group_name="external_sources",
    kinds={"arcgis", "external_api"},
)

osm_overpass = AssetSpec(
    key=AssetKey(["external", "osm_overpass"]),
    description="OpenStreetMap data via the Overpass API. Active fetchers "
                "use the Kumi Systems mirror at "
                "https://overpass.kumi.systems/api/interpreter (faster + more "
                "permissive than the main overpass-api.de). Per-feature "
                "scripts: scripts/one_off/fetch_osm_beach_polygons_ca.py "
                "(natural=beach polygons), fetch_osm_dog_features_ca.py "
                "(leisure=dog_park + dog tags), fetch_osm_dog_parks_ca.py, "
                "fetch_osm_coastline_oc.py, fetch_osm_sand_shingle_laguna.py. "
                "No single canonical loader — these scripts each refresh a "
                "different slice of public.osm_features.",
    group_name="external_sources",
    kinds={"openstreetmap", "external_api"},
)

ccc_arcgis_featureserver = AssetSpec(
    key=AssetKey(["external", "ccc_arcgis_featureserver"]),
    description="California Coastal Commission Public Access Points ArcGIS "
                "FeatureServer. ~1,631 features. Endpoint: "
                "https://services9.arcgis.com/wwVnNW92ZHUIr0V0/arcgis/rest/"
                "services/AccessPoints/FeatureServer/0/query. Fetched and "
                "upserted by the admin-load-ccc edge function (one-shot, "
                "admin-secret gated, idempotent).",
    group_name="external_sources",
    kinds={"arcgis", "external_api"},
)


assets = [
    us_beaches_csv,
    cpad_shapefile,
    cpad_arcgis_featureserver,
    osm_overpass,
    ccc_arcgis_featureserver,
]
