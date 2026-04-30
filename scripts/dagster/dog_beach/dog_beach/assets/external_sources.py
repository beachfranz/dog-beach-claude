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
                "National beach inventory CSV, originally compiled from NOAA + "
                "DBEACH datasets. Loaded by scripts/load_us_beach_points.py.",
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
    description="OpenStreetMap Overpass API. Source of public.osm_features "
                "(beach polygons + dog-related tags). Synced manually; no "
                "Python loader in repo. Refresh by re-running the Overpass "
                "query and bulk-loading via SQL.",
    group_name="external_sources",
    kinds={"openstreetmap", "external_api"},
)

ccc_data_portal = AssetSpec(
    key=AssetKey(["external", "ccc_data_portal"]),
    description="California Coastal Commission public access database. "
                "Source of public.ccc_access_points (~1.6K active points). "
                "Loaded historically via public.load_ccc_batch() SQL function; "
                "no Python loader in repo today.",
    group_name="external_sources",
    kinds={"ca_state", "external_api"},
)


assets = [
    us_beaches_csv,
    cpad_shapefile,
    cpad_arcgis_featureserver,
    osm_overpass,
    ccc_data_portal,
]
