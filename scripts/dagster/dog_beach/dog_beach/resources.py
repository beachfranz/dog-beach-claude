"""Dagster resources — every external surface the pipeline touches.

Each Resource is a ConfigurableResource — injected into assets/ops as a
parameter. Resources are constructed once per Dagster process and reused;
they encapsulate connection management, retries, secret handling, and
cost tracking.

Catalog (grouped by domain):

  Database
    PostgresPoolerResource              port 6543, transaction mode (default)
    PostgresSessionResource             port 5432, session mode (LISTEN/NOTIFY etc.)

  LLM / search
    AnthropicResource                   Claude API + cost helpers
    TavilyResource                      search + extract

  Supabase platform
    SupabaseEdgeFunctionResource        POST to /functions/v1/*
    SubprocessResource                  wraps legacy scripts/*.py

  External GIS / data
    ArcGISRestResource                  generic ArcGIS REST query helper
    OverpassResource                    OSM Overpass QL
    GoogleMapsResource                  geocode + Places
    GeoapifyResource                    legacy geocoder (parked)

  Scoring inputs (daily refresh)
    OpenMeteoResource                   weather forecast + current
    NOAACoOpsResource                   tide stations + predictions
    BestTimeResource                    crowd busyness (on hold per Franz)

  Photos
    WikimediaCommonsResource            Commons API + UA per WMF robots policy
    FlickrResource                      Flickr photo API
    PixabayResource / PexelsResource    royalty-free stock
    UnsplashResource                    stock photo API
    MapillaryResource                   street-level imagery (deprecated 2026-05-09)
    NPSMultimediaResource               federal land photos (parked, no key yet)

Naming: <Capability>Resource. Resources with parallel variants append a
suffix (PostgresSessionResource vs PostgresPoolerResource). When a single
API has multiple endpoints we collapse them into one resource exposing
multiple methods (NOAACoOpsResource exposes both stations() and tides()).

is_active toggle: resources for parked/deprecated APIs include a boolean
is_active config (default false) — calling them when inactive raises a
clear error rather than failing late in the stack. Keeps the lineage
diagram honest about what's hot vs cold.
"""

from __future__ import annotations
import subprocess
import sys
from typing import Any, Sequence

import psycopg2
import psycopg2.extras
from dagster import ConfigurableResource


# ════════════════════════════════════════════════════════════════════════
#  Database
# ════════════════════════════════════════════════════════════════════════

class PostgresPoolerResource(ConfigurableResource):
    """Transaction-mode pooler (port 6543). Default for in-pipeline SQL.

    Transaction mode releases the connection back to the pool between
    statements, so long-running function calls don't hold session-mode
    slots and exhaust the pool — the 2026-05-13 ECHECKOUTTIMEOUT /
    Cloudflare 524 incident pattern.

    statement_timeout is set by the caller via SET inside the transaction
    that runs the actual work. We do NOT pass libpq -c options here —
    pgbouncer in transaction mode rejects connections that include startup
    parameters it doesn't whitelist, causing SSL-closed errors at the
    first cursor.execute().
    """
    host: str
    port: int = 6543
    user: str
    password: str
    dbname: str = "postgres"
    sslmode: str = "require"
    default_statement_timeout: str = "600s"

    def get_connection(self):
        return psycopg2.connect(
            host=self.host, port=self.port, user=self.user,
            password=self.password, dbname=self.dbname, sslmode=self.sslmode,
        )


class PostgresSessionResource(ConfigurableResource):
    """Session-mode pooler (port 5432). Use ONLY when genuinely needed:
    LISTEN/NOTIFY, multi-statement txns with shared state, advisory locks,
    prepared statements reused across calls.

    For everything else use PostgresPoolerResource. Session-mode connections
    blocked on slow queries exhaust the pool (root cause of 2026-05-13).
    """
    host: str
    port: int = 5432
    user: str
    password: str
    dbname: str = "postgres"
    sslmode: str = "require"
    default_statement_timeout: str = "600s"

    def get_connection(self):
        return psycopg2.connect(
            host=self.host, port=self.port, user=self.user,
            password=self.password, dbname=self.dbname, sslmode=self.sslmode,
        )


# ════════════════════════════════════════════════════════════════════════
#  LLM / search
# ════════════════════════════════════════════════════════════════════════

class AnthropicResource(ConfigurableResource):
    """Wraps the Anthropic SDK. Used by Phase 26 (operator extract),
    Phase 29 (section_extract), Phase 30 (descriptions).

    Models:
      claude-haiku-4-5  — fast enum/bool extraction
      claude-sonnet-4   — 3-pass operator policy extraction
      claude-sonnet-4-5 — narrative prose generation
    """
    api_key: str
    default_model: str = "claude-haiku-4-5"

    def get_client(self):
        import anthropic
        return anthropic.Anthropic(api_key=self.api_key)

    @staticmethod
    def estimate_cost_usd(model: str, input_tokens: int, output_tokens: int) -> float:
        rates = {
            # USD per million tokens (input, output) — 2026 pricing
            "claude-haiku-4-5":  (1.00, 5.00),
            "claude-sonnet-4":   (3.00, 15.00),
            "claude-sonnet-4-5": (3.00, 15.00),
            "claude-opus-4":     (15.00, 75.00),
        }
        in_rate, out_rate = rates.get(model, (3.0, 15.0))
        return (input_tokens / 1_000_000 * in_rate
                + output_tokens / 1_000_000 * out_rate)


class TavilyResource(ConfigurableResource):
    """Tavily search/extract API. Used by Phase 26 for the candidate-URL
    search step before the LLM picker."""
    api_key: str

    def get_client(self):
        from tavily import TavilyClient
        return TavilyClient(api_key=self.api_key)


# ════════════════════════════════════════════════════════════════════════
#  Supabase platform
# ════════════════════════════════════════════════════════════════════════

class SupabaseEdgeFunctionResource(ConfigurableResource):
    """POST to Supabase Edge Functions. Used by Phase 32 (daily_refresh_fire
    calls daily-beach-refresh) and any asset that triggers a server-side
    write. Reads use anon key; writes use service_role + x-admin-secret."""
    supabase_url: str
    service_key: str
    admin_secret: str
    default_timeout_seconds: int = 600

    def call(
        self,
        function_name: str,
        body: dict[str, Any] | None = None,
        admin: bool = True,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        import requests
        url = f"{self.supabase_url.rstrip('/')}/functions/v1/{function_name}"
        headers = {
            "Authorization": f"Bearer {self.service_key}",
            "apikey": self.service_key,
            "Content-Type": "application/json",
        }
        if admin:
            headers["x-admin-secret"] = self.admin_secret
        resp = requests.post(
            url,
            json=body or {},
            headers=headers,
            timeout=timeout or self.default_timeout_seconds,
        )
        resp.raise_for_status()
        try:
            return resp.json()
        except ValueError:
            return {"raw": resp.text}


class SubprocessResource(ConfigurableResource):
    """Runs legacy Python scripts as Dagster ops without rewriting bodies.

    Mirrors the chunked-subprocess pattern from run_state_pipeline.py.
    Each call captures stdout/stderr, returns CompletedProcess, times out
    at the given threshold. For multi-chunk fanout (the v2 pattern) prefer
    DynamicPartitionsDefinition over a single op looping internally — the
    framework handles retry per chunk and partition-level concurrency.
    """
    repo_root: str
    python_executable: str = ""  # default = sys.executable

    def run(
        self,
        script_path: str,
        args: list[str] | None = None,
        timeout: int = 600,
        cwd: str | None = None,
    ) -> subprocess.CompletedProcess:
        py = self.python_executable or sys.executable
        cwd = cwd or self.repo_root
        return subprocess.run(
            [py, script_path, *(args or [])],
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )


# ════════════════════════════════════════════════════════════════════════
#  External GIS
# ════════════════════════════════════════════════════════════════════════

class ArcGISRestResource(ConfigurableResource):
    """Generic ArcGIS REST query helper. Many of our external GIS sources
    expose a FeatureServer or MapServer endpoint (USGS PAD-US, USFWS
    Critical Habitat, CDFW Wildlife Areas, California Coastal Commission
    access points, BIA tribal lands, NPS units).

    Per-source bbox/state filtering happens at the query level; this
    resource just provides paginated query() with retries.
    """
    default_timeout_seconds: int = 120
    default_page_size: int = 1000
    user_agent: str = "DogBeachScout/2.0 (https://beachfranz.github.io)"

    def query(
        self,
        endpoint: str,
        where: str = "1=1",
        out_fields: str = "*",
        geometry: dict | None = None,
        spatial_rel: str = "esriSpatialRelIntersects",
        return_geometry: bool = True,
        max_records: int | None = None,
        timeout: int | None = None,
    ) -> list[dict]:
        """Returns list of feature dicts (geometry + attributes per ESRI
        envelope). Handles pagination via resultOffset/resultRecordCount."""
        import requests
        results: list[dict] = []
        offset = 0
        page_size = self.default_page_size
        while True:
            params = {
                "where": where,
                "outFields": out_fields,
                "f": "json",
                "returnGeometry": "true" if return_geometry else "false",
                "spatialRel": spatial_rel,
                "resultOffset": offset,
                "resultRecordCount": page_size,
            }
            if geometry is not None:
                params["geometry"] = str(geometry)
                params["geometryType"] = "esriGeometryEnvelope"
                params["inSR"] = 4326
            resp = requests.get(
                endpoint, params=params,
                headers={"User-Agent": self.user_agent},
                timeout=timeout or self.default_timeout_seconds,
            )
            resp.raise_for_status()
            data = resp.json()
            features = data.get("features", [])
            if not features:
                break
            results.extend(features)
            if max_records and len(results) >= max_records:
                return results[:max_records]
            offset += page_size
            if not data.get("exceededTransferLimit") and len(features) < page_size:
                break
        return results


class OverpassResource(ConfigurableResource):
    """OSM Overpass QL endpoint. Used by Phase 6 (ensure_overpass —
    natural=beach polygons), Phase 7 (ensure_amenities — amenity points
    near beaches), and Phase 8 (ensure_dog_features — dog parks /
    off-leash zones)."""
    endpoint: str = "https://overpass-api.de/api/interpreter"
    default_timeout_seconds: int = 180
    user_agent: str = "DogBeachScout/2.0 (https://beachfranz.github.io)"

    def query(self, ql: str, timeout: int | None = None) -> dict:
        import requests
        resp = requests.post(
            self.endpoint,
            data={"data": ql},
            headers={"User-Agent": self.user_agent},
            timeout=timeout or self.default_timeout_seconds,
        )
        resp.raise_for_status()
        return resp.json()


class GoogleMapsResource(ConfigurableResource):
    """Google Maps Geocoding + Places. Used historically by us_beach_points
    seeding (Geoapify+Google blend) and v2-geocode-context for reverse
    geocode of new beaches into city/county context."""
    api_key: str
    default_timeout_seconds: int = 30

    def reverse_geocode(self, lat: float, lng: float) -> dict:
        import requests
        resp = requests.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"latlng": f"{lat},{lng}", "key": self.api_key},
            timeout=self.default_timeout_seconds,
        )
        resp.raise_for_status()
        return resp.json()


class GeoapifyResource(ConfigurableResource):
    """Geoapify geocoding. Legacy us_beach_points provenance. Parked;
    Google Maps + reverse-geocoded address propagation (2026-05-09) is
    the current path."""
    api_key: str = ""
    is_active: bool = False  # parked


# ════════════════════════════════════════════════════════════════════════
#  Scoring inputs (daily refresh)
# ════════════════════════════════════════════════════════════════════════

class OpenMeteoResource(ConfigurableResource):
    """Open-Meteo forecast + current API. Used by daily-beach-refresh
    and get-beach-now edge functions. Server-side calls — this resource
    exists for completeness / future direct lifts but daily-refresh is
    currently called via SupabaseEdgeFunctionResource."""
    forecast_endpoint: str = "https://api.open-meteo.com/v1/forecast"
    default_timeout_seconds: int = 30


class NOAACoOpsResource(ConfigurableResource):
    """NOAA CO-OPS Tides & Currents. Used for tide predictions and station
    metadata. Server-side calls; daily refresh uses it via the edge
    function path today."""
    stations_endpoint: str = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json"
    predictions_endpoint: str = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    default_timeout_seconds: int = 30


class BestTimeResource(ConfigurableResource):
    """BestTime.app crowd busyness API.

    PARKED per Franz 2026-05-06 ('besttime is on hold for now. it may go
    away'). Don't add new BestTime-dependent wiring. Existing daily-beach-
    refresh + scoring.ts Crowd component stay until explicitly retired.
    """
    api_key: str = ""
    is_active: bool = False  # on hold


# ════════════════════════════════════════════════════════════════════════
#  Photos
# ════════════════════════════════════════════════════════════════════════

class WikimediaCommonsResource(ConfigurableResource):
    """Wikimedia Commons API for photos. Used by Phase 31 (photos_wikimedia)
    via load_wikimedia_commons_photos.py. WMF robots policy requires a
    descriptive User-Agent string and serial requests with politeness
    pacing — the loader enforces both (serial lock + 1.5s pacing per
    project_pipeline_instantiation.md)."""
    user_agent: str = "DogBeachScout/2.0 (https://beachfranz.github.io; contact@franzfunk.com)"
    api_endpoint: str = "https://commons.wikimedia.org/w/api.php"
    pace_seconds: float = 1.5


class FlickrResource(ConfigurableResource):
    """Flickr API for photos. Used by load_flickr_photos.py.

    Per project_flickr_wa_anomaly.md (2026-05-12): RADIUS_KM=0.5,
    sort=interestingness-desc with per_page=20 produced WA hourly
    Flickr hit rate going from 5% → 71%. Default radius matters; 5km
    was pushing famous-but-far landmarks above the per-page cutoff."""
    api_key: str
    default_radius_km: float = 0.5
    default_per_page: int = 20


class PixabayResource(ConfigurableResource):
    """Pixabay royalty-free photos. Used by load_pixabay_photos.py."""
    api_key: str = ""


class PexelsResource(ConfigurableResource):
    """Pexels royalty-free photos. Used by load_pexels_photos.py."""
    api_key: str = ""


class UnsplashResource(ConfigurableResource):
    """Unsplash photos. Used by load_unsplash_photos.py."""
    access_key: str = ""


class MapillaryResource(ConfigurableResource):
    """Mapillary street-level imagery.

    DEPRECATED 2026-05-09 (per project_pipeline_instantiation.md): replaced
    by WikimediaCommonsResource. Commons photos are CC-licensed, higher
    quality (real photos vs street-view), keyword-biased, photographer-
    auto-blocklisted. Mapillary loader at scripts/load_mapillary_photos.py
    is retained for ad-hoc use only."""
    access_token: str = ""
    is_active: bool = False  # deprecated


class NPSMultimediaResource(ConfigurableResource):
    """NPS Multimedia API for federal-land photos (national seashores,
    NWRs, etc.).

    PARKED: no API key yet per project_pipeline_instantiation.md punch list.
    Source code value 'nps' pre-allowed in beach_photos.source CHECK so
    schema is ready when key arrives."""
    api_key: str = ""
    is_active: bool = False  # awaiting API key
