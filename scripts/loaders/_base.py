"""StateParksLoader ABC — shared scaffolding for state-parks photo loaders.

Per Franz 2026-05-22 — refactored from the 3 hand-built loaders
(load_cdpr_park_gallery.py + load_oprd_photos.py + load_wsprc_photos.py)
after side-by-side analysis showed ~70% of each was identical scaffolding.

What the base class owns
========================
- Env + DB connection (POOLER + PG dict)
- HTTP session factory (browser UA + truststore-injected SSL)
- Standard CLI shape (--pilot / --full / --park / --fids / --dry-run /
  --refresh / --workers, mutually-exclusive selection flags)
- Polygon → beach-fid resolution via beach_polygon_membership
  (mng_type/mng_name filterable for state-park sub-types)
- Park-name normalization + token-subset matching
- Per-fid photo INSERT with ON CONFLICT DO NOTHING
- Refresh path (DELETE-before-insert)
- Parallel page fetch (ThreadPoolExecutor + per-thread session)
- Progress logging + totals

What the subclass provides
==========================
- Class constants (state_code, source_name, attribution, sort_order_base,
  polygon_filter, license)
- discover_parks(http) → {park_key: ParkInfo}
- fetch_park_photos(http, park_info) → list[Photo]
- Optional: normalize_park_name(name) override for state-specific suffix
  lists; filter_photos() hook for per-source cross-promo dedup; etc.

Subclass total length target: ~80 lines.

Composition with state_park_urls.json registry
==============================================
Each subclass's class constants align with one entry in
scripts/state_park_urls.json. New states wire up by:
  1. Adding their entry to state_park_urls.json (already canonical registry)
  2. Picking the closest existing subclass OR writing a new one based on
     the pattern category (CMS-list / API-backed / master-table /
     embed-only / DB-driven)
"""
from __future__ import annotations

import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

try:
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

import argparse
import json
import os
import re
import threading
import time
import urllib.parse
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

from scripts.common import normalize_place_name, PLACE_NAME_SUFFIXES

# ─── Env / DB ────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(
    host=_p.hostname, port=_p.port or 5432, user=_p.username,
    password=os.environ["SUPABASE_DB_PASSWORD"],
    dbname=(_p.path or "/postgres").lstrip("/"), sslmode="require",
)

# ─── HTTP ────────────────────────────────────────────────────────────

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
)
BROWSER_HEADERS = {
    "User-Agent":      BROWSER_UA,
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

# Default suffixes used by normalize_park_name() — pulled from the
# canonical PLACE_NAME_SUFFIXES tuple in scripts.common.text_cleaning
# so all loaders share the same baseline. Subclass-specific extras go
# in the subclass's extra_name_suffixes class attribute.
DEFAULT_NAME_SUFFIXES = PLACE_NAME_SUFFIXES

# ─── Dataclasses ─────────────────────────────────────────────────────

@dataclass
class Photo:
    """One photo to insert into beach_photos."""
    url: str
    filename: str
    page_url: str
    external_id: str            # stable per-park dedup key
    meta: dict = field(default_factory=dict)  # extras → source_meta jsonb


@dataclass
class ParkInfo:
    """One park's discovery payload."""
    key: str | int              # opaque, source-specific identifier
    name: str                   # human-readable name (used for matching)
    url: str | None = None      # park's own page URL (for fetch_park_photos)
    fids: list[int] = field(default_factory=list)  # beach fids in this park's polygon
    meta: dict = field(default_factory=dict)       # extras (page_id, slug, etc.)


# ─── ABC ─────────────────────────────────────────────────────────────

class StateParksLoader(ABC):
    """ABC for per-state-parks-dept photo loaders.

    Override class constants + 2 abstract methods. Defaults inherited
    from the base cover the ~70% of behavior common to all loaders.
    """

    # ── REQUIRED class constants (subclass overrides) ────────────────
    state_code: str = ""               # "CA" / "OR" / "WA" / "MD"
    source_name: str = ""              # "cdpr" / "oprd" / "wsprc" — beach_photos.source value
    attribution: str = ""              # "California State Parks" etc.
    license: str = "unknown"

    # Sort-order band. CDPR/OPRD/WSPRC all use 20-band (below Flickr=10,
    # above generic Unsplash=25). New sources keep this convention.
    sort_order_base: int = 20

    # Polygon filter for PAD-US fanout. Subclass overrides for sub-types
    # like {'mng_type':'STAT','mng_name':'SPR'} (OPRD/WSPRC). CDPR uses
    # governance-evidence-based discovery so this is unused.
    polygon_filter: dict = {"mng_type": "STAT"}

    # Extra suffixes to strip in name normalization (state-specific
    # ColdFusion/Drupal/CMS conventions). Subclass extends.
    extra_name_suffixes: tuple = ()

    # Pacing between HTTP requests (seconds). Subclass may tighten or
    # loosen based on observed site behavior.
    pacing_s: float = 1.0
    timeout_s: int = 30

    # ── REQUIRED abstract methods (subclass implements) ──────────────

    @abstractmethod
    def discover_parks(self, http: requests.Session) -> dict[Any, ParkInfo]:
        """Return all parks in this state's parks-dept catalog.

        Implementations vary by pattern:
          - CMS list page    → scrape /find-parks listing
          - API-backed       → hit JSON endpoint
          - DB-driven        → query beach_enrichment_provenance (CDPR-style)
          - Master pet table → scrape one canonical page with all parks

        Returns: {park_key: ParkInfo}. park_key is opaque (any hashable).
        """
        ...

    @abstractmethod
    def fetch_park_photos(self, http: requests.Session,
                          park: ParkInfo) -> list[Photo]:
        """Fetch + parse one park's photo gallery. Idempotent per call."""
        ...

    # ── OPTIONAL hooks (subclass MAY override) ───────────────────────

    def normalize_park_name(self, name: str) -> str:
        """Lowercase + strip CMS suffixes + collapse non-alnum.
        Used for polygon-name matching.

        Delegates to scripts.common.text_cleaning.normalize_place_name()
        which carries the canonical implementation + suffix tuple
        (consolidated 2026-05-22 from 4 duplicate impls)."""
        return normalize_place_name(name, extra_suffixes=self.extra_name_suffixes)

    def should_skip_park(self, park: ParkInfo) -> bool:
        """Filter hook (e.g., CDPR's umbrella-page filter). Default = no skip."""
        return False

    def filter_photos(self, photos: list[Photo], park: ParkInfo) -> list[Photo]:
        """Post-fetch filter hook (e.g., WSPRC's cross-promo token filter).
        Default = pass-through."""
        return photos

    # ── Shared HTTP / DB helpers ─────────────────────────────────────

    def http_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update(BROWSER_HEADERS)
        return s

    def get_state_polygons(self, pg, fids_filter: list[int] | None = None
                           ) -> dict[str, dict]:
        """PAD-US polygons for this state matching `polygon_filter`,
        containing ≥1 scoreable beach. {normalized_polygon_name: record}.

        `record` has: polygon_name, beach_fids[].

        Subclasses with non-PAD-US discovery (e.g., CDPR governance-based)
        should NOT call this; they resolve fids in discover_parks() instead.
        """
        clauses = ["g.state = %s", "g.is_active",
                   "g.scoring_tier IN ('daily','hourly')",
                   "bpm.polygon_kind = 'pad_us_unit'"]
        params: list = [self.state_code]
        for col, val in self.polygon_filter.items():
            clauses.append(f"bpm.{col} = %s")
            params.append(val)
        if fids_filter:
            clauses.append("bpm.gold_fid = ANY(%s)")
            params.append(fids_filter)
        sql = f"""
            SELECT bpm.polygon_name,
                   array_agg(DISTINCT bpm.gold_fid ORDER BY bpm.gold_fid) AS beach_fids
              FROM beach_polygon_membership bpm
              JOIN beaches_gold g ON g.fid = bpm.gold_fid
             WHERE {' AND '.join(clauses)}
             GROUP BY bpm.polygon_name
        """
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        out: dict[str, dict] = {}
        for r in rows:
            n = self.normalize_park_name(r["polygon_name"])
            if n:
                out.setdefault(n, dict(r))
        return out

    def resolve_park_polygons(self, parks: dict[Any, ParkInfo],
                              polygons_by_norm: dict[str, dict]
                              ) -> dict[Any, ParkInfo]:
        """Attach beach_fids to ParkInfo by name-matching against the
        polygon dict from get_state_polygons(). Returns parks where match
        succeeded (caller can compare len() to detect unresolved)."""
        resolved: dict[Any, ParkInfo] = {}
        for key, p in parks.items():
            n = self.normalize_park_name(p.name)
            if not n:
                continue
            rec = polygons_by_norm.get(n)
            if rec is None:
                # Token-subset fallback
                p_tokens = set(n.split())
                if not p_tokens:
                    continue
                best = None
                for pn, r in polygons_by_norm.items():
                    c_tokens = set(pn.split())
                    if p_tokens.issubset(c_tokens) or c_tokens.issubset(p_tokens):
                        if best is None or len(c_tokens) < len(set(best[0].split())):
                            best = (pn, r)
                rec = best[1] if best else None
            if rec:
                p.fids = list(rec["beach_fids"])
                resolved[key] = p
        return resolved

    def insert_photos_for_fid(self, pg, fid: int, photos: list[Photo],
                              park: ParkInfo, refresh: bool) -> int:
        """One row per photo for fid. Returns rows actually inserted."""
        if not photos:
            return 0
        n = 0
        with pg.cursor() as cur:
            if refresh:
                cur.execute(
                    "DELETE FROM beach_photos WHERE arena_group_id = %s "
                    "AND source = %s",
                    (fid, self.source_name),
                )
            for i, ph in enumerate(photos):
                meta = {"park_name": park.name, "filename": ph.filename}
                meta.update(park.meta)
                meta.update(ph.meta)
                try:
                    cur.execute("""
                        INSERT INTO beach_photos
                          (arena_group_id, source, external_id, image_url, thumb_url,
                           attribution, license, page_url, distance_m, sort_order,
                           source_meta, loaded_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                        ON CONFLICT (arena_group_id, source, external_id) DO NOTHING
                    """, (
                        fid, self.source_name, ph.external_id,
                        ph.url, ph.url,
                        self.attribution, self.license, ph.page_url,
                        None,                                  # distance_m (centroid backfill)
                        self.sort_order_base + i,
                        json.dumps(meta),
                    ))
                    if cur.rowcount > 0:
                        n += 1
                except psycopg2.Error as e:
                    print(f"  [INSERT FAIL] fid={fid} {ph.filename}: {e}",
                          flush=True)
                    pg.rollback()
                    return n
            pg.commit()
        return n

    # ── CLI orchestration ────────────────────────────────────────────

    def build_argparser(self) -> argparse.ArgumentParser:
        ap = argparse.ArgumentParser(description=self.__doc__ or "")
        grp = ap.add_mutually_exclusive_group()
        grp.add_argument("--pilot", type=int, help="Limit to first N parks")
        grp.add_argument("--full", action="store_true",
                         help=f"All {self.state_code} {self.source_name.upper()} parks")
        grp.add_argument("--park", help="One park by exact name (diagnostic)")
        ap.add_argument("--fids",
                        help="Comma-sep beach fids — restrict to parks containing "
                             "at least one of these fids (pipeline chunked dispatch)")
        ap.add_argument("--refresh", action="store_true",
                        help=f"DELETE existing {self.source_name} rows for each "
                             "beach before insert")
        ap.add_argument("--dry-run", action="store_true",
                        help="Print plan + photos found, but skip DB writes")
        ap.add_argument("--workers", type=int, default=5,
                        help="Parallel HTTP fetch workers (default: 5)")
        return ap

    def run_cli(self) -> int:
        """Standard orchestration: discover → resolve → fetch (parallel) →
        insert per-fid (sequential). Subclasses generally just call this."""
        args = self.build_argparser().parse_args()
        if not (args.pilot or args.full or args.park or args.fids):
            print("ERROR: provide --pilot N, --full, --park NAME, or --fids LIST",
                  file=sys.stderr)
            return 2

        fids_filter: list[int] | None = None
        if args.fids:
            try:
                fids_filter = [int(x.strip()) for x in args.fids.split(",") if x.strip()]
            except ValueError:
                print("ERROR: --fids must be comma-separated integers",
                      file=sys.stderr)
                return 2

        pg = psycopg2.connect(**PG)
        pg.autocommit = False
        try:
            http = self.http_session()
            print(f"Discovering {self.state_code} {self.source_name.upper()} parks...",
                  flush=True)
            all_parks = self.discover_parks(http)
            if not all_parks:
                print(f"ERROR: discover_parks returned no parks", file=sys.stderr)
                return 1
            print(f"  catalog: {len(all_parks)} parks", flush=True)

            # Polygon-driven resolution (skipped if subclass already populated fids)
            needs_polygon_resolution = any(not p.fids for p in all_parks.values())
            if needs_polygon_resolution:
                polygons = self.get_state_polygons(pg, fids_filter=fids_filter)
                print(f"  {self.state_code} scoreable polygons: {len(polygons)}",
                      flush=True)
                resolved = self.resolve_park_polygons(all_parks, polygons)
            else:
                # Subclass (e.g., CDPR) already resolved fids during discovery
                resolved = all_parks
                if fids_filter:
                    resolved = {k: p for k, p in resolved.items()
                                if any(f in fids_filter for f in p.fids)}

            # Apply --park / --pilot / --fids filters
            if args.park:
                wanted = args.park.lower()
                resolved = {k: p for k, p in resolved.items()
                            if p.name.lower() == wanted}
                if not resolved:
                    print(f"ERROR: no park named {args.park!r} in resolved set",
                          file=sys.stderr)
                    return 2
            elif args.pilot:
                keys = sorted(resolved.keys(), key=lambda k: resolved[k].name)
                resolved = {k: resolved[k] for k in keys[:args.pilot]}

            # Subclass skip filter (e.g., CDPR umbrella pages)
            resolved = {k: p for k, p in resolved.items()
                        if not self.should_skip_park(p)}

            if not resolved:
                print("No parks resolved to scoreable beaches.", flush=True)
                return 0
            print(f"\nTargets: {len(resolved)} resolved parks", flush=True)
            if args.dry_run:
                print("  [DRY-RUN] no DB writes\n", flush=True)

            # Parallel page fetch with per-thread session
            _local = threading.local()
            def _worker_http():
                if not hasattr(_local, "s"):
                    _local.s = self.http_session()
                return _local.s

            def _fetch_one(key_park):
                key, park = key_park
                try:
                    photos = self.fetch_park_photos(_worker_http(), park)
                    photos = self.filter_photos(photos, park)
                    return key, park, photos, None
                except Exception as e:
                    return key, park, [], str(e)

            results: list[tuple[Any, ParkInfo, list[Photo], str | None]] = []
            with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
                futs = [pool.submit(_fetch_one, (k, p)) for k, p in resolved.items()]
                for i, fut in enumerate(as_completed(futs), 1):
                    key, park, photos, err = fut.result()
                    results.append((key, park, photos, err))
                    if err:
                        print(f"  [{i}/{len(resolved)}] {park.name} ERROR: {err}",
                              flush=True)
                    else:
                        print(f"  [{i}/{len(resolved)}] {park.name:<45}  "
                              f"photos={len(photos)}", flush=True)

            # Sequential per-fid insert (DB writes cheap; avoids lock contention)
            total_photos = total_inserted = 0
            for key, park, photos, err in results:
                if err:
                    continue
                total_photos += len(photos)
                if args.dry_run:
                    for ph in photos[:3]:
                        print(f"    {ph.filename}  -> {ph.url[:80]}", flush=True)
                    if len(photos) > 3:
                        print(f"    ... +{len(photos)-3} more", flush=True)
                    continue
                for fid in park.fids:
                    total_inserted += self.insert_photos_for_fid(
                        pg, fid, photos, park, args.refresh,
                    )

            print(f"\n=== TOTALS ===")
            print(f"  catalog parks:       {len(all_parks)}")
            print(f"  resolved parks:      {len(resolved)}")
            print(f"  unique photos found: {total_photos}")
            print(f"  rows inserted:       {total_inserted}")
            return 0
        finally:
            pg.close()
