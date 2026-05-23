"""CDPR (California State Parks) photo loader, on the StateParksLoader ABC.

Pattern: DB-driven park-list (no scrape of CDPR site index). Each park's
gallery lives at parks.ca.gov/Gallery/<page_id>/. fid-attribution comes
from beach_enrichment_provenance.source_url where the page_id is already
recorded — fan out per page_id.

This is the "DB-driven" pattern category (vs WSPRC's sitemap-driven and
OPRD's CMS-embed). The ABC handles this by letting discover_parks return
ParkInfo with .fids already populated, which signals the base to skip
polygon-based resolution.

Refactored from load_cdpr_park_gallery.py per Franz 2026-05-22.
"""
from __future__ import annotations

import os
import re
import sys

# sys.path bootstrap per HARD pin sys-path-bootstrap-for-common-imports —
# enables `from scripts.loaders._base import ...` whether invoked as a
# script (`python scripts/loaders/cdpr.py`) or imported.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import psycopg2
import requests

from scripts.loaders._base import StateParksLoader, ParkInfo, Photo, PG

# parks.ca.gov page IDs that are umbrella/home pages, not actual parks.
UMBRELLA_PAGE_IDS = {"23973", "30471"}

# Image filename pattern in the gallery markup
IMG_RE = re.compile(
    r'(https?://www\.parks\.ca\.gov/pages/\d+/images/[^"\'\\s<>]+\.(?:jpg|jpeg|png))',
    re.IGNORECASE,
)

PAGE_ID_RE = re.compile(r"page_id=(\d+)")


class CdprLoader(StateParksLoader):
    state_code = "CA"
    source_name = "cdpr"
    attribution = "California State Parks"
    license = "cdpr_state_parks"
    # CDPR ranks shared park-gallery photos BEHIND geo-matched Flickr/WC
    # — base 1000 (vs other loaders' 20). Sub-beach order added on top.
    sort_order_base = 1000
    # polygon_filter unused — CDPR is DB-driven, not PAD-US-driven.
    polygon_filter = {}

    # ── Discovery: DB-driven via beach_enrichment_provenance ────────

    def discover_parks(self, http: requests.Session) -> dict[str, ParkInfo]:
        """Find CDPR parks by querying for parks.ca.gov source_urls in
        governance provenance. Returns {page_id: ParkInfo(fids=[...])} —
        fids are populated directly here, signaling the base to skip
        polygon resolution.
        """
        sql = """
            SELECT bep.source_url, c.gold_fid
              FROM beach_enrichment_provenance bep
              JOIN (
                SELECT gold_fid FROM beach_enrichment_provenance
                 WHERE field_group='governance' AND is_canonical=true
                   AND claimed_values->>'name' = 'California Department of Parks and Recreation'
              ) c ON c.gold_fid = bep.gold_fid
              JOIN beaches_gold g ON g.fid = c.gold_fid AND g.is_active
             WHERE bep.source_url ILIKE '%parks.ca.gov%'
        """
        pg = psycopg2.connect(**PG)
        try:
            with pg.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        finally:
            pg.close()

        by_page: dict[str, list[int]] = {}
        for url, fid in rows:
            m = PAGE_ID_RE.search(url or "")
            if not m:
                continue
            page_id = m.group(1)
            by_page.setdefault(page_id, [])
            if fid not in by_page[page_id]:
                by_page[page_id].append(fid)

        out: dict[str, ParkInfo] = {}
        for page_id, fids in by_page.items():
            out[page_id] = ParkInfo(
                key=page_id,
                name=f"page_id={page_id}",   # CDPR has no human name in DB; use page_id
                url=f"https://www.parks.ca.gov/Gallery/{page_id}",
                fids=sorted(fids),
                meta={"park_page_id": page_id, "kind": "park_gallery_shared"},
            )
        return out

    # ── Skip umbrella pages (CDPR-specific) ─────────────────────────

    def should_skip_park(self, park: ParkInfo) -> bool:
        return str(park.key) in UMBRELLA_PAGE_IDS

    # ── Per-park gallery scrape ─────────────────────────────────────

    def fetch_park_photos(self, http: requests.Session,
                          park: ParkInfo) -> list[Photo]:
        try:
            r = http.get(park.url, timeout=self.timeout_s)
            if r.status_code != 200:
                return []
            html = r.text
        except requests.RequestException:
            return []

        # Dedupe while preserving order
        seen: set[str] = set()
        out: list[Photo] = []
        for m in IMG_RE.finditer(html):
            u = m.group(1).replace("http://", "https://")
            if u in seen:
                continue
            seen.add(u)
            filename = u.rsplit("/", 1)[-1]
            out.append(Photo(
                url=u,
                filename=filename,
                page_url=f"https://www.parks.ca.gov/?page_id={park.key}",
                external_id=filename,   # filename is stable per park page
                meta={"gallery_index": len(out)},
            ))
        return out


if __name__ == "__main__":
    import sys
    sys.exit(CdprLoader().run_cli())
