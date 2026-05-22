"""MD DNR (Maryland Department of Natural Resources) photo loader, on
the StateParksLoader ABC.

Pattern: SharePoint static HTML — `/publiclands/Pages/parkmap.aspx`
lists all parks via static `<a>` tags in regional subdirectories
(/western, /eastern, /central, /southern). Per-park profile pages have
content imagery inline as `<img src="/publiclands/PublishingImages/..."`
tags (no derivative-size prefix like Drupal; just the canonical path).

This is the 4th pattern category (after Drupal-sitemap / CMS-embed /
DB-driven): SHAREPOINT-STATIC. Probably reusable for other states whose
DNRs run on SharePoint (.aspx URLs are the tell).

Per Franz 2026-05-22 — 4th subclass / state-launch for MD.
"""
from __future__ import annotations

import re
import urllib.parse

import requests

from scripts.loaders._base import StateParksLoader, ParkInfo, Photo

MD_DNR_BASE = "https://dnr.maryland.gov"
PARKMAP_PATH = "/publiclands/Pages/parkmap.aspx"

# Park link pattern: <a href="/publiclands/Pages/<region>/<slug>.aspx">Name</a>
# Regions observed: western, eastern, central, southern
PARK_LINK_RE = re.compile(
    r'<a\s+[^>]*href="(/publiclands/Pages/(?:western|eastern|central|southern)/'
    r'([a-z0-9_-]+)\.aspx)"[^>]*>([^<]{3,80})</a>',
    re.IGNORECASE,
)

# Image link pattern: anything under /publiclands/PublishingImages/
# (skip /_layouts/ which is SharePoint chrome — favicons, common icons).
IMG_RE = re.compile(
    r'<img[^>]+src="(/publiclands/PublishingImages/([^"?]+\.(?:jpg|jpeg|png)))[^"]*"',
    re.IGNORECASE,
)

# Skip common chrome / placeholder filenames
DROP_FILENAMES = re.compile(
    r'(?:logo|icon|favicon|placeholder|spacer|spcommon|^MPS-LOGO)',
    re.IGNORECASE,
)


class MdDnrLoader(StateParksLoader):
    state_code = "MD"
    source_name = "md_dnr"
    attribution = "Maryland Department of Natural Resources"
    sort_order_base = 20
    # MD PAD-US uses mng_type='STAT' for state-managed. Adding mng_name
    # filter would over-restrict (MD doesn't have OPRD/WSPRC-style
    # 'SPR' sub-type granularity). Polygon-name matching against the
    # DNR's parkmap names handles self-filtering.
    polygon_filter = {"mng_type": "STAT"}

    # ── Discovery: scrape parkmap.aspx for park links ───────────────

    def discover_parks(self, http: requests.Session) -> dict[str, ParkInfo]:
        try:
            r = http.get(MD_DNR_BASE + PARKMAP_PATH, timeout=self.timeout_s)
            if r.status_code != 200:
                return {}
        except requests.RequestException:
            return {}

        out: dict[str, ParkInfo] = {}
        for href, slug, name in PARK_LINK_RE.findall(r.text):
            # Some anchors repeat across the page (sidebar + body); first wins.
            if slug in out:
                continue
            out[slug] = ParkInfo(
                key=slug,
                name=name.strip(),
                url=MD_DNR_BASE + href,
                meta={"slug": slug},
            )
        return out

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

        found: dict[str, Photo] = {}
        for path, filename in IMG_RE.findall(html):
            if DROP_FILENAMES.search(filename):
                continue
            if filename in found:
                continue
            url = urllib.parse.urljoin(MD_DNR_BASE, path)
            found[filename] = Photo(
                url=url,
                filename=filename,
                page_url=park.url,
                external_id=f"md_dnr:{park.key}:{filename}",
            )
        return list(found.values())

    # ── Park-name-token filter for cross-park chrome ────────────────

    def filter_photos(self, photos: list[Photo], park: ParkInfo) -> list[Photo]:
        """Drop images whose filename has no distinctive token from the
        park name. SharePoint pages sometimes embed sidebar promos
        pointing at other parks' /PublishingImages/ assets.
        """
        park_tokens = self._park_name_tokens(park.name)
        if not park_tokens:
            return photos
        keep = []
        for ph in photos:
            fname_norm = re.sub(
                r"[^a-z]+", " ",
                urllib.parse.unquote(ph.filename).lower(),
            )
            fname_words = set(fname_norm.split())
            if park_tokens & fname_words:
                keep.append(ph)
                continue
            low = fname_norm.replace(" ", "")
            if any(t in low for t in park_tokens):
                keep.append(ph)
        return keep

    @staticmethod
    def _park_name_tokens(park_name: str) -> set[str]:
        stop = {"state", "park", "forest", "lake", "area", "natural",
                "resources", "the", "of", "and", "wildlife", "management"}
        toks = [t for t in re.sub(r"[^a-z]+", " ", park_name.lower()).split()
                if t and t not in stop and len(t) >= 4]
        return set(toks)


if __name__ == "__main__":
    import sys
    sys.exit(MdDnrLoader().run_cli())
