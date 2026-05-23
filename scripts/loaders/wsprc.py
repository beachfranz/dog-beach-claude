"""WSPRC (Washington State Parks) photo loader, on the StateParksLoader ABC.

Pattern: Drupal CMS with sitemap-driven park-list discovery + deterministic
image paths under /sites/default/files/styles/.../public/<YYYY-MM>/<file>.jpg.

Refactored from load_wsprc_photos.py per Franz 2026-05-22.
"""
from __future__ import annotations

import os
import re
import sys
import urllib.parse

# sys.path bootstrap per HARD pin sys-path-bootstrap-for-common-imports —
# enables `from scripts.loaders._base import ...` whether invoked as a
# script (`python scripts/loaders/wsprc.py`) or imported.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import requests

from scripts.loaders._base import StateParksLoader, ParkInfo, Photo

WSPRC_BASE = "https://parks.wa.gov"

# Drupal CMS image pattern. The "styles/<preset>/public/" prefix is added
# for derivative sizes; we strip to recover the original.
IMG_RE = re.compile(
    r'src="(/sites/default/files/styles/[^"]+/public/(\d{4}-\d{2})/([^"?]+\.(?:jpg|jpeg|png))[^"]*)"',
    re.IGNORECASE,
)
DROP_FILENAMES = re.compile(
    r'(?:logo|icon|_icon\.|kayaking\.|beach-exploration\.|paddleboarding\.)',
    re.IGNORECASE,
)
DROP_STYLES = re.compile(
    r'/styles/(?:small_thumbnail_|teaser_|hero_thumb_)',
    re.IGNORECASE,
)


class WsprcLoader(StateParksLoader):
    state_code = "WA"
    source_name = "wsprc"
    attribution = "Washington State Parks and Recreation Commission"
    sort_order_base = 20
    polygon_filter = {"mng_type": "STAT", "mng_name": "SPR"}
    # Order longest-first; base sorts before stripping but explicit helps doc-side.
    extra_name_suffixes = (
        " marine state park trail", " state park trail",
        " marine state park", " conservation area", " recreation area",
    )

    # ── Discovery: parks.wa.gov sitemap.xml ─────────────────────────

    def discover_parks(self, http: requests.Session) -> dict[str, ParkInfo]:
        try:
            r = http.get(f"{WSPRC_BASE}/sitemap.xml", timeout=self.timeout_s)
            if r.status_code != 200:
                return {}
        except requests.RequestException:
            return {}

        out: dict[str, ParkInfo] = {}
        for url in re.findall(
            r'<loc>(https://parks\.wa\.gov/find-parks/state-parks/[^<]+)</loc>',
            r.text,
        ):
            slug = url.rsplit("/", 1)[-1]
            slug = re.sub(r"-(?:retired|archived|deleted)-.*$", "", slug)
            name = re.sub(r"-+", " ", slug).title()  # "cama-beach-state-park" → "Cama Beach State Park"
            if slug not in out:
                out[slug] = ParkInfo(key=slug, name=name, url=url,
                                     meta={"slug": slug})
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
        for m in IMG_RE.finditer(html):
            path, _ym, filename = m.group(1), m.group(2), m.group(3)
            if DROP_FILENAMES.search(filename):
                continue
            if DROP_STYLES.search(path):
                continue
            original = re.sub(r"/styles/[^/]+/public/", "/", path)
            url = urllib.parse.urljoin(WSPRC_BASE, original)
            if filename in found:
                continue
            found[filename] = Photo(
                url=url,
                filename=filename,
                page_url=park.url,
                external_id=f"wsprc:{filename}",
            )
        return list(found.values())

    # ── Cross-promo filter (WSPRC-specific) ─────────────────────────

    def filter_photos(self, photos: list[Photo], park: ParkInfo) -> list[Photo]:
        """Drop sidebar cross-promo thumbnails. The Drupal page surfaces
        small thumbnails for OTHER parks in the sidebar; their filenames
        share no token with this park's name. Require ≥1 distinctive
        park-name token in the filename."""
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
            # Loose fallback: substring check (filenames like 'CamaBeach_*'
            # have no separator between tokens)
            low = fname_norm.replace(" ", "")
            if any(t in low for t in park_tokens):
                keep.append(ph)
        return keep

    @staticmethod
    def _park_name_tokens(park_name: str) -> set[str]:
        stop = {"state", "park", "historical", "historic", "area", "beach",
                "island", "harbor", "point", "bay", "lake", "river",
                "cove", "spit", "the", "of"}
        toks = [t for t in re.sub(r"[^a-z]+", " ", park_name.lower()).split()
                if t and t not in stop and len(t) >= 4]
        return set(toks)


if __name__ == "__main__":
    import sys
    sys.exit(WsprcLoader().run_cli())
