"""DNREC DE (Delaware Division of Parks and Recreation) photo loader,
on the StateParksLoader ABC.

Pattern: WordPress with per-park slug pages.
  - Park-finder index at /park-finder/ exposes all 18 parks as
    /park/<slug>/ links.
  - Per-park profile pages embed photos from wp-content/uploads as
    <img src="https://www.destateparks.com/wp-content/uploads/YYYY-MM/<name>.jpg">.
  - Three asset name conventions distinguish content vs chrome:
      banner_<Park_Name>.jpg     — header image (1 per park)
      highlight_<Park_Name>_*.jpg — body imagery (3-6 per park)
      programs_<Topic>_*.jpg     — cross-park program promos (skip)
      facebook-/insta-/Fill-     — social-share chrome (skip)
      cropped-DESP-Favicon-*     — favicons (skip)
  - Thumbnail derivative suffix follows WordPress convention:
      banner_Fort_DuPont-150x150.jpg → banner_Fort_DuPont.jpg
    Strip via re.sub on -\\d+x\\d+ before the extension.

Pattern category: WORDPRESS-SLUG. New in this loader; reusable for any
WordPress site whose state parks follow the /park/<slug>/ convention.

Per Franz 2026-05-23 — 5th subclass / state-launch for DE.
"""
from __future__ import annotations

import os
import re
import sys
import urllib.parse
from typing import Iterable

# sys.path bootstrap per HARD pin sys-path-bootstrap-for-common-imports —
# enables `from scripts.loaders._base import ...` whether invoked as a
# script (`python scripts/loaders/dnrec_de.py`) or imported.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import requests

from scripts.loaders._base import StateParksLoader, ParkInfo, Photo

DESP_BASE = "https://www.destateparks.com"
FINDER_PATH = "/park-finder/"
PARK_PATH = "/park/{slug}/"

SLUG_RE = re.compile(r'/park/([a-z0-9\-]+)/')
# wp-content/uploads images on the destateparks domain (any subpath)
IMG_RE = re.compile(
    r'(https?://(?:www\.)?destateparks\.com/wp-content/uploads/'
    r'[^"\'<>\s]+?\.(?:jpg|jpeg|png|webp))',
    re.IGNORECASE,
)
# Thumbnail derivative suffix: image-150x150.jpg → image.jpg
THUMB_RE = re.compile(r'-\d+x\d+(\.(?:jpg|jpeg|png|webp))$', re.IGNORECASE)

# Filename fragments that mark non-content site chrome
CHROME_FRAGMENTS = (
    "facebook-", "insta-", "Fill-", "cropped-DESP-Favicon",
    "/themes/desp/assets/", "globe.png", "search-icon", "close-icon",
    "menu-icon",
)


class DnrecDeLoader(StateParksLoader):
    """DE state-parks photo loader."""

    state_code = "DE"
    source_name = "destateparks"
    attribution = "Delaware State Parks (destateparks.com)"
    sort_order_base = 60   # state-parks tier (NPS=10, CDPR=20, etc.)
    polygon_filter = {"mng_type": "STAT"}  # PAD-US state lands only
    license = "fair-use"

    def discover_parks(self, http: requests.Session) -> dict[str, ParkInfo]:
        """One GET on /park-finder/ exposes all 18 park slugs."""
        r = http.get(DESP_BASE + FINDER_PATH, timeout=20)
        r.raise_for_status()
        slugs = sorted(set(SLUG_RE.findall(r.text)))
        out: dict[str, ParkInfo] = {}
        for slug in slugs:
            name = slug.replace("-", " ").title()
            # Some slugs lack the "State Park" suffix; add it for consistency
            # with PAD-US Unit_Nm matching ("Fort Dupont State Park").
            if "state park" not in name.lower() and "state heritage" not in name.lower():
                name += " State Park"
            out[slug] = ParkInfo(
                key=slug,
                name=name,
                url=urllib.parse.urljoin(DESP_BASE, PARK_PATH.format(slug=slug)),
                meta={"slug": slug},
            )
        return out

    def fetch_park_photos(self, http: requests.Session, park: ParkInfo) -> list[Photo]:
        """Per-park profile page — pull wp-content images, filter chrome,
        strip thumbnail derivative suffix."""
        r = http.get(park.url, timeout=20)
        r.raise_for_status()
        seen: set[str] = set()
        photos: list[Photo] = []
        for raw_url in IMG_RE.findall(r.text):
            if any(frag in raw_url for frag in CHROME_FRAGMENTS):
                continue
            if not self._is_park_relevant(raw_url, park.name):
                continue
            url = THUMB_RE.sub(r"\1", raw_url)  # strip -WxH suffix
            if url in seen:
                continue
            seen.add(url)
            filename = url.rsplit("/", 1)[-1]
            photos.append(Photo(
                url=url,
                filename=filename,
                page_url=park.url,
                external_id=filename,  # stable per-park dedup key
            ))
        return photos

    @staticmethod
    def _is_park_relevant(url: str, park_name: str) -> bool:
        """Keep banner_/highlight_ generically; cross-park assets only if
        the filename references this park's name tokens. programs_* are
        cross-park promos — drop unless name-matched (rare)."""
        fname = url.rsplit("/", 1)[-1].lower()
        # Always keep banner_ + highlight_ — they're per-park by construction
        if fname.startswith(("banner_", "highlight_")):
            return True
        # Name-match: any park-name token (≥4 chars) in filename
        toks = [t for t in re.sub(r"[^a-z]+", " ", park_name.lower()).split()
                if t not in {"state", "park", "heritage"} and len(t) >= 4]
        if toks and any(t in fname for t in toks):
            return True
        return False

if __name__ == "__main__":
    import sys
    sys.exit(DnrecDeLoader().run_cli())
