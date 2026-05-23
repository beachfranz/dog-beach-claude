"""NH State Parks (New Hampshire Division of Parks and Recreation) photo
loader, on the StateParksLoader ABC.

Pattern: Kentico CMS with sitemap-driven park discovery + two coexisting
image URL conventions on the same domain.

Discovery: /sitemap.xml exposes every park as
  <loc>https://www.nhstateparks.org/find-parks-trails/<slug></loc>
(slug has no further path separators, so the category-filter pages like
/find-parks-trails/types-of-nh-state-parks/parks-with-hiking exclude
themselves naturally from the discovery regex).

Image patterns on a park page:
  - /NHStateParks/media/NHStateParks/<subpath>/<file>.jpg?ext=.jpg
    Used by hero banners (under New_Hero-Banners/Park-Page-Banners/) and
    by site chrome (Logos/, Main-Nav-Images/, CTA-Tiles/, Icons/).
  - /getmedia/<guid>/<filename>.aspx?width=N&height=M
    Kentico's GUID-keyed managed-content endpoint. Carries both real park
    photos (1080+ wide) and tiny activity icons (46×46).

Filter strategy:
  1. Drop known chrome subpaths under /NHStateParks/media/.
  2. Drop /getmedia/ URLs whose dimensions are <400 on either axis
     (activity-symbol icons live at 46×46 / 200×200).
  3. Drop /getmedia/ filenames that look like icons/symbols/buttons or
     single-word activity tokens (fishing.aspx, hiking-icon-*, etc).
  4. Park-relevance: hero banners under /Park-Page-Banners/ keep
     unconditionally (per-park by construction); /getmedia/ photos keep
     when the filename contains a ≥4-char park-name token. This mirrors
     dnrec_de's _is_park_relevant heuristic.

Pattern category: KENTICO-CMS. New in this loader; reusable for any
Kentico-backed state-parks site (NM and TN both run Kentico variants
per outside survey).

Per Franz 2026-05-23 LATE — 6th subclass / state-launch for NH.
"""
from __future__ import annotations

import os
import re
import sys
import urllib.parse

# sys.path bootstrap per HARD pin sys-path-bootstrap-for-common-imports —
# enables `from scripts.loaders._base import ...` whether invoked as a
# script (`python scripts/loaders/nhsp.py`) or imported.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import requests

from scripts.loaders._base import StateParksLoader, ParkInfo, Photo

NHSP_BASE = "https://www.nhstateparks.org"
SITEMAP_PATH = "/sitemap.xml"
PARK_PATH = "/find-parks-trails/{slug}"

# Sitemap park URL: <loc>https://www.nhstateparks.org/find-parks-trails/<slug></loc>.
# Anchor on `</loc>` after the slug (no extra path segments) so category
# pages (/find-parks-trails/types-of-nh-state-parks/parks-with-hiking)
# are excluded.
SITEMAP_RE = re.compile(
    r'<loc>https://www\.nhstateparks\.org/find-parks-trails/'
    r'([a-z0-9\-]+)</loc>',
    re.IGNORECASE,
)

# Captures both image URL conventions in one pass. Anchored on src=" so
# we only pick up <img>-style references, not background-image CSS.
IMG_RE = re.compile(
    r'src="('
    r'/NHStateParks/media/[^"]+?\.(?:jpg|jpeg|png|webp)'
    r'|/getmedia/[^"]+?\.aspx'
    r')([^"]*)"',
    re.IGNORECASE,
)

# /getmedia/<guid>/... → grab the guid for stable external_id.
GETMEDIA_GUID_RE = re.compile(
    r'/getmedia/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/',
    re.IGNORECASE,
)

# Chrome subpaths under /NHStateParks/media/ — always drop.
CHROME_SUBPATH_RE = re.compile(
    r'/NHStateParks/media/NHStateParks/(?:Logos|Main-Nav-Images|CTA-Tiles|Icons|'
    r'Buttons|Footer-Images|Header-Images)/',
    re.IGNORECASE,
)

# Hero banner subpath — always keep unconditionally. The CMS uses two
# layouts: the canonical /New_Hero-Banners/Park-Page-Banners/ subfolder
# and a flatter /New_Hero-Banners/* layout (with sibling /Mobile/
# variants). Both are real per-park heroes.
HERO_BANNER_RE = re.compile(
    r'/NHStateParks/media/NHStateParks/New_Hero-Banners/',
    re.IGNORECASE,
)

# Single-word activity-icon names that ship as /getmedia/ assets across
# all park pages. These ARE filtered by the dimension check too, but
# the explicit list catches edge cases where a width param is absent.
ACTIVITY_ICON_TOKENS = (
    "fishing", "hiking", "swimming", "boating", "kayaking", "biking",
    "camping", "skiing", "snowshoeing", "snowmobile", "equestrian",
    "picnic", "wildlife", "hunting", "atv", "ohrv", "rving",
)

# Filename suffixes/fragments that mark icons/buttons regardless of size.
ICON_FRAGMENT_RE = re.compile(
    r'(?:-icon|-symbol|-button|_icon|_symbol|_button|_logo|-logo)',
    re.IGNORECASE,
)

# Minimum dimension to count as a real photo (not an activity icon).
MIN_DIM_PX = 400


def _extract_dims(query: str) -> tuple[int | None, int | None]:
    """Parse ?width=N&height=M from a query string. Returns (None, None)
    when neither param is present (Kentico defaults to original size)."""
    if not query:
        return None, None
    params = urllib.parse.parse_qs(query.lstrip("?"))
    w = params.get("width", [None])[0]
    h = params.get("height", [None])[0]
    try:
        wi = int(w) if w is not None else None
    except (TypeError, ValueError):
        wi = None
    try:
        hi = int(h) if h is not None else None
    except (TypeError, ValueError):
        hi = None
    return wi, hi


class NhspLoader(StateParksLoader):
    """NH state-parks photo loader."""

    state_code = "NH"
    source_name = "nhstateparks"
    attribution = "New Hampshire State Parks"
    sort_order_base = 60   # state-parks tier (matches destateparks band)
    polygon_filter = {"mng_type": "STAT"}  # PAD-US state lands only
    license = "fair-use"
    # NH park names follow simple suffixes; nothing extra beyond the
    # baseline normalize_place_name() tuple.
    extra_name_suffixes = ()

    # ── Discovery: nhstateparks.org/sitemap.xml ─────────────────────

    def discover_parks(self, http: requests.Session) -> dict[str, ParkInfo]:
        try:
            r = http.get(NHSP_BASE + SITEMAP_PATH, timeout=self.timeout_s)
            if r.status_code != 200:
                return {}
        except requests.RequestException:
            return {}

        out: dict[str, ParkInfo] = {}
        for slug in SITEMAP_RE.findall(r.text):
            if slug in out:
                continue
            name = slug.replace("-", " ").title()
            # NH sitemap slugs are already park-name-shaped (e.g.
            # "hampton-beach-state-park"). No "State Park" suffix
            # injection needed.
            url = NHSP_BASE + PARK_PATH.format(slug=slug)
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

        seen: set[str] = set()
        photos: list[Photo] = []
        park_tokens = self._park_name_tokens(park.name)

        for m in IMG_RE.finditer(html):
            path, query = m.group(1), m.group(2) or ""
            # Always drop chrome subpaths
            if CHROME_SUBPATH_RE.search(path):
                continue
            is_hero = bool(HERO_BANNER_RE.search(path))

            # Dimension gate (skip for hero banners — they're per-park
            # and the dimension hint isn't always present)
            if not is_hero:
                w, h = _extract_dims(query)
                if (w is not None and w < MIN_DIM_PX) or \
                   (h is not None and h < MIN_DIM_PX):
                    continue

            # Filename-derived icon filter
            filename = path.rsplit("/", 1)[-1]
            if ICON_FRAGMENT_RE.search(filename):
                continue
            # Single-word activity-icon endpoints (fishing.aspx etc.)
            stem = filename.rsplit(".", 1)[0].lower()
            if stem in ACTIVITY_ICON_TOKENS:
                continue

            # Park-relevance: hero banners pass automatically; everything
            # else needs a park-name token in the filename.
            if not is_hero and park_tokens:
                if not self._filename_matches_park(filename, park_tokens):
                    continue

            # Stable external_id: GUID for /getmedia/, filename for media/
            guid_m = GETMEDIA_GUID_RE.search(path)
            external_id = guid_m.group(1) if guid_m else filename

            # Canonical URL: strip resize params so Vision gets the full
            # asset. Origin still returns content; query was only a
            # server-side render hint.
            full_url = urllib.parse.urljoin(NHSP_BASE, path)
            if full_url in seen:
                continue
            seen.add(full_url)

            photos.append(Photo(
                url=full_url,
                filename=filename,
                page_url=park.url,
                external_id=external_id,
                meta={"is_hero": is_hero},
            ))
        return photos

    # ── Helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _park_name_tokens(park_name: str) -> set[str]:
        """≥4-char distinctive tokens; mirrors WSPRC + DE convention."""
        stop = {"state", "park", "beach", "lake", "river", "pond",
                "mountain", "notch", "falls", "point", "the", "of",
                "and", "area", "natural", "historic", "historical",
                "recreation", "memorial", "wayside"}
        toks = [t for t in re.sub(r"[^a-z]+", " ", park_name.lower()).split()
                if t and t not in stop and len(t) >= 4]
        return set(toks)

    @staticmethod
    def _filename_matches_park(filename: str, park_tokens: set[str]) -> bool:
        fname_norm = re.sub(r"[^a-z]+", " ",
                            urllib.parse.unquote(filename).lower())
        words = set(fname_norm.split())
        if park_tokens & words:
            return True
        # Fallback: substring on concatenated form (e.g. "HamptonBeach_*"
        # has no separator between tokens).
        low = fname_norm.replace(" ", "")
        return any(t in low for t in park_tokens)


if __name__ == "__main__":
    sys.exit(NhspLoader().run_cli())
