"""OPRD (Oregon Parks and Recreation Department) photo loader, on
the StateParksLoader ABC.

Pattern: ColdFusion CMS with /index.cfm?do=visit.find embedded park-list
JSON + /index.cfm?do=park.profile&parkId=N rendering serverData.photos +
serverData.a11yPhotos JSON arrays inline.

Refactored from load_oprd_photos.py per Franz 2026-05-22.
"""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.parse

# sys.path bootstrap per HARD pin sys-path-bootstrap-for-common-imports —
# enables `from scripts.loaders._base import ...` whether invoked as a
# script (`python scripts/loaders/oprd.py`) or imported.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import requests

from scripts.loaders._base import StateParksLoader, ParkInfo, Photo

OPRD_BASE = "https://stateparks.oregon.gov"
FIND_PATH = "/index.cfm?do=visit.find"
PROFILE_PATH = "/index.cfm?do=park.profile&parkId={park_id}"

# Embedded park list JSON in the find page
PARK_LIST_RE = re.compile(
    r'\{"park_id":(\d+),"hub_property_id":\d+,"park_name":"([^"]+)"'
)
# serverData.photos / serverData.a11yPhotos JSON arrays
PHOTOS_RE = re.compile(
    r'serverData\.(photos|a11yPhotos)\s*=\s*(\[.*?\]);', re.DOTALL
)


class OprdLoader(StateParksLoader):
    state_code = "OR"
    source_name = "oprd"
    attribution = "Oregon Parks and Recreation Department"
    sort_order_base = 20
    polygon_filter = {"mng_type": "STAT", "mng_name": "SPR"}
    # OPRD's extras beyond DEFAULT_NAME_SUFFIXES — most are already there.

    # ── Discovery: scrape /index.cfm?do=visit.find for park-list JSON ─

    def discover_parks(self, http: requests.Session) -> dict[int, ParkInfo]:
        try:
            r = http.get(OPRD_BASE + FIND_PATH, timeout=self.timeout_s)
            if r.status_code != 200:
                return {}
        except requests.RequestException:
            return {}

        out: dict[int, ParkInfo] = {}
        for pid_str, name in PARK_LIST_RE.findall(r.text):
            pid = int(pid_str)
            if pid in out:
                continue
            out[pid] = ParkInfo(
                key=pid,
                name=name,
                url=OPRD_BASE + PROFILE_PATH.format(park_id=pid),
                meta={"park_id": pid},
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
        for kind, raw in PHOTOS_RE.findall(html):
            try:
                arr = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(arr, list):
                continue
            for entry in arr:
                if not isinstance(entry, dict):
                    continue
                fname = entry.get("park_image_file") or entry.get("park_image_name")
                img_url = entry.get("imageFile")
                if not (fname and img_url):
                    continue
                if fname in found:
                    continue  # a11yPhotos can overlap photos
                found[fname] = Photo(
                    url=img_url,
                    filename=fname,
                    page_url=park.url,
                    external_id=f"oprd:{park.key}:{fname}",
                    meta={
                        "kind": kind,                              # 'photos' | 'a11yPhotos'
                        "park_image_id": entry.get("park_image_id"),
                        "description": entry.get("park_image_description") or "",
                        "title": entry.get("park_image_title") or "",
                    },
                )
        return list(found.values())


if __name__ == "__main__":
    import sys
    sys.exit(OprdLoader().run_cli())
