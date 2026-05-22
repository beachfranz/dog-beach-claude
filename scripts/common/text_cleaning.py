"""Canonical text-cleaning helpers.

Consolidates the normalize/slugify/place-name-handling logic that was
previously duplicated across (per 2026-05-22 audit):
  - scripts/loaders/_base.py            normalize_park_name
  - scripts/load_oprd_photos.py         _normalize_park_name
  - scripts/load_wsprc_photos.py        inline regex normalization
  - scripts/backfill_agency_photo_centroid.py  normalize()
  - scripts/extract_cdpr_master_pet_table.py   string cleaning
  - scripts/derive_policy_source_for_jurisdiction.py  _slugify_jurisdiction
  - scripts/bulk_promote_socal.py       slugify()

Conventions
===========
- All functions are pure (no DB, no HTTP, no I/O).
- All return str or set[str]; never None.
- Empty/whitespace input → empty string return (not None, not crash).
- URL-decode INPUT before passing to normalize_text() if upstream is a
  URL-encoded filename or path component.

Each helper has a docstring that names ONE call-site pattern it
replaces, so future readers can match their use case to the right
helper.
"""
from __future__ import annotations

import re
import unicodedata
import urllib.parse

# ─── Standard place-name suffixes ────────────────────────────────────
#
# Used by normalize_place_name() to strip CMS / parks-dept boilerplate
# before matching against polygon names. Longest-first so the strip is
# greedy (e.g. " state recreation site" beats " state").

PLACE_NAME_SUFFIXES: tuple[str, ...] = (
    " state recreation site",
    " state recreation area",
    " state scenic viewpoint",
    " state scenic corridor",
    " state natural area",
    " state natural site",
    " state heritage site",
    " state historical site",
    " state historic site",
    " state wayside",
    " state forest",
    " state park",
    " state trail",
    " state beach",
    " marine state park",
    " historical state park",
    " national park",
    " national forest",
    " national wildlife refuge",
    " national recreation area",
    " national seashore",
    " national monument",
    " regional park",
    " county park",
    " conservation area",
    " recreation area",
)


# ─── Helpers ─────────────────────────────────────────────────────────

def strip_accents(s: str) -> str:
    """Remove diacritics. 'Café' → 'Cafe'.

    Replaces ad-hoc unicodedata calls scattered across loaders.
    """
    if not s:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(c)
    )


def collapse_whitespace(s: str) -> str:
    """Collapse runs of whitespace to single spaces + strip ends.

    Replaces `re.sub(r'\\s+', ' ', s).strip()` written ~6 times across
    scripts."""
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()


def normalize_text(s: str) -> str:
    """Lowercase + URL-decode + strip accents + non-alnum-to-space + collapse.

    The canonical "fuzzy-match key" used by every loader and the codify
    name-resolver. Output is a sequence of lowercase ASCII tokens
    separated by single spaces.

    Replaces:
      - scripts/backfill_agency_photo_centroid.py `normalize()`
        (was: lowercase + urlunquote + re.sub non-alnum + collapse)
      - scripts/loaders/_base.py inline normalization step

    Example:
      "Café del Mar"      → "cafe del mar"
      "Cama-Beach_2024%20"→ "cama beach 2024"
      "St. Mary's River"  → "st mary s river"
    """
    if not s:
        return ""
    s = urllib.parse.unquote(s).lower()
    s = strip_accents(s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    return collapse_whitespace(s)


def normalize_place_name(s: str, extra_suffixes: tuple[str, ...] = ()) -> str:
    """`normalize_text` + greedy suffix strip of common place-name CMS terms.

    Used for matching state-park / federal-land / county-park names
    against polygon names where the polygon may use a shorter form.

    `extra_suffixes` lets the caller add domain-specific strips on top
    of `PLACE_NAME_SUFFIXES`. Examples: OPRD uses
    " state recreation site" (default); WSPRC adds
    " marine state park trail" (state-specific).

    Replaces:
      - scripts/loaders/_base.py `normalize_park_name()` (different
        suffix tuple per subclass; merge here)
      - scripts/load_oprd_photos.py `_normalize_park_name()` (OPRD-tuple)
      - scripts/load_wsprc_photos.py `resolve_park_url()` slug
        normalization (WSPRC-tuple)
      - scripts/extract_cdpr_master_pet_table.py inline name cleanup

    Example:
      "Cama Beach Historical State Park" → "cama beach historical"
      "Sandy Point State Park"           → "sandy point"
      "Janes Island State Park"          → "janes island"
    """
    s = (s or "").lower()
    suffixes = extra_suffixes + PLACE_NAME_SUFFIXES
    # Longest-first so " state recreation site" wins over " state".
    for sfx in sorted(suffixes, key=len, reverse=True):
        if s.endswith(sfx):
            s = s[: -len(sfx)]
            break
    return normalize_text(s)


def slugify(s: str, separator: str = "-") -> str:
    """`normalize_text` + replace spaces with `separator`.

    Default `-` → kebab-case for URL slugs.
    Pass `separator='_'` for snake_case identifiers.

    Replaces:
      - scripts/bulk_promote_socal.py `slugify()`
      - scripts/derive_policy_source_for_jurisdiction.py
        `_slugify_jurisdiction()`
      - PL/pgSQL `_make_location_slug()` (semantic parity; not a
        Python-side caller)

    Example:
      "Sandy Point State Park" → "sandy-point-state-park"
      "Sandy Point State Park", "_" → "sandy_point_state_park"
    """
    return normalize_text(s).replace(" ", separator)
