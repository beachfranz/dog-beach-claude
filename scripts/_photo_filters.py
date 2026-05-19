"""Shared filters for photo loaders (Unsplash, Pexels, etc.)

The wrong-beach filter rejects photos whose alt-text/caption mentions
a SPECIFIC beach name that doesn't match the target. Generic
"a beach with blue water" passes; "Martins Beach" for a Crescent Bay
target gets rejected.
"""

from __future__ import annotations
import re

# Drop these tokens when extracting distinctive parts of a beach name.
# "Coronado Dog Beach" -> distinctive: ["coronado", "dog"]
# "Crescent Bay Beach" -> ["crescent", "bay"]
# "Aliso Beach" -> ["aliso"]
_GENERIC_TOKENS = {
    "beach", "park", "state", "city", "the", "pet",
    "north", "south", "east", "west", "main", "cove", "point",
    "harbor", "bay", "sands", "dunes", "of", "and",
}

# Match "X Beach" / "X Y Beach" / "X Y Z Beach" — up to 3 capitalized words
# before "Beach". Captures the X(s).
_BEACH_NAME_RE = re.compile(r"\b((?:[A-Z][a-z]+(?:\s+|-))+)([Bb]each)\b")


def beach_name_tokens(beach_name: str) -> set[str]:
    """Extract distinctive tokens from a beach name (lowercase)."""
    if not beach_name:
        return set()
    words = re.findall(r"[A-Z][a-z]+", beach_name)
    return {w.lower() for w in words if w.lower() not in _GENERIC_TOKENS}


def extract_beach_names(text: str) -> list[str]:
    """Extract 'X Beach' patterns from text. Returns the X portion (without 'Beach')."""
    if not text:
        return []
    out = []
    for m in _BEACH_NAME_RE.finditer(text):
        prefix = m.group(1).strip().rstrip("-").strip()
        # Skip pure compass/generic prefixes ("North Beach", "Main Beach")
        words = [w.lower() for w in re.findall(r"[A-Z][a-z]+", prefix)]
        # If ALL words are generic, this isn't a useful beach name — skip
        if all(w in _GENERIC_TOKENS for w in words):
            continue
        out.append(prefix)
    return out


def is_wrong_beach(alt_text: str, target_tokens: set[str]) -> bool:
    """Returns True if alt mentions a SPECIFIC beach that doesn't match target.

    - Target name appears anywhere in caption -> False (keep)
    - No specific beach mentioned -> False (keep — generic vibe shot)
    - All mentioned beach names are different from target -> True (reject)
    """
    if not alt_text or not target_tokens:
        return False
    # If target token appears anywhere in the text, keep
    text_words = {w.lower() for w in re.findall(r"[A-Za-z]+", alt_text)}
    if text_words & target_tokens:
        return False
    # No target match — see if any other "X Beach" is named
    extracted = extract_beach_names(alt_text)
    if not extracted:
        return False  # No specific beach claim, keep as generic
    return True  # Caption names a different specific beach


def haversine_m(la1: float, lo1: float, la2: float, lo2: float) -> float:
    from math import radians, sin, cos, asin, sqrt
    la1, lo1, la2, lo2 = map(radians, [la1, lo1, la2, lo2])
    a = sin((la2 - la1) / 2) ** 2 + cos(la1) * cos(la2) * sin((lo2 - lo1) / 2) ** 2
    return 2 * 6_371_000 * asin(sqrt(a))


# ═══════════════════════════════════════════════════════════════════════
# Pre-vision ranking (photo curation v3 — Franz 2026-05-19)
# Spec: docs/photo_curation_v3_spec.md
# ═══════════════════════════════════════════════════════════════════════

from datetime import datetime, timezone, timedelta

# Rare-content keyword overrides — title hits force-tag outside the cap.
RARE_KEYWORDS: set[str] = {
    # Dogs (Phase 1 hard-keep in get_beach_photos_diverse)
    "dog", "dogs", "puppy", "puppies", "pup", "pups", "doggo", "doggy",
    "doggie", "pooch", "hound", "canine",
    # Surf
    "surf", "surfer", "surfers", "surfing", "surfboard", "paddleboard",
    "kayak", "kayaking", "wave", "waves", "longboard", "kiteboard", "windsurf",
    # Atmosphere
    "sunset", "sunrise", "twilight", "dusk", "dawn",
    "fog", "foggy", "mist", "misty", "storm", "stormy",
    # Landscape — rare iconic features
    "driftwood", "tide pool", "tide pools", "tidepool", "cliff", "cliffs",
    "arch", "cave", "lagoon", "sandbar", "dune", "dunes", "bluff", "headland",
    # Path (NEW v3 — Franz: include more pics of paths)
    "path", "trail", "boardwalk", "walkway", "stairs",
    # Structure — iconic
    "pier", "lighthouse", "jetty",
}
# Two-word rare keywords (matched as substring after lower()).
RARE_PHRASES: set[str] = {"golden hour", "sea stack", "sea stacks"}

# Hard exclusion regex — these titles never enter the candidate pool.
# Extends the existing list in train_photo_model.py with v3 vehicle terms.
NEGATIVE_RE = re.compile(
    r"\b("
    # Wildlife close-ups + iNaturalist scientific specimens
    r"gull|tern|plover|sandpiper|larus|crab|anemone|barnacle|specimen|"
    r"crassadoma|meliscaeva|megapenthes|eristalis|evacanthus|dolichovespula|"
    r"inaturalist|"
    # Vehicles (v3 expansion per Franz 2026-05-19)
    r"car|cars|truck|trucks|train|rv|parking|highway|driveway|sedan|suv|"
    # Maps + abstract content
    r"map|chart|satellite|aerial|infographic|"
    # Event content (not dog-beach relevant)
    r"festival|tournament|triathlon|race|marathon|wedding|parade"
    r")\b",
    re.I,
)

# Source priorities — Type B (page-gallery, NULL distance) outranks Type A.
SOURCE_WEIGHT: dict[str, float] = {
    "ccc":       3.0,   # CA-only; highest curator-keep density (81%)
    "cdpr":      2.0,   # Type B
    "nps":       2.0,   # Type B
    "wikimedia": 1.0,   # Type A geo-tagged
    "flickr":    1.0,   # Type A geo-tagged
    "mapillary": 0.0,
    "unsplash":  0.0,
    "manual":    3.0,   # curator-uploaded
}

# Per-tier vision-tagging caps. dogs_allowed='no' overrides scoring_tier.
def cap_for_beach(scoring_tier: str | None, dogs_allowed: str | None) -> int:
    if (dogs_allowed or "").lower() == "no":
        return 0
    if scoring_tier == "hourly":
        return 15
    if scoring_tier == "daily":
        return 12
    return 10


def has_rare_keyword(title_text: str) -> bool:
    """True if title contains any rare-content keyword/phrase."""
    if not title_text:
        return False
    t = title_text.lower()
    # Word-boundary match for single tokens
    words = set(re.findall(r"[a-z]+", t))
    if words & RARE_KEYWORDS:
        return True
    # Substring match for multi-word phrases
    return any(phrase in t for phrase in RARE_PHRASES)


def title_excluded(title_text: str) -> bool:
    """True if title matches the NEGATIVE_RE hard-exclusion regex."""
    if not title_text:
        return False
    return bool(NEGATIVE_RE.search(title_text))


def score_photo(p: dict, photographer_kr: dict[str, tuple[float, int]] | None = None) -> float:
    """Pre-vision rank score for a single photo dict.

    Expected keys (caller normalizes):
      source           text     — 'flickr' / 'wikimedia' / etc.
      distance_m       float|None
      title_text       str      — title + attribution + caption concatenated
      photographer     str|None
      captured_at      datetime|None
      curator_touched  bool     — curated_at IS NOT NULL
    """
    score = 0.0

    # Curator-touched always wins
    if p.get("curator_touched"):
        score += 10.0

    # Source weight (Type B > Type A baseline)
    score += SOURCE_WEIGHT.get((p.get("source") or "").lower(), 0.0)

    # Distance bonus (Type A only — NULL distance gets +0, no penalty)
    d = p.get("distance_m")
    if d is not None:
        if d <= 200:    score += 2.0
        elif d <= 500:  score += 1.0
        # >500m is excluded upstream (in pre_vision_rank)

    # Title bonuses
    title = p.get("title_text") or ""
    if has_rare_keyword(title):
        score += 5.0
    elif re.search(r"\bbeach\b", title, re.I):
        score += 0.3

    # Photographer keep-rate (requires ≥5 prior decisions to count)
    pr = p.get("photographer")
    if pr and photographer_kr and pr in photographer_kr:
        rate, n = photographer_kr[pr]
        if n >= 5:
            if rate >= 0.80:   score += 1.0
            elif rate <= 0.20: score -= 1.0

    # Recency micro-bonus
    ca = p.get("captured_at")
    if ca:
        now = datetime.now(timezone.utc)
        if ca.tzinfo is None:
            ca = ca.replace(tzinfo=timezone.utc)
        age = now - ca
        if age <= timedelta(days=365 * 5):
            score += 0.02
        elif age <= timedelta(days=365 * 10):
            score += 0.01

    return score


def pre_vision_rank(
    photos: list[dict],
    beach_meta: dict,
    photographer_kr: dict[str, tuple[float, int]] | None = None,
) -> list[dict]:
    """Apply v3 hard exclusions + score + per-tier cap + rare-keyword override.

    Returns the subset of photos that should be vision-tagged, with each
    photo dict gaining `_rank_score` and `_rank_reason` keys (debug).

    beach_meta: {scoring_tier, dogs_allowed}
    """
    cap = cap_for_beach(beach_meta.get("scoring_tier"), beach_meta.get("dogs_allowed"))
    if cap == 0:
        return []   # no-dogs beaches skip vision tagging entirely

    eligible = []
    rare_hits = []
    for p in photos:
        title = p.get("title_text") or ""
        # Hard exclusions
        d = p.get("distance_m")
        if d is not None and d > 500:
            continue
        if title_excluded(title):
            continue
        # Score
        p["_rank_score"] = score_photo(p, photographer_kr)
        eligible.append(p)
        if has_rare_keyword(title):
            rare_hits.append(p)

    # Top-N by score
    eligible.sort(key=lambda x: x["_rank_score"], reverse=True)
    selected = eligible[:cap]

    # Force-include any rare-keyword hits not already in selected
    selected_ids = {id(x) for x in selected}
    for r in rare_hits:
        if id(r) not in selected_ids:
            selected.append(r)
            selected_ids.add(id(r))

    return selected


# ─── Self-test ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    cases = [
        # (beach name, alt text, expected_wrong)
        ("Crescent Bay Beach", "sunset on Martins Beach with rocks", True),
        ("Crescent Bay Beach", "Crescent Bay sunset", False),
        ("Crescent Bay Beach", "a view of a beach with clear blue water", False),
        ("Aliso Beach",        "Santa Cruz Beach Boardwalk", True),
        ("Aliso Beach",        "Beautiful Aliso Beach in California", False),
        ("Coronado Dog Beach", "Coronado Beach with people and dogs", False),
        ("Huntington Beach Dog Beach", "Huntington Beach Pier", False),
        ("Aliso Beach",        "View from Aliso toward Treasure Island Beach", False),  # mentions both
    ]
    for name, alt, exp in cases:
        toks = beach_name_tokens(name)
        got = is_wrong_beach(alt, toks)
        status = "OK " if got == exp else "FAIL"
        print(f"{status} {name!r} + {alt!r:55s} -> wrong={got} (expected {exp}, tokens={toks})")
