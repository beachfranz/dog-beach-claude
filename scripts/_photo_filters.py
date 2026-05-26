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

# ═══════════════════════════════════════════════════════════════════════
# Consolidated term lists (merged from load_flickr_photos.py +
# load_wikimedia_commons_photos.py 2026-05-19 per Franz "do the merge").
# These are now the canonical lists; loaders import from here.
# ═══════════════════════════════════════════════════════════════════════

POSITIVE_TERMS_DOG: list[str] = [
    "dog", "dogs", "doggie", "doggy",
    "puppy", "pup", "pups", "puppies",
    "canine", "pooch", "hound",
]
POSITIVE_TERMS_GENERIC: list[str] = [
    "sand", "shore",
    "leash", "leashed", "off-leash", "off leash",
    "rules", "regulations", "regulation",
    "permitted", "allowed", "prohibited",
    "ordinance",
]
POSITIVE_TERMS: list[str] = POSITIVE_TERMS_DOG + POSITIVE_TERMS_GENERIC

# Per-entity positive supplements (Franz 2026-05-26 photo-filter split).
# Dog parks favor infrastructure / equipment / off-leash-area terms;
# 'sand'/'shore' drop because most dog parks are grass/mulch/turf.
POSITIVE_TERMS_DOG_PARK_EXTRA: list[str] = [
    # Infrastructure
    "fence", "fenced", "gate", "double gate", "double-gate",
    "chain link", "vestibule",
    # Equipment / amenities
    "agility", "tunnel", "a-frame", "weave", "jump",
    "play area", "bench", "water bowl", "pet water", "drinking fountain",
    # Naming
    "bark park", "dog park", "dog run", "off-leash area", "ola", "paw",
    # Subgroups
    "small dog", "large dog", "sm-dog", "lg-dog",
    # Surface
    "mulch", "wood chips", "turf", "grass", "shade",
]


def positive_terms_for(entity: str) -> tuple[list[str], list[str]]:
    """Returns (positive_dog_terms, positive_generic_terms) for an entity.

    Dog parks drop sand/shore (beach signal) and gain the DOG_PARK_EXTRA
    list. Dog-specific terms (dog/puppy/etc.) apply to both entities.
    """
    if entity == "dog_park":
        # Drop sand/shore — not load-bearing for grass/dirt dog parks
        generic = [t for t in POSITIVE_TERMS_GENERIC if t not in {"sand", "shore"}]
        generic = generic + POSITIVE_TERMS_DOG_PARK_EXTRA
        return POSITIVE_TERMS_DOG, generic
    return POSITIVE_TERMS_DOG, POSITIVE_TERMS_GENERIC

# Merged from both loaders' NEGATIVE_TERMS + the prior centralized NEGATIVE_RE.
# Anything matching these (word-boundary, case-insensitive) is hard-excluded
# at ingest. Photos won't even enter the candidate pool.
NEGATIVE_TERMS: list[str] = [
    # ── Vehicles / infrastructure (Franz v3 expansion) ─────────────────
    "train", "railway", "railroad", "locomotive",
    "car", "cars", "truck", "trucks", "vehicle", "automobile",
    "pontiac", "chevrolet", "ford", "toyota", "honda",
    "parking lot", "parking", "driveway", "highway",
    "sedan", "suv", "rv",
    # ── Wildlife / sea creatures (specimen photos clutter the gallery) ─
    # Marine mammals deliberately NOT here (whale/dolphin/porpoise are
    # beach spectacle — Franz 2026-05-19 "drop the mammals").
    "fish", "crab", "lobster", "shrimp",
    "octopus", "squid", "cuttlefish",
    "jellyfish", "sea nettle", "sea jelly",
    "starfish", "sea star",
    "urchin", "anemone", "barnacle",
    "scallop", "mussel", "clam", "oyster", "abalone",
    "sea cucumber", "sea spider", "sea slug", "sea worm",
    "nudibranch", "mollusk", "crustacean", "specimen",
    "coral", "plankton",
    # ── Birds (specimen photos) ────────────────────────────────────────
    "bird", "birds", "gull", "seagull", "larus",
    "pelican", "cormorant", "heron", "egret",
    "tern", "plover", "sandpiper", "shorebird",
    "duck", "goose", "swan", "raptor", "hawk", "osprey",
    "curlew", "willet", "godwit", "sanderling", "phalarope",
    "inaturalist",
    # ── Latin genus names for specimen photos ──────────────────────────
    "apostichopus", "pycnogonum", "ophioderma", "crassadoma",
    "platynereis", "numenius", "phalacrocorax", "pelecanus",
    "haliaeetus", "calidris", "limosa", "americanus",
    "panamense", "californicus", "bicanaliculata",
    "astropecten", "amphistichus", "verrilli", "armatus", "koelzi",
    "surfperch", "sand star", "spiny sand", "calico surf",
    "dolichovespula", "meliscaeva", "megapenthes",
    "eristalis", "evacanthus",
    # ── Pure-graphic / non-photo ───────────────────────────────────────
    "map", "diagram", "chart", "logo", "plaque",
    "satellite", "aerial", "infographic",
    "construction", "interior", "screenshot",
    # ── Event content (mostly Flickr-noise per 2026-05-11 pilot) ───────
    "festival", "tournament", "competition", "triathlon",
    "race", "marathon", "concert", "wedding",
    "parade", "fundraiser",
]

# Compile to a single word-boundary regex from NEGATIVE_TERMS. Multi-word
# phrases (e.g. "parking lot", "sea star") are matched as substrings
# (the \b at edges still works on the outermost word boundaries).
def _compile_negative_re(terms: list[str]) -> re.Pattern:
    # Escape each term + sort longest-first so multi-word phrases match
    # before any embedded shorter word.
    escaped = sorted({re.escape(t) for t in terms}, key=len, reverse=True)
    pattern = r"\b(" + "|".join(escaped) + r")\b"
    return re.compile(pattern, re.I)

NEGATIVE_RE = _compile_negative_re(NEGATIVE_TERMS)

# Per-entity negative supplements (Franz 2026-05-26 photo-filter split).
# Dog parks: catch indoor restaurant/cafe/yard-house false positives + art
# installation hits (Canton "Puppy Park" sculpture park) + baby/birthday
# event photos. Drop bird/gull/seagull negatives — a dog chasing a bird at
# a park IS legitimate content; bird specimen Latin names stay negative.
DOG_PARK_NEGATIVE_REMOVALS: set[str] = {
    "bird", "birds", "gull", "seagull", "larus",
    "pelican", "cormorant", "heron", "egret",
    "tern", "plover", "sandpiper", "shorebird",
    "duck", "goose", "swan", "raptor", "hawk", "osprey",
    "curlew", "willet", "godwit", "sanderling", "phalarope",
}
DOG_PARK_NEGATIVE_ADDITIONS: list[str] = [
    # Indoor / food / venue false-positives
    "restaurant", "cafe", "coffee shop", "bar", "menu",
    "yard house",
    # Event / people false-positives
    "baby", "babies", "birthday", "wedding party", "bridal",
    # Art installations (Canton "Puppy Park" sculpture park 2026-05-26)
    "art installation", "sculpture", "mural", "gallery",
    "puppy park",   # specific: Canton's pink-poodle/roller-coaster art piece
    # Non-dog pets (puppy mistagged as kitten owners' pet photos)
    "cat", "kitten", "parakeet", "parrot",
]

NEGATIVE_TERMS_DOG_PARK: list[str] = (
    [t for t in NEGATIVE_TERMS if t not in DOG_PARK_NEGATIVE_REMOVALS]
    + DOG_PARK_NEGATIVE_ADDITIONS
)
NEGATIVE_RE_DOG_PARK = _compile_negative_re(NEGATIVE_TERMS_DOG_PARK)


def negative_re_for(entity: str) -> re.Pattern:
    return NEGATIVE_RE_DOG_PARK if entity == "dog_park" else NEGATIVE_RE


def entity_name_keyword(entity: str) -> str:
    """Word for score_photo's name-bonus boost — 'beach' vs 'dog park'."""
    return "dog park" if entity == "dog_park" else "beach"


# ═══════════════════════════════════════════════════════════════════════
# ENTITIES — single source of truth for per-loader entity dispatch.
# Every loader imports this and uses ENTITIES[args.entity][...] for
# table / FK / RPC / curate decisions. Adding a new entity = one entry
# here; loaders should never re-declare these per-file.
# ═══════════════════════════════════════════════════════════════════════
ENTITIES = {
    "beach": {
        "table":             "beaches_gold",
        "fk_col":            "arena_group_id",
        "photo_table":       "beach_photos",
        "select_fields":     "fid,name,display_name_override,county_name,state,scoring_tier",
        "has_lat_lon":       False,    # lat/lng via get_beach_info RPC
        "lat_lon_rpc":       "get_beach_info",
        "supports_agencies":  True,
        "supports_curator_rpcs": True,  # blocked_photographers + rejected RPC
        "default_query_kw":  "beach",
        "auto_curate_top":   0,         # admin curator UI gates display
    },
    "dog_park": {
        "table":             "dog_parks_gold",
        "fk_col":            "dog_park_fid",
        "photo_table":       "dog_park_photos",
        "select_fields":     "fid,name,display_name_override,address_city,state,lat,lon",
        "has_lat_lon":       True,     # dog_parks_gold has lat + lon columns
        "lat_lon_rpc":       None,
        "supports_agencies":  False,
        "supports_curator_rpcs": False,
        "default_query_kw":  "dog park",
        "auto_curate_top":   0,        # vision-tag pipeline + dp_photos_curate gates display
    },
}

# Source priorities — Type B (page-gallery, NULL distance) outranks Type A.
SOURCE_WEIGHT: dict[str, float] = {
    "ccc":       3.0,   # CA-only; highest curator-keep density (81%)
    "cdpr":      2.0,   # Type B
    "nps":       2.0,   # Type B
    "wsprc":     2.0,   # Type B — parks.wa.gov page galleries (2026-05-19)
    "flickr":    1.3,   # Type A geo-tagged. Small bonus over wikimedia because
                        # Flickr fetches parallelize cleanly (no per-host serial
                        # lock); wikimedia is serial-locked + 1.5s paced per
                        # the wikimedia-integration pin. Same quality bar,
                        # better pipeline throughput. Per Franz 2026-05-19.
    "wikimedia": 1.0,   # Type A geo-tagged
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


def title_excluded(title_text: str, entity: str = "beach") -> bool:
    """True if title matches the entity's negative-term regex."""
    if not title_text:
        return False
    return bool(negative_re_for(entity).search(title_text))


def score_photo(p: dict, photographer_kr: dict[str, tuple[float, int]] | None = None,
                entity: str = "beach") -> float:
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
    else:
        kw = entity_name_keyword(entity)  # "beach" or "dog park"
        if re.search(rf"\b{re.escape(kw)}\b", title, re.I):
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
    entity: str = "beach",
) -> list[dict]:
    """Unified ingest filter (Franz 2026-05-19 collapsed architecture):
    Apply hard exclusions + score + per-tier cap + rare-keyword override.

    Used at LOAD time by every photo loader (Flickr, Wikimedia, etc.) so
    only survivors land in beach_photos. There's no second post-load filter
    — vision tagging runs on everything inserted.

    Distance policy (mirrors prior Flickr dog-loose pattern per
    [[loose-radius-dog-filter]]):
      - default hard cap: 500m
      - rare-keyword titles (dog/surf/sunset/etc.): hard cap 2000m
        (catches Del Mar Dog Beach -style event/cluster photos at the
        parking lot/trail head)

    Returns the subset of photos to ingest, with each gaining a
    `_rank_score` key (debug).

    beach_meta: {scoring_tier, dogs_allowed}
    """
    # Dog parks: no scoring_tier (all definitionally dogs-allowed); use cap=15.
    if entity == "dog_park":
        cap = 15
    else:
        cap = cap_for_beach(beach_meta.get("scoring_tier"), beach_meta.get("dogs_allowed"))
        if cap == 0:
            return []   # no-dogs beaches skip vision tagging entirely

    eligible = []
    rare_hits = []
    for p in photos:
        title = p.get("title_text") or ""
        d = p.get("distance_m")
        rare = has_rare_keyword(title)
        # Hard exclusions (entity-specific NEGATIVE_RE)
        if title_excluded(title, entity):
            continue
        if d is not None:
            if rare:
                if d > 2000: continue    # dog-loose-radius escape for rare content
            else:
                if d > 500:  continue    # tight default
        # Score
        p["_rank_score"] = score_photo(p, photographer_kr, entity)
        eligible.append(p)
        if rare:
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
