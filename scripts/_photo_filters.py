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
