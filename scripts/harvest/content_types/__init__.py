"""Pluggable content_type modules for the harvest framework.

Each module exposes:

    CONTENT_TYPE: str                    -- check-constraint string
    EXTRACTION_PROMPT: str               -- LLM system prompt
    ATTRIBUTES_FIELDS: list[str]         -- JSONB attribute keys this content_type populates
    SIBLING_URL_HINTS: list[str]         -- candidate sub-paths to try (e.g. '/dog-parks', '/dogparks')
    INVENTORY_TABLE: str                 -- source-of-truth table this content_type compares against
    inventory_match_query(cursor, row)   -- per-row match logic
"""
from . import dog_park

REGISTRY = {
    dog_park.CONTENT_TYPE: dog_park,
}


def get(content_type: str):
    if content_type not in REGISTRY:
        raise KeyError(f"Unknown content_type: {content_type}. Available: {list(REGISTRY)}")
    return REGISTRY[content_type]
