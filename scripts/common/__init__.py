"""Shared helpers used across pipeline scripts.

Started 2026-05-22 as consolidation move #2 from the clean-pipeline audit.
Goal: eliminate the 8+ duplicate implementations of text normalization,
slugification, and place-name handling identified in the audit.

Modules:
  - text_cleaning: normalize_text, slugify, normalize_place_name,
                   collapse_whitespace, strip_accents

Adding new helpers
==================
Build the canonical version here first, then migrate existing duplicate
implementations to import from this module. Don't add helpers nobody
uses — extract from real call sites.
"""
from scripts.common.text_cleaning import (
    normalize_text,
    slugify,
    normalize_place_name,
    collapse_whitespace,
    strip_accents,
    PLACE_NAME_SUFFIXES,
)

__all__ = [
    "normalize_text",
    "slugify",
    "normalize_place_name",
    "collapse_whitespace",
    "strip_accents",
    "PLACE_NAME_SUFFIXES",
]
