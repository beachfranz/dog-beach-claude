"""Typed accessors for the operator vs operators table choice.

The two tables look like duplicates because of their names, but they
serve DIFFERENT roles:

  public.operator   (singular, ~159 rows) — hand-curated consumer-surface
                                              registry. Cascade-wired via
                                              entity_operator + policy_source
                                              .issuing_operator_id.
                                              "Who shows up on beach.html?"

  public.operators  (plural, ~9,275 rows)  — bulk-extracted from TIGER cities,
                                              CPAD units, OSM. Most rows are
                                              level=city. Used by the
                                              operator_llm_extract pipeline.
                                              "Where do extractions attach?"

Both are load-bearing. Don't merge. See:
  - HARD memory pin: operate-two-table-finding
  - Bridge FK migration: 20260522_operators_canonical_bridge.sql
  - Table comments: 20260522_operator_table_comments.sql

These helpers exist so new code doesn't have to remember which table to
query. Prefer them over raw SQL where possible. They're thin — they don't
hide the underlying tables, just disambiguate the choice at the call site.

USAGE
=====

    from scripts.common.operator import (
        get_canonical_operator,
        get_extraction_operator,
        canonical_for_extraction,
        extractions_for_canonical,
    )

    # I want the consumer-surface operator row (operator singular):
    op = get_canonical_operator(name='Crystal Cove Conservancy')
    op = get_canonical_operator(id=8)

    # I want the extraction-pool operator row (operators plural):
    ext = get_extraction_operator(canonical_name='City of Half Moon Bay')
    ext = get_extraction_operator(id=42)

    # Cross-bridge: given an extraction row, find its consumer-surface
    # counterpart (via the formal FK, NOT by name match):
    op = canonical_for_extraction(extraction_id=42)

    # Reverse-bridge: given a consumer-surface operator, find all
    # extraction-pool rows that point at it (may be 0, 1, or many):
    exts = extractions_for_canonical(canonical_id=8)
"""
from __future__ import annotations
from typing import Any

from scripts.common.db import connect


__all__ = [
    "get_canonical_operator",
    "get_extraction_operator",
    "canonical_for_extraction",
    "extractions_for_canonical",
    "add_canonical_operator",
]


# ─── Writers ─────────────────────────────────────────────────────────

def add_canonical_operator(
    *,
    name: str,
    op_type: str,          # operator.type — 'nonprofit', 'city', 'county', 'state', 'federal_department', 'city_department', 'special_district'
    state_code: str,       # operators.state_code — 'CA', 'NH', 'WA', etc.
    short_name: str | None = None,
    web_url: str | None = None,
    metadata: dict | None = None,
    plural_level: str = "private",  # operators.level — 'private' is the right default for nonprofits
    plural_subtype: str | None = "ngo",
    plural_slug: str | None = None,  # auto-derived from name if None
    notes: str | None = None,
    apply: bool = False,
) -> dict:
    """Atomically add a new operator to BOTH public.operator (singular) AND
    public.operators (plural), with the canonical_operator_id FK set.

    Per Franz 2026-05-22 LATE decision: new-state nonprofits + sub-agencies
    MUST be in both tables so the bridge FK works consistently. Avoids the
    4-orphan pattern that CA accidentally accumulated.

    Idempotent: if an operator (singular) row with the same name already
    exists, that one is reused (no duplicate insert).

    Default: dry-run (returns what WOULD be inserted; no writes). Pass
    apply=True to commit.

    Args
    ----
    name           : Canonical name. Used for both operator.name and
                     operators.canonical_name.
    op_type        : operator.type enum (nonprofit, city, county, state, …).
    state_code     : 2-letter state code for operators.state_code.
    short_name     : Optional short name shared by both tables.
    web_url        : Optional URL for operator.web_url.
    metadata       : Optional jsonb for operator.metadata (e.g.,
                     {'created_for': 'NH-launch', 'agent_verified': True}).
    plural_level   : operators.level enum — default 'private' (right for
                     nonprofits). Use 'city' for city sub-departments,
                     'county' for county sub-agencies, etc.
    plural_subtype : operators.subtype — default 'ngo' (right for
                     nonprofits). Use 'special-district' for sub-agencies.
    plural_slug    : Optional slug for operators.slug. If None, auto-derived
                     from name.
    notes          : Optional note carried on the operators (plural) row.

    Returns
    -------
    dict with keys: 'singular_id', 'plural_id', 'applied' (bool),
                    'singular_inserted' (bool — false when reused).
    """
    import re
    if plural_slug is None:
        # crude but consistent — lowercase, dashes, strip non-alnum
        plural_slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

    with connect() as c, c.cursor() as cur:
        # 1. Find or insert singular
        cur.execute(
            "SELECT id FROM public.operator WHERE LOWER(name) = LOWER(%s)",
            (name,),
        )
        row = cur.fetchone()
        if row:
            singular_id = row[0]
            singular_inserted = False
        else:
            if not apply:
                # In dry-run, fabricate a placeholder; don't actually insert
                singular_id = -1
                singular_inserted = True
            else:
                import json
                cur.execute(
                    "INSERT INTO public.operator "
                    "  (name, type, short_name, web_url, metadata) "
                    "VALUES (%s, %s, %s, %s, %s::jsonb) RETURNING id",
                    (name, op_type, short_name, web_url,
                     json.dumps(metadata or {})),
                )
                singular_id = cur.fetchone()[0]
                singular_inserted = True

        # 2. Check if plural row already exists for this singular
        if singular_id > 0:
            cur.execute(
                "SELECT id FROM public.operators WHERE canonical_operator_id = %s",
                (singular_id,),
            )
            existing = cur.fetchone()
            if existing:
                # Already paired — return existing
                if apply:
                    c.commit()
                return {
                    "singular_id": singular_id,
                    "plural_id": existing[0],
                    "applied": apply,
                    "singular_inserted": singular_inserted,
                    "plural_inserted": False,
                }

        # 3. Insert plural row with FK back to singular
        if not apply:
            return {
                "singular_id": singular_id,
                "plural_id": -1,
                "applied": False,
                "singular_inserted": singular_inserted,
                "plural_inserted": True,
                "would_insert_plural": {
                    "slug": plural_slug, "canonical_name": name,
                    "short_name": short_name, "level": plural_level,
                    "subtype": plural_subtype, "state_code": state_code,
                    "origin_source": "manual",
                    "canonical_operator_id": singular_id,
                    "notes": notes,
                },
            }

        cur.execute(
            "INSERT INTO public.operators "
            "  (slug, canonical_name, short_name, level, subtype, state_code, "
            "   origin_source, is_active, canonical_operator_id, notes) "
            "VALUES (%s, %s, %s, %s, %s, %s, 'manual', true, %s, %s) "
            "RETURNING id",
            (plural_slug, name, short_name, plural_level, plural_subtype,
             state_code, singular_id, notes),
        )
        plural_id = cur.fetchone()[0]
        c.commit()

        return {
            "singular_id": singular_id,
            "plural_id": plural_id,
            "applied": True,
            "singular_inserted": singular_inserted,
            "plural_inserted": True,
        }


# ─── Direct accessors ────────────────────────────────────────────────

def get_canonical_operator(
    *,
    id: int | None = None,
    name: str | None = None,
    short_name: str | None = None,
) -> dict[str, Any] | None:
    """Fetch ONE row from public.operator (singular, curated consumer
    surface). Pass exactly one of `id`, `name`, `short_name`.

    Returns dict (column → value) or None if no match.
    """
    n_args = sum(x is not None for x in (id, name, short_name))
    if n_args != 1:
        raise ValueError(
            "get_canonical_operator: pass exactly one of id / name / short_name "
            f"(got {n_args})"
        )
    with connect() as c, c.cursor() as cur:
        if id is not None:
            cur.execute("SELECT * FROM public.operator WHERE id = %s", (id,))
        elif name is not None:
            cur.execute(
                "SELECT * FROM public.operator WHERE LOWER(name) = LOWER(%s) "
                "LIMIT 1",
                (name,),
            )
        else:  # short_name
            cur.execute(
                "SELECT * FROM public.operator WHERE LOWER(short_name) = LOWER(%s) "
                "LIMIT 1",
                (short_name,),
            )
        row = cur.fetchone()
        if not row:
            return None
        return dict(zip([d[0] for d in cur.description], row))


def get_extraction_operator(
    *,
    id: int | None = None,
    canonical_name: str | None = None,
    slug: str | None = None,
) -> dict[str, Any] | None:
    """Fetch ONE row from public.operators (plural, bulk extraction pool).
    Pass exactly one of `id`, `canonical_name`, `slug`.

    Returns dict (column → value) or None if no match.
    """
    n_args = sum(x is not None for x in (id, canonical_name, slug))
    if n_args != 1:
        raise ValueError(
            "get_extraction_operator: pass exactly one of id / canonical_name / "
            f"slug (got {n_args})"
        )
    with connect() as c, c.cursor() as cur:
        if id is not None:
            cur.execute("SELECT * FROM public.operators WHERE id = %s", (id,))
        elif canonical_name is not None:
            cur.execute(
                "SELECT * FROM public.operators WHERE LOWER(canonical_name) = LOWER(%s) "
                "LIMIT 1",
                (canonical_name,),
            )
        else:  # slug
            cur.execute(
                "SELECT * FROM public.operators WHERE slug = %s LIMIT 1",
                (slug,),
            )
        row = cur.fetchone()
        if not row:
            return None
        return dict(zip([d[0] for d in cur.description], row))


# ─── Bridge accessors (use the formal FK; never LOWER-name) ──────────

def canonical_for_extraction(extraction_id: int) -> dict[str, Any] | None:
    """Given an extraction-pool operators.id, return the canonical
    operator row via the formal FK (operators.canonical_operator_id).

    Returns dict or None if no canonical counterpart exists (most TIGER
    cities — they have no consumer-surface presence).
    """
    with connect() as c, c.cursor() as cur:
        cur.execute(
            "SELECT op.* FROM public.operator op "
            "JOIN public.operators o ON o.canonical_operator_id = op.id "
            "WHERE o.id = %s",
            (extraction_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return dict(zip([d[0] for d in cur.description], row))


def extractions_for_canonical(canonical_id: int) -> list[dict[str, Any]]:
    """Given a canonical operator.id, return ALL extraction-pool rows
    (operators.canonical_operator_id = canonical_id). Most singular
    operators have 0 or 1 extraction-pool counterparts; a few have 2+
    (variant names like "Long Beach" + "City of Long Beach").

    Returns list of dicts (possibly empty).
    """
    with connect() as c, c.cursor() as cur:
        cur.execute(
            "SELECT * FROM public.operators WHERE canonical_operator_id = %s "
            "ORDER BY id",
            (canonical_id,),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]
