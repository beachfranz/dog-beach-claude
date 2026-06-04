"""DEPRECATED post Phase 5a (2026-06-04).

This module was the helper layer for the two-table operator model
(`public.operator` singular + `public.operators` plural, bridged by
`operators.canonical_operator_id`). Both the singular table AND the
bridge column were dropped in Phase 5a (commit 89d11d8) when the model
was unified into a single `public.operators` table with `is_canonical`
boolean.

All functions in this module reference dropped objects and will fail
on first DB call. Rather than silent breakage, they now raise
NotImplementedError immediately with a migration-path message.

MIGRATION GUIDE
===============

Old call                                          New pattern
─────────────────────────────────────────────────────────────────────────
get_canonical_operator(name='...')                SELECT * FROM public.operators
                                                  WHERE LOWER(canonical_name) = LOWER(%s)
                                                    AND is_canonical = true LIMIT 1

get_canonical_operator(id=N)                      SELECT * FROM public.operators
                                                  WHERE id = N AND is_canonical = true

get_extraction_operator(canonical_name='...')     Same query as get_canonical_operator
                                                  but WITHOUT the is_canonical filter
                                                  (since 'extraction pool' = whole table now)

canonical_for_extraction(extraction_id=N)         SELECT * FROM public.operators o
                                                  WHERE o.id = COALESCE(
                                                    (SELECT parent_operator_id FROM
                                                     public.operators WHERE id = N), N
                                                  ) AND o.is_canonical = true

extractions_for_canonical(canonical_id=N)         SELECT * FROM public.operators
                                                  WHERE parent_operator_id = N
                                                  OR (id = N AND is_canonical = true)

add_canonical_operator(name=..., op_type=...)     INSERT INTO public.operators
                                                  (canonical_name, slug, state_code, level,
                                                   is_canonical, is_active, origin_source,
                                                   ...) VALUES (...);
                                                  Single-table insert. No FK bridge.
                                                  Set is_canonical=true.

The semantic shift: "canonical vs extraction" is no longer two tables;
it's an `is_canonical` boolean on rows in one table. The cascade resolver
(`_resolve_beach_operator`) and `policy_source_effective_tier_for_beach`
all filter `op.is_canonical = true` to scope to the canonical subset.

See:
  - Phase 5a:  supabase/migrations/20260604_operator_unify_phase5a_drop_singular.sql
  - Phase 14d: supabase/migrations/20260604_phase14d_resolver_rewrite.sql
  - Phase 14h: supabase/migrations/20260604_phase14h_dog_park_rpcs_operator_repoint.sql

If you need these helpers as live functions again, redesign them around
the unified table. Do NOT silently re-introduce a two-table abstraction —
the unification was deliberate. Per `[[never-solve-same-problem-twice]]`.
"""
from __future__ import annotations
from typing import Any


__all__ = [
    "get_canonical_operator",
    "get_extraction_operator",
    "canonical_for_extraction",
    "extractions_for_canonical",
    "add_canonical_operator",
]


_DEPRECATION_MSG = (
    "scripts.common.operator was deprecated 2026-06-04 (Phase 5a). "
    "The public.operator (singular) table and operators.canonical_operator_id "
    "FK bridge were dropped. See the module docstring for the migration "
    "guide to direct SQL using public.operators + is_canonical filter."
)


def add_canonical_operator(
    *,
    name: str,
    op_type: str,
    state_code: str,
    short_name: str | None = None,
    web_url: str | None = None,
    metadata: dict | None = None,
    plural_level: str = "private",
    plural_subtype: str | None = "ngo",
    plural_slug: str | None = None,
    notes: str | None = None,
    apply: bool = False,
) -> dict:
    """DEPRECATED — see module docstring."""
    raise NotImplementedError(_DEPRECATION_MSG)


def get_canonical_operator(
    *,
    id: int | None = None,
    name: str | None = None,
    short_name: str | None = None,
) -> dict[str, Any] | None:
    """DEPRECATED — see module docstring."""
    raise NotImplementedError(_DEPRECATION_MSG)


def get_extraction_operator(
    *,
    id: int | None = None,
    canonical_name: str | None = None,
    slug: str | None = None,
) -> dict[str, Any] | None:
    """DEPRECATED — see module docstring."""
    raise NotImplementedError(_DEPRECATION_MSG)


def canonical_for_extraction(extraction_id: int) -> dict[str, Any] | None:
    """DEPRECATED — see module docstring."""
    raise NotImplementedError(_DEPRECATION_MSG)


def extractions_for_canonical(canonical_id: int) -> list[dict[str, Any]]:
    """DEPRECATED — see module docstring."""
    raise NotImplementedError(_DEPRECATION_MSG)
