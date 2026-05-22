"""Shared helpers used across pipeline scripts.

Started 2026-05-22 as consolidation move #2 from the clean-pipeline audit.
Expanded on the clean-pipeline-v2 branch to cover the Day-1 foundation
modules (db, llm, supa) per the audit's Top-10.

Modules
=======
- text_cleaning : normalize_text, slugify, normalize_place_name, ...
- db            : connect, thread_conn (replaces 21+ verbatim _connect copies)
- llm           : SONNET/HAIKU/OPUS model registry, sdk_client, call, call_json
- supa          : unified PostgREST client (replaces 12 drifting supa impls)

Side effect at import: inject truststore into SSL (Windows AV-MITM
compatibility — same pivot as merge_operator_dogs_policy + Mapillary
loader + pipeline runner). Scripts that `import scripts.common` (or
anything from it) get truststore for free; no need to remember per-script.

Adding new helpers
==================
Build the canonical version here first, then migrate existing duplicate
implementations to import from this module. Don't add helpers nobody
uses — extract from real call sites.
"""
# Windows AV-MITM cert verification blocks default SSL. Inject truststore
# so any urllib/httpx/requests call in any script that touches `common`
# uses the system trust store. Side-effect at import time is intentional.
try:
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

from scripts.common.text_cleaning import (
    normalize_text,
    slugify,
    normalize_place_name,
    collapse_whitespace,
    strip_accents,
    PLACE_NAME_SUFFIXES,
)

__all__ = [
    # text_cleaning
    "normalize_text",
    "slugify",
    "normalize_place_name",
    "collapse_whitespace",
    "strip_accents",
    "PLACE_NAME_SUFFIXES",
]
# db/llm/supa are NOT re-exported at top level — import explicitly:
#   from scripts.common.db import connect, thread_conn
#   from scripts.common.llm import SONNET, call_json, sdk_client
#   from scripts.common.supa import supa
# This keeps the namespace tidy and makes call sites explicit about which
# subsystem they're using.
