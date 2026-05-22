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
# Side effects at package import: (1) load .env so submodules can read
# os.environ[...] at their own module-load time; (2) inject truststore so
# urllib/httpx/requests calls use the system trust store (Windows AV-MITM).
# Both are intentional — centralizes the boilerplate every script used to carry.
from pathlib import Path as _Path
from dotenv import load_dotenv as _load_dotenv
_load_dotenv(_Path(__file__).resolve().parent.parent / "pipeline" / ".env")

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
