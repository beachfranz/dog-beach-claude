"""Canonical Supabase REST (PostgREST) client.

Replaces 12 drifting `supa()` impls identified by the 2026-05-22 audit.
Two prior signatures existed (photo-loader style + codify style); this
module unifies them.

Usage
=====
    from scripts.common.supa import supa

    # SELECT (default)
    rows = supa("/rest/v1/beaches_gold",
                params={"select": "fid,name", "state": "eq.CA", "limit": "10"})

    # INSERT
    supa("/rest/v1/beach_photos", method="POST",
         body={"arena_group_id": 12345, "source": "manual", ...})

    # UPSERT (PostgREST resolution=merge-duplicates)
    supa("/rest/v1/beach_dog_policy", method="POST",
         body={...}, upsert=True)

    # PATCH (UPDATE)
    supa(f"/rest/v1/beaches_gold?fid=eq.{fid}", method="PATCH",
         body={"description": "..."})

Caller-side conventions
=======================
- Always include `apikey` + `Authorization` via SERVICE_KEY (handled here).
- `Prefer: return=representation` is the default — POST/PATCH returns the
  affected rows. Override with `prefer="return=minimal"` if you don't need
  the body.
- Errors raise `RuntimeError` with the status code + first 300 chars of
  the response body for debugging.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]


def supa(
    path: str,
    *,
    method: str = "GET",
    params: dict | None = None,
    body: dict | list | None = None,
    prefer: str | list[str] | None = None,
    upsert: bool = False,
    timeout: int = 60,
) -> Any:
    """PostgREST request. Returns parsed JSON (list/dict) or None on empty.

    `path` should be relative (e.g. `/rest/v1/beaches_gold`). The base
    `SUPABASE_URL` is prepended.

    `params` dict is URL-encoded with safe punctuation kept for PostgREST
    operators (e.g. `eq.X`, `in.(a,b)`, `gt.5`).

    `prefer` can be a string OR list of strings (joined with ','). Setting
    `upsert=True` adds `resolution=merge-duplicates`.

    Raises `RuntimeError` for non-2xx responses with status + body excerpt.
    """
    url = f"{SUPABASE_URL}{path}"
    if params:
        url = url + "?" + urllib.parse.urlencode(params, safe=",.()*=:")

    headers = {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
    }

    req_body: bytes | None = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        req_body = json.dumps(body).encode("utf-8")

    if body is not None or method in ("PATCH", "PUT", "DELETE"):
        prefers: list[str] = []
        if prefer is None:
            prefers.append("return=representation")
        elif isinstance(prefer, str):
            prefers.append(prefer)
        else:
            prefers.extend(prefer)
        if upsert:
            prefers.append("resolution=merge-duplicates")
        headers["Prefer"] = ",".join(prefers)

    req = urllib.request.Request(url, method=method, data=req_body)
    for k, v in headers.items():
        req.add_header(k, v)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            if not raw:
                return None
            return json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode("utf-8", "ignore")[:300]
        raise RuntimeError(
            f"Supabase {method} {path} → HTTP {e.code}: {body_txt}"
        ) from None
