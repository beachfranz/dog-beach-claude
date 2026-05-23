"""Canonical Postgres connection helpers.

Replaces the 21+ verbatim copies of `_connect()` across scripts identified
by the 2026-05-22 audit. Loads .env, parses pooler-url, returns a
psycopg2 connection. Plus `thread_conn()` for ThreadPoolExecutor scripts.

Usage
=====
    from scripts.common.db import connect, thread_conn

    # Foreground / single-threaded
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")

    # ThreadPoolExecutor (per-thread connection, cached on threading.local)
    def worker(item):
        conn = thread_conn()
        with conn.cursor() as cur:
            ...
"""
from __future__ import annotations

import os
import threading
import urllib.parse
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# Repo root is two parents up: scripts/common/db.py → repo
ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

POOLER_URL = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
_PP = urllib.parse.urlparse(POOLER_URL)

PG_KWARGS: dict[str, str | int] = dict(
    host=_PP.hostname,
    port=_PP.port or 5432,
    user=_PP.username,
    password=os.environ["SUPABASE_DB_PASSWORD"],
    dbname=(_PP.path or "/postgres").lstrip("/"),
    sslmode="require",
)


def connect(**overrides):
    """Open a fresh psycopg2 connection to the pooler.

    Pass overrides to merge into PG_KWARGS (e.g., `connect(application_name='my_script')`).
    Caller owns the connection — commit/rollback/close as appropriate.
    """
    return psycopg2.connect(**{**PG_KWARGS, **overrides})


_TLS = threading.local()


def thread_conn(**overrides):
    """Per-thread cached connection. Auto-reconnects if closed OR if a ping
    fails (pgbouncer-killed-from-other-side detection).

    Caller is responsible for commit/rollback. The connection persists for
    the thread's lifetime; let the thread end (or call `close_thread_conn()`)
    to release.

    pgbouncer session-mode pool ceiling is 15 — keep ThreadPoolExecutor
    max_workers <= 10 to leave headroom.

    2026-05-23 LATE: added SELECT 1 ping. psycopg2's `conn.closed` is only
    set when WE explicitly close. If pgbouncer kills the conn from the
    other side, `closed` stays 0 until the next query — which then fails
    with OperationalError mid-work. The ping catches this BEFORE work
    starts. Diagnosed during DE virgin-state pipeline test —
    backfill_agency_photo_centroid.py died at 72s in a per-thread UPDATE
    after pgbouncer dropped an idle connection.
    """
    c = getattr(_TLS, "conn", None)
    if c is not None and not c.closed:
        try:
            with c.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return c
        except Exception:
            # pgbouncer killed it; fall through to reconnect
            try:
                c.close()
            except Exception:
                pass
            _TLS.conn = None
    c = connect(**overrides)
    _TLS.conn = c
    return c


def close_thread_conn():
    """Close + clear the per-thread cached connection. Optional."""
    c = getattr(_TLS, "conn", None)
    if c is not None and not c.closed:
        c.close()
    _TLS.conn = None
