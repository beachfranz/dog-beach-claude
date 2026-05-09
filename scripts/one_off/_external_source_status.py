"""Helper: upsert into public.external_source_status from bulk loaders."""
from __future__ import annotations
import psycopg2


def record_status(pg_kwargs: dict, source: str, state: str,
                  status: str, row_count: int | None = None,
                  notes: str | None = None) -> None:
    """Upsert a (source, state) row. Caller passes a psycopg2 connection
    config dict (same shape as PG in the bulk loaders).

    status must be one of 'ok' | 'failed' | 'skipped'.
    last_loaded_at is bumped to now() on every call.
    """
    if status not in ('ok', 'failed', 'skipped'):
        raise ValueError(f'bad status: {status!r}')
    with psycopg2.connect(**pg_kwargs) as c:
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("""
                insert into public.external_source_status
                  (source, state, last_loaded_at, row_count, status, notes)
                values (%s, %s, now(), %s, %s, %s)
                on conflict (source, state) do update set
                  last_loaded_at = excluded.last_loaded_at,
                  row_count      = excluded.row_count,
                  status         = excluded.status,
                  notes          = excluded.notes
            """, (source, state, row_count, status, notes))
