"""
discover_urls.py — URL discovery MVP for the unified pipeline (Layer D).

Per the unified-v1 probe (n=98, 2026-05-04): URL pool starvation is the
dominant constraint on extraction coverage. This script discovers
beach-specific URLs via web search and queues them in `discovered_urls`,
where extract_for_beach.gather_urls() picks them up.

Two backends, auto-selected:
  1. Brave Search API (preferred) — set BRAVE_API_KEY in pipeline/.env.
     Free tier 2,000 queries/month; $3/1K beyond. Sign up:
     https://api.search.brave.com/app/keys
  2. Anthropic web_search tool (fallback) — uses existing ANTHROPIC_API_KEY.
     ~$10/1K searches but works immediately.

Usage:
  # Discover for the 85-beach scoreable extraction-gap surface
  python scripts/discover_urls.py --gap-only --apply

  # Specific fids
  python scripts/discover_urls.py --fids 9716,3671,8474 --apply

  # Dry-run (no API calls, just show plan)
  python scripts/discover_urls.py --gap-only --dry-run

Outputs land in `discovered_urls` table; validate by fetching + running
the existing name-match gate. Validated URLs flow into
extract_for_beach.gather_urls() automatically.

See project_url_discovery_scope.md for the three scope tiers (MVP,
Standard, Full). This script is MVP.
"""
from __future__ import annotations
import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

import psycopg2
import psycopg2.extras
from bs4 import BeautifulSoup
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "Chrome/124.0.0.0 Safari/537.36")
HEADERS = {"User-Agent": USER_AGENT,
           "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
           "Accept-Language": "en-US,en;q=0.9",
           "Accept-Encoding": "identity"}

# Authority priors live in public.state_url_priors (per-state) — see
# 20260504_unified_pipeline_phase1_state_url_priors.sql. We call the
# DB function _state_authority_score(state, url) to resolve.

def authority_score(cur, state: str, url: str) -> int:
    cur.execute("select public._state_authority_score(%s, %s) as s", (state, url))
    r = cur.fetchone()
    return (r["s"] if r else 1) or 1


def classify_kind(url: str) -> str:
    host = (urllib.parse.urlparse(url).hostname or "").lower()
    if host.endswith(".gov") or host.endswith(".ca.us") or host.endswith(".org") and ("parks" in host or "nps" in host):
        return "gov"
    if any(k in host for k in ["bringfido","dogtrekker","californiabeaches","alltrails","yelp","tripadvisor"]):
        return "aggregator"
    if any(k in host for k in ["visit","tourism","cvb"]):
        return "visitor_bureau"
    if "wikipedia" in host:
        return "wikipedia"
    return "unknown"


def normalize_url(url: str) -> str:
    """Lowercase scheme+host, strip trailing slash, drop query/fragment for dedup key."""
    u = urllib.parse.urlparse(url)
    scheme = (u.scheme or "https").lower()
    host = (u.hostname or "").lower()
    path = u.path.rstrip("/") or ""
    if u.port: host = f"{host}:{u.port}"
    return f"{scheme}://{host}{path}"


# ─────────────────────────────────────────────────────────────────────────
# Search backends
# ─────────────────────────────────────────────────────────────────────────

def brave_search(query: str, count: int = 10) -> list[dict]:
    """Brave Search API call. Returns [{url, title, description, rank}, ...]."""
    key = os.environ.get("BRAVE_API_KEY")
    if not key:
        return None  # signal not-configured to caller
    url = "https://api.search.brave.com/res/v1/web/search?" + urllib.parse.urlencode({
        "q": query, "count": min(count, 20),
        "country": "US", "search_lang": "en", "safesearch": "off",
    })
    req = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": key,
        "User-Agent": USER_AGENT,
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
            if resp.headers.get("Content-Encoding") == "gzip":
                import gzip
                raw = gzip.decompress(raw)
            data = json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:400]
        print(f"  brave_search HTTP {e.code}: {e.reason} — {body}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"  brave_search error: {e}", file=sys.stderr)
        return []
    web = (data.get("web") or {}).get("results") or []
    return [{"url": r.get("url"), "title": r.get("title"),
             "description": r.get("description") or r.get("snippet"),
             "rank": i + 1}
            for i, r in enumerate(web) if r.get("url")][:count]


def anthropic_web_search(query: str, count: int = 10) -> list[dict]:
    """Fallback: use Anthropic web_search tool. More expensive than Brave but
    works with the existing ANTHROPIC_API_KEY. Asks Claude to return JSON list
    of relevant URLs."""
    try:
        from anthropic import Anthropic
    except ImportError:
        return []
    client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    sys_prompt = (
        "You are a search assistant. Use the web_search tool to find URLs for the "
        "given query. Return ONLY a JSON array of objects with shape "
        '{"url": "...", "title": "...", "description": "..."}. '
        "Aim for the top 10 most relevant results that are .gov or specific "
        "park/beach pages. Return JSON only, no other text."
    )
    user_prompt = f"Search query: {query}\n\nReturn JSON list of top results."
    try:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2000,
            system=sys_prompt,
            tools=[{"type": "web_search_20250305", "name": "web_search"}],
            messages=[{"role": "user", "content": user_prompt}],
        )
    except Exception as e:
        print(f"  anthropic_web_search error: {e}", file=sys.stderr)
        return []
    # Extract final text response (after tool use)
    text = ""
    for block in resp.content:
        if hasattr(block, "text"):
            text += block.text
    # Parse JSON from response
    text = re.sub(r"^```(?:json)?\s*", "", text.strip())
    text = re.sub(r"\s*```$", "", text)
    try:
        rows = json.loads(text)
        if not isinstance(rows, list): return []
        return [{"url": r.get("url"), "title": r.get("title"),
                 "description": r.get("description"), "rank": i + 1}
                for i, r in enumerate(rows) if r.get("url")][:count]
    except Exception:
        return []


def detect_backends() -> list[str]:
    """Return ordered list of available backends. Brave is preferred (cheap + fast);
    Anthropic web_search runs as fallback when Brave returns 0 results."""
    backends = []
    if os.environ.get("BRAVE_API_KEY"):
        backends.append("brave_search")
    if os.environ.get("ANTHROPIC_API_KEY"):
        backends.append("anthropic_web")
    return backends


def detect_backend() -> str:
    """Single primary backend label. Kept for backward compat / display."""
    backends = detect_backends()
    return backends[0] if backends else None


# ─────────────────────────────────────────────────────────────────────────
# Validation: fetch + name-match gate
# ─────────────────────────────────────────────────────────────────────────

def fetch_html(url: str, timeout: int = 12) -> str | None:
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ct = resp.headers.get("Content-Type", "")
            if "html" not in ct.lower():
                return None
            raw = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return raw.decode(charset, errors="replace")
    except Exception:
        return None


def strip_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "noscript", "iframe"]):
        tag.decompose()
    return (soup.body or soup).get_text(separator="\n", strip=True)[:12000]


def name_match(text: str, beach_name: str) -> bool:
    """Same gate as extract_for_beach.name_match — permissive."""
    text_lower = text.lower()
    name_lower = beach_name.lower()
    if name_lower in text_lower:
        return True
    stopwords = {'the','a','an','of','and','beach','park','state','county','city',
                 'municipal','dog','dogs','area','point','access'}
    words = [w for w in re.findall(r"\w+", name_lower)
             if w not in stopwords and len(w) >= 4]
    if not words:
        return name_lower in text_lower
    for w in words:
        if w in text_lower:
            return True
        if len(w) >= 5 and w[:4] in text_lower and any(
            len(c) >= 5 and c.startswith(w[:4]) for c in re.findall(r"\w+", text_lower)
        ):
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────
# DB ops
# ─────────────────────────────────────────────────────────────────────────

def select_target_fids(cur, args) -> list[dict]:
    if args.fids:
        fids = [int(x.strip()) for x in args.fids.split(",")]
        cur.execute("""
          select g.fid, g.name, g.county_name, g.state, j.name as city
            from public.beaches_gold g
            left join public.jurisdictions j on j.id = g.c1_jurisdiction_id
           where g.fid = any(%s)""", (fids,))
        return list(cur.fetchall())
    if args.gap_only:
        # Beaches with NO conclusive policy evidence — the 85-beach surface
        cur.execute("""
          select g.fid, g.name, g.county_name, g.state, j.name as city
            from public.beaches_gold g
            left join public.jurisdictions j on j.id = g.c1_jurisdiction_id
           where g.is_active and g.is_scoreable
             and not exists (
               select 1 from public.beach_enrichment_provenance bep
                where bep.gold_fid = g.fid and bep.field_group = 'dogs'
             )
           order by g.fid
           limit %s""", (args.limit or 200,))
        return list(cur.fetchall())
    raise SystemExit("Specify --fids or --gap-only")


def already_in_pool(cur, fid: int, url_normalized: str) -> bool:
    cur.execute("""select 1 from public.discovered_urls
                    where gold_fid=%s and url_normalized=%s""",
                (fid, url_normalized))
    return cur.fetchone() is not None


def insert_discovered_url(cur, fid: int, state: str, query: str, source: str, r: dict):
    url = r["url"]
    norm = normalize_url(url)
    if already_in_pool(cur, fid, norm):
        return False
    cur.execute("""
      insert into public.discovered_urls
        (gold_fid, url, url_normalized, source, query, rank,
         authority_score, kind, title, snippet)
      values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
      on conflict (gold_fid, url_normalized) do nothing
    """, (fid, url, norm, source, query, r.get("rank"),
          authority_score(cur, state, url), classify_kind(url),
          (r.get("title") or "")[:500],
          (r.get("description") or "")[:1000]))
    return cur.rowcount > 0


def validate_pending(cur, fid: int, beach_name: str, conn) -> tuple[int, int]:
    """Fetch each pending URL for this beach, run name-match, mark validated/rejected."""
    cur.execute("""select id, url from public.discovered_urls
                    where gold_fid=%s and validation_status='pending'""", (fid,))
    rows = cur.fetchall()
    validated = rejected = 0
    for r in rows:
        html = fetch_html(r["url"])
        if not html:
            cur.execute("""update public.discovered_urls
                             set validation_status='rejected',
                                 validated_at=now(), rejected_reason='fetch_failed'
                            where id=%s""", (r["id"],))
            rejected += 1
            continue
        text = strip_html(html)
        if name_match(text, beach_name):
            cur.execute("""update public.discovered_urls
                             set validation_status='validated', validated_at=now()
                            where id=%s""", (r["id"],))
            validated += 1
        else:
            cur.execute("""update public.discovered_urls
                             set validation_status='rejected',
                                 validated_at=now(), rejected_reason='name_match_failed'
                            where id=%s""", (r["id"],))
            rejected += 1
    conn.commit()
    return validated, rejected


# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="URL discovery MVP — find beach-specific URLs via web search.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--fids", help="Comma-separated gold fids.")
    g.add_argument("--gap-only", action="store_true",
                   help="Run on scoreable beaches with no dogs evidence.")
    ap.add_argument("--limit", type=int, help="Cap on number of beaches.")
    ap.add_argument("--apply", action="store_true",
                    help="Actually call search API + write rows. Default is dry-run.")
    ap.add_argument("--no-validate", action="store_true",
                    help="Skip the validation pass (fetch + name-match).")
    ap.add_argument("--max-results", type=int, default=10)
    args = ap.parse_args()

    backends = detect_backends()
    if not backends:
        print("ERROR: no search backend configured.")
        print("  Set BRAVE_API_KEY in scripts/pipeline/.env (preferred — free 2K/mo at")
        print("    https://api-dashboard.search.brave.com/register), OR")
        print("  ensure ANTHROPIC_API_KEY is set (uses web_search tool, ~$10/1K).")
        return 2
    primary = backends[0]
    fallback = backends[1] if len(backends) > 1 else None
    print(f"Backends: primary={primary}" + (f", fallback={fallback}" if fallback else ""))
    if primary == "anthropic_web":
        print("  (Anthropic web_search primary — pricier than Brave; consider getting a Brave key.)")

    conn = psycopg2.connect(**PG)
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    candidates = select_target_fids(cur, args)
    print(f"Selected {len(candidates)} beach(es)")
    if args.limit:
        candidates = candidates[:args.limit]

    if not args.apply:
        for c in candidates[:10]:
            q = f"{c['name']} {c.get('city') or ''} {c['county_name']} dogs".strip()
            print(f"  fid={c['fid']:<6} {c['name']:<32} query={q!r}")
        if len(candidates) > 10:
            print(f"  ... + {len(candidates)-10} more")
        print("\n--dry-run: not calling API or writing rows.")
        return 0

    total_inserted = 0
    total_validated = 0
    total_rejected = 0
    fallback_fired = 0
    for i, c in enumerate(candidates, 1):
        name = c["name"]
        city = c.get("city") or ""
        query = f"{name} {city} {c['county_name']} dogs".strip()
        # Try primary; if 0 results AND fallback exists, try fallback.
        # Each result is tagged with the backend that produced it (source column),
        # so provenance survives the fallback path.
        backend_used = primary
        results = (brave_search(query, args.max_results) if primary == "brave_search"
                   else anthropic_web_search(query, args.max_results))
        if results is None:
            results = []
        if not results and fallback:
            backend_used = fallback
            fallback_fired += 1
            results = (brave_search(query, args.max_results) if fallback == "brave_search"
                       else anthropic_web_search(query, args.max_results))
            if results is None:
                results = []
        new_count = 0
        for r in results:
            if insert_discovered_url(cur, c["fid"], c.get("state") or "CA", query, backend_used, r):
                new_count += 1
        conn.commit()
        total_inserted += new_count

        validated = rejected = 0
        if not args.no_validate:
            validated, rejected = validate_pending(cur, c["fid"], name, conn)
        total_validated += validated
        total_rejected += rejected

        print(f"  [{i}/{len(candidates)}] fid={c['fid']:<6} {name[:30]:<30} "
              f"results={len(results)} new={new_count} val={validated} rej={rejected}",
              flush=True)
        time.sleep(0.5)  # polite to Brave free tier

    print()
    print(f"=== TOTALS ===")
    print(f"  rows inserted:   {total_inserted}")
    print(f"  validated:       {total_validated}")
    print(f"  rejected:        {total_rejected}")
    if fallback:
        print(f"  fallback fired:  {fallback_fired} of {len(candidates)} beaches "
              f"({fallback_fired*100//max(1,len(candidates))}%)")
    print(f"  beaches with ≥1 validated URL:")
    cur.execute("""select count(distinct gold_fid) as n from public.discovered_urls
                    where validation_status='validated' and gold_fid = any(%s)""",
                ([c["fid"] for c in candidates],))
    print(f"    {cur.fetchone()['n']} / {len(candidates)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
