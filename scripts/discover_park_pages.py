"""
discover_park_pages.py
----------------------
For beaches with no CPAD park_url (~363 in CA), discover the beach's
specific page on the agency's website (CPAD agncy_web).

Discovery techniques (in order of preference):
  1. Sitemap-grep — fetch <agency>/sitemap.xml (or sitemap_index.xml),
     parse URLs, score by name-token overlap with beach.display_name,
     pick the highest-scoring URL above MIN_MATCH_SCORE.
  2. (later) Site internal search via /?s=<name>
  3. (later) Depth-1 crawl from agncy_web

Writes top candidate(s) to discovered_park_pages. extract_from_park_url.py
picks them up via park_url_scrape_queue (UNIONed with CPAD URLs).

Usage:
  python scripts/discover_park_pages.py --limit 20
  python scripts/discover_park_pages.py --all
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, urljoin
from xml.etree import ElementTree as ET

import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / "pipeline" / ".env")

SUPABASE_URL          = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]

WORKERS               = 5
SITEMAP_TIMEOUT       = 20.0
MIN_MATCH_SCORE       = 0.30

# Skip these agency websites entirely — no sitemap or known dead-ends
SKIP_AGENCY_DOMAINS = {
    "parks.ca.gov",       # JS-rendered SPA, no sitemap
    "ca.gov",             # generic root, useless
    "wikipedia.org",      # CPAD points some park_urls at wikipedia
    "wikipedia.com",
}
USER_AGENT            = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

# Skip URLs whose path contains these segments — they're not park pages.
# File extensions are dot-less because url_path_tokens splits on non-alphanumerics
# (so /foo/bar.pdf → tokens {foo, bar, pdf}, never `.pdf`).
SKIP_PATH_TOKENS = {
    "blog", "news", "press", "events", "media", "video", "photo", "gallery",
    "calendar", "contact", "search", "login", "account", "cart", "tag",
    "category", "page", "feed", "rss", "atom", "sitemap",
    "wp", "admin", "json", "embed",
    "pdf", "jpg", "jpeg", "png", "gif", "css", "js", "svg",
}


def sb_headers() -> dict[str, str]:
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }


def _agency_skipped(agency_url: str) -> bool:
    if not agency_url:
        return True
    host = urlparse(agency_url).netloc.lower()
    return any(skip in host for skip in SKIP_AGENCY_DOMAINS)


def fetch_targets(limit: Optional[int]) -> list[dict]:
    """Beaches with no usable CPAD park_url but with agncy_web."""
    # Use a server-side computed view via SQL — easier than chaining REST filters.
    sql = """
    with cpad_best as (
      select distinct on (s.fid)
        s.fid, s.display_name, c.park_url, c.agncy_web
      from public.locations_stage s
      join public.cpad_units c on st_contains(c.geom, s.geom::geometry)
      where s.is_active = true
      order by s.fid, st_area(c.geom::geography) asc
    )
    select fid, display_name, agncy_web
    from cpad_best
    where (park_url is null or park_url ~* '(parks\\.ca\\.gov|encinitasca\\.gov)')
      and agncy_web is not null
      and not exists (
        select 1 from public.discovered_park_pages d where d.fid = cpad_best.fid
      )
    order by fid
    """
    if limit:
        sql += f" limit {limit}"
    # Use the rpc endpoint by wrapping in a function call... actually
    # easier to use REST query. PostgREST doesn't support arbitrary SQL.
    # Use the rpc pattern: write the query to a temp file via supabase CLI.
    # (Falling back to Python subprocess.)
    import subprocess, json, tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as tf:
        tf.write(sql + ';')
        tmpfile = tf.name
    try:
        r = subprocess.run(
            ['supabase', 'db', 'query', '--linked', '-f', tmpfile],
            capture_output=True, text=True, timeout=120,
        )
        if r.returncode != 0:
            print(f"SQL failed: {r.stderr}", file=sys.stderr)
            return []
        out = r.stdout
        s, e = out.find('{'), out.rfind('}')
        return json.loads(out[s:e+1]).get('rows', [])
    finally:
        try: os.unlink(tmpfile)
        except: pass

    # ↓ unreachable; filtering happens below


def filter_targets(rows: list[dict], dry_run: bool = False) -> list[dict]:
    """Drop rows whose agency_url is skipped or missing, and log those
    drops as attempts so we have a complete audit trail per beach."""
    keep: list[dict] = []
    for r in rows:
        agncy = r.get("agncy_web") or ""
        if not agncy:
            insert_attempt(r["fid"], None, "sitemap", "agency_missing",
                           notes="beach has no CPAD agncy_web", dry_run=dry_run)
            continue
        if _agency_skipped(agncy):
            insert_attempt(r["fid"], agncy, "sitemap", "agency_skipped",
                           notes="agency_url matched skip-domain (parks.ca.gov, ca.gov, wikipedia)",
                           dry_run=dry_run)
            continue
        keep.append(r)
    return keep


def insert_discovery(fid: int, source_url: str, source_method: str,
                     agency_url: str, match_score: float, notes: str = "",
                     dry_run: bool = False) -> None:
    if dry_run:
        print(f"  [dry-run] fid={fid}  {source_method}  score={match_score}  {source_url}")
        return
    payload = {
        "fid": fid, "source_url": source_url, "source_method": source_method,
        "agency_url": agency_url, "match_score": match_score, "notes": notes,
    }
    url = f"{SUPABASE_URL}/rest/v1/discovered_park_pages?on_conflict=fid,source_url"
    headers = {**sb_headers(), "Prefer": "resolution=ignore-duplicates"}
    resp = httpx.post(url, headers=headers, json=payload, timeout=15)
    if not resp.is_success and resp.status_code != 409:
        print(f"  insert failed for fid={fid}: {resp.status_code} {resp.text[:200]}", file=sys.stderr)


def insert_attempt(fid: int, agency_url: Optional[str], source_method: str,
                   status: str, sitemap_url_count: Optional[int] = None,
                   best_score: Optional[float] = None,
                   best_url: Optional[str] = None,
                   notes: Optional[str] = None,
                   dry_run: bool = False) -> None:
    """Audit log: every discovery attempt outcome (success or failure).
    Lets us measure no-sitemap and no-match failure modes without re-running."""
    if dry_run:
        return
    payload = {
        "fid": fid, "agency_url": agency_url, "source_method": source_method,
        "status": status, "sitemap_url_count": sitemap_url_count,
        "best_score": best_score, "best_url": best_url, "notes": notes,
    }
    url = f"{SUPABASE_URL}/rest/v1/discovery_attempts"
    resp = httpx.post(url, headers=sb_headers(), json=payload, timeout=15)
    if not resp.is_success:
        print(f"  attempt-log insert failed for fid={fid}: {resp.status_code} {resp.text[:200]}", file=sys.stderr)


# ── Tokenization for matching ───────────────────────────────────────────────

STOPWORDS = {
    "the", "a", "an", "of", "and", "at", "in", "on", "to", "for",
    "park", "beach", "area", "point", "cove", "bay",
    "ca", "california", "state", "city", "county", "national",
}

def normalize_name(s: str) -> set[str]:
    """Tokens from a name: lowercase, alphanumeric only, drop stopwords + short."""
    if not s: return set()
    parts = re.findall(r"[a-z0-9]+", s.lower())
    return {p for p in parts if len(p) >= 3 and p not in STOPWORDS}


def url_path_tokens(url: str) -> set[str]:
    """Tokens from a URL path."""
    p = urlparse(url).path.lower()
    p = re.sub(r"\.(html?|aspx?|php|jsp)$", "", p)
    return set(re.findall(r"[a-z0-9]+", p))


def url_path_joined(url: str) -> str:
    """Path collapsed to alphanumerics-only. Lets us catch single-word slugs
    like /parks/ribbonbeach where the tokenizer would otherwise split into
    {parks, ribbonbeach} and miss the `ribbon` name token."""
    p = urlparse(url).path.lower()
    p = re.sub(r"\.(html?|aspx?|php|jsp)$", "", p)
    return re.sub(r"[^a-z0-9]+", "", p)


def url_score(beach_name: str, candidate_url: str) -> float:
    """0.00–1.00 score for how well a URL's path matches a beach name."""
    name_tokens = normalize_name(beach_name)
    if not name_tokens:
        return 0.0
    path_tokens = url_path_tokens(candidate_url)
    if any(t in path_tokens for t in SKIP_PATH_TOKENS):
        return 0.0
    # Exact-token matches against the URL's tokenized path
    matched = name_tokens & path_tokens
    # Substring matches against the joined path for longer name tokens
    # (5+ chars to avoid `art` matching `parts` etc.)
    joined = url_path_joined(candidate_url)
    for t in name_tokens:
        if t not in matched and len(t) >= 5 and t in joined:
            matched.add(t)
    if not matched:
        return 0.0
    # Jaccard-ish: matched / name_tokens (penalizes URLs that miss name parts)
    return round(len(matched) / len(name_tokens), 2)


# ── Sitemap fetch + parse ───────────────────────────────────────────────────

NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

async def fetch_sitemap_urls(client: httpx.AsyncClient, agency_url: str,
                              max_urls: int = 5000) -> list[str]:
    """Fetch sitemap(s) at the agency URL and return a flat list of URLs.
    Supports sitemap_index.xml (recursive)."""
    base = agency_url.rstrip("/")
    parsed = urlparse(base)
    root = f"{parsed.scheme}://{parsed.netloc}"

    candidates = [
        f"{root}/sitemap.xml",
        f"{root}/sitemap_index.xml",
        f"{root}/sitemap-index.xml",
        f"{root}/wp-sitemap.xml",
    ]

    all_urls: list[str] = []
    for sm_url in candidates:
        try:
            resp = await client.get(sm_url, headers={"User-Agent": USER_AGENT}, timeout=SITEMAP_TIMEOUT)
            if not resp.is_success:
                continue
            text = resp.text
            try:
                tree = ET.fromstring(text)
            except ET.ParseError:
                continue
            tag = tree.tag.split("}")[-1]
            if tag == "sitemapindex":
                # Nested sitemaps — fetch each and accumulate
                for loc in tree.findall(".//sm:loc", NS):
                    if not loc.text or len(all_urls) >= max_urls:
                        break
                    try:
                        sub = await client.get(loc.text.strip(), headers={"User-Agent": USER_AGENT}, timeout=SITEMAP_TIMEOUT)
                        if sub.is_success:
                            sub_tree = ET.fromstring(sub.text)
                            for u in sub_tree.findall(".//sm:loc", NS):
                                if u.text:
                                    all_urls.append(u.text.strip())
                                    if len(all_urls) >= max_urls:
                                        break
                    except Exception:
                        continue
            else:
                # Direct sitemap
                for u in tree.findall(".//sm:loc", NS):
                    if u.text:
                        all_urls.append(u.text.strip())
                        if len(all_urls) >= max_urls:
                            break
            if all_urls:
                return all_urls
        except Exception as e:
            print(f"    sitemap error ({sm_url}): {e}", file=sys.stderr)
            continue
    return all_urls


# ── Discovery flow ──────────────────────────────────────────────────────────

async def discover_one(sem: asyncio.Semaphore, client: httpx.AsyncClient,
                       beach: dict, dry_run: bool) -> str:
    async with sem:
        fid     = beach["fid"]
        name    = beach["display_name"]
        agency  = beach["agncy_web"]

        urls = await fetch_sitemap_urls(client, agency)
        if not urls:
            insert_attempt(fid, agency, "sitemap", "no_sitemap",
                           notes="no sitemap.xml/sitemap_index.xml/wp-sitemap.xml accessible",
                           dry_run=dry_run)
            return f"  fid={fid} {name!r}  no-sitemap  agency={agency}"

        # Score every URL; track best even if below threshold so we know
        # "we tried, the sitemap had no per-beach plug for this name"
        all_scored = [(url_score(name, u), u) for u in urls]
        all_scored.sort(key=lambda x: -x[0])
        best_overall_score, best_overall_url = (all_scored[0] if all_scored else (0.0, None))

        scored = [(s, u) for s, u in all_scored if s >= MIN_MATCH_SCORE]
        if not scored:
            insert_attempt(fid, agency, "sitemap", "no_match",
                           sitemap_url_count=len(urls),
                           best_score=best_overall_score,
                           best_url=best_overall_url,
                           notes=f"sitemap had {len(urls)} urls but no per-beach plug above {MIN_MATCH_SCORE} threshold",
                           dry_run=dry_run)
            return f"  fid={fid} {name!r}  no-match  ({len(urls)} sitemap urls, best={best_overall_score})"

        top_score, top_url = scored[0]
        insert_discovery(fid, top_url, "sitemap", agency, top_score,
                         f"top of {len(urls)} sitemap urls", dry_run)
        insert_attempt(fid, agency, "sitemap", "success",
                       sitemap_url_count=len(urls),
                       best_score=top_score,
                       best_url=top_url,
                       dry_run=dry_run)
        return f"  fid={fid} {name!r}  ok  score={top_score}  {top_url}"


async def run(args: argparse.Namespace) -> None:
    targets = fetch_targets(args.limit)
    targets = filter_targets(targets, dry_run=args.dry_run)
    print(f"Loaded {len(targets)} discovery targets (after skip-agency filter)")
    if not targets:
        return

    sem = asyncio.Semaphore(WORKERS)
    limits = httpx.Limits(max_keepalive_connections=WORKERS, max_connections=WORKERS * 2)
    async with httpx.AsyncClient(limits=limits, follow_redirects=True) as client:
        tasks = [discover_one(sem, client, b, args.dry_run) for b in targets]
        for t in asyncio.as_completed(tasks):
            print(await t)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--all",   action="store_true")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    if args.all:
        args.limit = None
    asyncio.run(run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
