# `scripts/fetch/` — 403-defeat fetchers for law-as-primary-source research

Scripts for pulling primary-source text from sites that block WebFetch
(city code platforms, EBRPD, OC Register, ecfr.gov HTML, etc.). Built
for the [[law-as-primary-source-ca]] initiative — see `docs/walkthrough_*.md`
for what these fetch.

## Quickstart

```bash
# Federal regulations (eCFR public API — fast, clean XML, no bot detection)
python scripts/fetch/fetch_ecfr.py 36 7 7.97       # 36 CFR §7.97 (GGNRA)
python scripts/fetch/fetch_ecfr.py 36 2 2.15       # 36 CFR §2.15 (NPS pets)

# California state law (leginfo.legislature.ca.gov — works reliably)
python scripts/fetch/fetch_leginfo.py PRC 6001     # Public Resources §6001
python scripts/fetch/fetch_leginfo.py HSC 115880   # Health & Safety §115880 (AB 411)

# Municipal codes on ecode360 (HBMC and others — works reliably with --wait)
python scripts/fetch/fetch_ecode360.py 43799422    # HBMC §13.08 chapter

# Municipal codes on Municode (bot-protected — often fails; see notes below)
python scripts/fetch/fetch_municode.py long_beach <nodeId>
python scripts/fetch/fetch_municode.py long_beach --url-only   # just build URL

# Any other URL (district sites, news articles, etc.)
python scripts/fetch/fetch_html.py https://www.ebparks.org/parks/crown-beach

# Auto-router: picks the right backend by hostname
python scripts/fetch/fetch_url.py "https://www.ecfr.gov/current/title-36/chapter-I/part-7/section-7.97"
python scripts/fetch/fetch_url.py "https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=PRC&sectionNum=6001"
python scripts/fetch/fetch_url.py "https://ecode360.com/43799422"
```

## Why two backends

| Backend | When to use | Speed | Reliability |
|---|---|---|---|
| **eCFR API** (`fetch_ecfr.py`) | Federal CFR sections | ~1s | High — government API, no bot detection |
| **leginfo** (`fetch_leginfo.py`) | CA state codes (PRC/HSC/FGC/etc.) | 5-8s | High — server-rendered, no bot detection |
| **ecode360** (`fetch_ecode360.py`) | Municipal codes on ecode360 (HBMC, ~40 CA cities) | 8-15s | High — SPA but cooperative |
| **Municode** (`fetch_municode.py`) | Municipal codes on library.municode.com (~50% of CA cities) | 15s+ | LOW — bot-protected, often returns "not authorized" |
| **Playwright** (`fetch_html.py`) | Everything else | 5-15s | High for most sites; Cloudflare-Enterprise still blocks |

WebFetch (the AI-tool default) gets 403'd on a long list of sites we need
to read for this project — ecode360, municode, qcode, amlegal, ebparks,
parks.ca.gov PDFs, Federal Register HTML, ecfr.gov HTML, OC Register, LA
Times. These scripts route around that.

## Dependencies

Already installed in this environment (see also `requirements.txt` if
present):

- `playwright` (browser automation, includes a Chromium binary)
- `truststore` (Windows: uses the OS native cert store to avoid
  OpenSSL/SChannel trust mismatches — same fix as `git -c
  http.sslBackend=schannel`)
- `certifi` (fallback trust bundle when truststore isn't available)

If setting up fresh:

```bash
pip install playwright truststore certifi
playwright install chromium
```

## Known limits — when to fall back to user-paste

- **Cloudflare Turnstile** challenges — some Federal Register paths and
  a few city-code-platform deep links use Turnstile JS challenges that
  Playwright doesn't solve out of the box. If you see an interstitial
  page in the output, the user pastes the verbatim text instead.
- **Authenticated sources** — court PACER records, Westlaw/Lexis. No
  scripted access; manual extraction.
- **Government PDFs with binary streams** — some (like NPS Compendium
  PDFs and CFR-edition PDFs from GovInfo) don't render to text cleanly
  through any HTTP fetch. Read the file locally with a PDF tool or get
  the equivalent HTML / API representation.
- **Municode bot-protection** — `library.municode.com` actively
  detects Playwright (headed or headless) and returns "The requested
  content cannot be found or you are not authorized to view it." for
  any deep-section nodeId. The SPA has no public JSON API to call
  directly. `fetch_municode.py` confirms the failure and exits 5;
  workflow then is CPRA request or manual browser-paste into the
  walkthrough doc's evidence_verbatim. Discovered 2026-05-16 with
  EBRPD Ordinance 38 + Long Beach Dog Beach Zone designation.
  ~50% of CA cities are on Municode, so this is a significant
  coverage gap for Wave 3.

## Useful patterns discovered

- **Date-stamped filenames as metadata.** EBRPD's PDF ordinances
  encode the effective date in the filename (e.g.,
  `Ord38-09052023FINAL.pdf` = Sept 5, 2023). Exploiting filenames
  this way gives `last_amended_date` without opening the PDF.
  Worth checking for similar patterns on other government sites
  (city clerks, CDFW PDFs, NPS Compendiums) before parsing PDF
  text.
- **Site-internal links are reliable.** If a page mentions
  "VIEW ONLINE" / "Ordinance 38" / etc. but the surface text
  doesn't include the source URL, the HTML grep
  (`fetch_html.py --html | grep -oE 'href="[^"]+"'`) usually
  surfaces the canonical link cheaply.

## Auto-router URL detection

`fetch_url.py` routes based on hostname:

- `*.ecfr.gov` → API path, parses title/part/section from the URL
- `leginfo.legislature.ca.gov` → leginfo helper, parses lawCode + sectionNum
- `ecode360.com` → ecode360 helper, parses numeric nodeId from path
- `library.municode.com` → Municode helper (bot-protection warning on failure)
- everything else → Playwright

For URLs that don't match expected sub-patterns, falls back to
Playwright with a stderr warning.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 2 | Bad args |
| 3 | Fetch error (network, HTTP error, missing selector) |
| 4 | Timeout |
| 5 | Bot-protected content (Municode "not authorized" page; CPRA fallback) |

## Related

- `docs/walkthrough_hbdb.md`, `walkthrough_crystal_cove.md`,
  `walkthrough_fort_funston.md`, `walkthrough_crown_memorial.md` —
  beaches these scripts have been used against.
- Memory pin: [[law-as-primary-source-ca]] — strategy + CPRA-vs-scrape
  process notes.
