# Gold Set — Tier 2 picklist (draft)

**Goal:** add ~26 beaches to the gold set, chosen to fill the gaps Tier 1 left.
Tier 1 (11 beaches) is heavily SoCal city + dog beaches; underrepresents NPS,
state-park "no dogs", county-operated, and NorCal.

After curation: **37 beaches × 14 fields ≈ 518 truth cells**, enough to score
variants with real signal across the full shape spectrum.

---

## Coverage rationale

| Cluster | # | Why this gap matters |
|---|---|---|
| NPS / federal | 4 | Tier 1 has zero NPS. PRNS has a famous exception (Limantour off-leash within a no-dogs seashore); GGNRA has Fort Funston (off-leash institution). Catches the operator-vs-unit nuance. |
| LA basin (city/state mix) | 4 | LA county pattern (city beaches under county operator) is unmodeled. Will Rogers/Santa Monica/Venice/Malibu Lagoon span "no dogs" → "leashed" → "varies by zone". |
| SD city beaches | 4 | SD has rich text + multiple distinct beaches per city. La Jolla Cove sea-lion regs are an edge-case for `dogs_seasonal_restrictions`. |
| SF Bay urban/Marin | 3 | Bay Area dog culture is loud; rules are non-trivial. Stinson is a duplicate-name (county vs GGNRA) — exposes how cascade resolves. |
| Central Coast state parks | 5 | CA State Parks spectrum: Asilomar (Monterey), Carmel River (leashed), Montara/Pacifica (San Mateo), Pismo (SLO dogs allowed) — each has a different official answer. |
| Santa Cruz | 3 | Santa Cruz has its own city policy + 2 state beaches (rare clean cluster). |
| SB/Ventura state parks | 3 | Carpinteria (often leashed), Refugio (no dogs), San Buenaventura (mixed). |

---

## Picks (group_id → name, county, operator)

### NPS / federal (4)
- [ ] **8865** — Limantour Beach (Marin) · *Point Reyes NS — off-leash exception inside a mostly-no-dogs seashore*
- [ ] **8758** — Fort Funston (San Francisco) · *GGNRA — institutional off-leash, the SoCal-equivalent of HB Dog Beach for SF*
- [ ] **8536** — Tennessee Beach (Marin) · *GGNRA — typical leashed coastal NPS*
- [ ] **5946** — Cuyler Harbor Beach Landing (Ventura) · *Channel Islands NP — boats-only, near-zero dog access*

### LA basin (4)
- [ ] **8472** — Will Rogers State Beach (LA) · *State beach, Santa Monica Bay; CDPR rule says no dogs on sand*
- [ ] **8246** — Santa Monica State Beach (LA) · *Long stretch, multiple jurisdictions inside one polygon*
- [ ] **8247** — Venice Beach (LA) · *City beach under county operator*
- [ ] **8475** — Malibu Lagoon State Beach (LA) · *Surfrider Beach — wildlife restrictions complicate dog policy*

### SD city beaches (4)
- [ ] **8359** — Ocean Beach (San Diego) · *City OB north of the dog-beach; different rules, same name confusion as LA Stinson*
- [ ] **8347** — La Jolla Shores (San Diego) · *Heavily-trafficked city beach, dog-friendly with hours*
- [ ] **8348** — La Jolla Cove (San Diego) · *Sea lion sanctuary — seasonal closures, signage-driven rules*
- [ ] **8341** — Cardiff State Beach (San Diego) · *State, dogs allowed leashed — north-county counterweight to existing Del Mar*

### SF Bay urban / Marin (3)
- [ ] **8260** — Baker Beach (San Francisco) · *GGNRA, leashed only, dense use*
- [ ] **8226** — Seal Rocks Beach (San Francisco) · *Lands End / GGNRA — testing edge of "is this even a beach"*
- [ ] **8236** — Stinson Beach (Marin) · *GGNRA version (the more-trafficked one). Co-named with 8238 county — duplicate-name regression test*

### Central Coast state-park spectrum (5)
- [ ] **9302** — Asilomar State Beach (Monterey) · *Pebble Beach-adjacent state beach*
- [ ] **8287** — Carmel River State Beach (Monterey) · *Distinct from city Carmel Beach; bird sanctuary interaction*
- [ ] **8480** — Montara State Beach (San Mateo) · *Famous CDPR no-dog rule*
- [ ] **8607** — Pacifica State Beach (San Mateo) · *State, dogs welcomed — operator answer flips per beach*
- [ ] **8394** — Pismo State Beach (SLO) · *Dogs allowed leashed; ATV-friendly state beach*

### Santa Cruz (3)
- [ ] **8300** — Cowell Beach (Santa Cruz) · *City beach, surf school crowd*
- [ ] **8210** — Natural Bridges State Beach (Santa Cruz) · *Monarch butterfly preserve — seasonal restrictions*
- [ ] **8243** — New Brighton State Beach (Santa Cruz) · *Standard CDPR leashed rule*

### SB / Ventura state parks (3)
- [ ] **8673** — Carpinteria State Beach (Santa Barbara) · *Heavily curated CDPR; dog-allowed-area split*
- [ ] **5939** — Refugio State Beach (Santa Barbara) · *Remote CDPR; pure leashed-dogs answer*
- [ ] **8490** — San Buenaventura State Beach (Ventura) · *City-of-Ventura-managed CDPR; mixed answer*

**Total: 26 picks**

---

## How to launch the second round

Once you've reviewed/edited this list:

```bash
# 1. Update GAP_B_BEACHES in scripts/extract_for_orphans.py with the new arena_group_ids
# 2. Run extraction
EXTRACT_SET=gap_b python scripts/extract_for_orphans.py
# 3. Re-pull data + re-inject into admin/gold-set-curator.html
#    (I can do this in one shot — the puller script is parameterized by the GAP_B list)
```

Roughly $3-5 of API calls + ~10 min runtime.

After extraction, tell me to "regenerate the curator" and I'll pull with all
37 arena_group_ids, re-inject the JSON, and the page will show both Tier 1
(curated) + Tier 2 (uncurated) beaches.

---

*Drafted 2026-04-30. Edit picks freely; add/remove with rationale. The
script doesn't care about ordering — list is just the source of truth.*
