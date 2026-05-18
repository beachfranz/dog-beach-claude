# Water-conditions layer — advisory engine extension spec

Operational scoping for cautions / warnings / alerts on beach.html + mobile-beach.html. **Extends the existing deterministic advisory engine** (the one hot-asphalt advisories run through), NOT a new parallel cautions strip.

Crystallized 2026-05-17 evening during a strategy conversation. Sources prioritized: **NWS active alerts + Open-Meteo marine** ([[nws-beach-hazards-integration]] + [[open-meteo-expansion]] both reframed).

---

## Two questions hiding in one

| Question | Surface | Source |
|---|---|---|
| Real-time **appeal** ("is this beach good NOW?") | Pleasantness-only layer (separate, lower-stakes) | Middle-band data from continuous forecasts |
| **Cautions** ("things you NEED to know") | **Existing advisory engine** (same UI weight as hot asphalt) | Active warnings + edge-band data + translation |

They overlap (a hazard reduces appeal) but aren't the same surface. A beach can be hazard-free but unappealing (cold, raining, low tide gone bad). A beach can be appealing in general but disqualifying today.

---

## Dual-use signal pattern (the load-bearing insight)

Same data point feeds two surfaces depending on **value band**:

| Signal | Middle band → pleasantness layer | Edge band → advisory engine |
|---|---|---|
| Sea surface temperature (SST) | "warm water today" | too cold for swim / too cold for paws on damp sand / too warm at inland lakes |
| Wave height | "small waves" / "good swimming" | dog-swim threshold crossed → "skip the swim" |
| Sand surface temp (derived) | neutral | paw-burn threshold crossed → "hot asphalt" pattern (precedent) |
| Wind | "calm" / "breezy" | dust / blowing-sand / high-wind small-dog advisory |

**Value-band routes which surface.** Architecture stays clean: ONE source, ONE measurement table, TWO consumers that filter on band.

---

## What this extends, not creates

The product already has a deterministic advisory layer (hot-asphalt advisory is the canonical example). NWS warnings (riptide, beach hazards, high surf) and Open-Meteo threshold crossings (SST cold/hot, waves too big, sand burning) all route into that **same layer** with translated dog-impact text.

**Do NOT build:**
- A separate "cautions strip" parallel to existing advisories
- A composite "real-time appeal" score that conflates pleasantness + safety
- Narrator-only treatment for safety signals (user needs to know — same UI weight as hot asphalt)

**Do build:**
- New input adapters (NWS poller, Open-Meteo marine fetcher)
- A translation layer (NWS event type → dog-impact class + plain-language text)
- A threshold layer (Open-Meteo continuous signals → edge-band advisory triggers)
- A pleasantness-only "real-time appeal" layer fed by middle-band data (separate from advisories)

---

## Sources

### NWS active alerts
- Endpoint: `https://api.weather.gov/alerts/active`
- Auth: none; attribution required
- Cadence: poll every 15min, OR ATOM subscribe
- Shape: GeoJSON polygons + event_type + severity + valid_from/to
- Work: PIP active polygons against in-scope beach fids; upsert into `beach_active_alert`

### Open-Meteo marine
- Endpoint: `https://marine-api.open-meteo.com/v1/marine`
- Auth: none; generous rate limits
- Cadence: hourly cron, 168-hour horizon
- Variables: `wave_height`, `wave_period`, `wave_direction`, `wind_wave_*`, `swell_wave_*`, `sea_surface_temperature`, `ocean_current_velocity`
- Work: per-beach or grid lookup; upsert into `beach_marine_forecast`

### Sand temperature (derived)
- Not in either API directly
- Derive from: air temp + solar angle (lat + time-of-day + date) + cloud cover + albedo
- Paws burn at ~50°C surface
- Worth including in the marine pass — small additional code, real user value, follows hot-asphalt precedent

---

## Schema sketch (measurements-class per [[entity-modeling]])

```sql
CREATE TABLE beach_marine_forecast (
  beach_fid bigint NOT NULL REFERENCES ...,
  ts timestamptz NOT NULL,
  wave_height_m numeric,
  wave_period_s numeric,
  sst_c numeric,
  wind_speed_ms numeric,
  sand_surface_c numeric,        -- derived
  source text NOT NULL DEFAULT 'open_meteo',
  fetched_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (beach_fid, ts)
);

CREATE TABLE beach_active_alert (
  beach_fid bigint NOT NULL REFERENCES ...,
  alert_id text NOT NULL,         -- NWS-provided ID
  nws_event_type text NOT NULL,
  severity text,                  -- as-received from NWS
  valid_from timestamptz NOT NULL,
  valid_to timestamptz NOT NULL,
  dog_impact_class text NOT NULL, -- translated; e.g. 'skip_swim', 'paws_warning'
  dog_impact_text text NOT NULL,  -- translated; user-facing
  source text NOT NULL DEFAULT 'nws',
  fetched_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (beach_fid, alert_id)
);
```

Advisory engine then consumes both tables alongside its existing deterministic-rule inputs.

---

## Translation layer

YAML in repo (`config/nws_dog_impact_map.yaml`), **not DB** — safety-critical, needs to be PR-reviewable.

```yaml
- event_type: Rip Current Statement
  dog_impact_class: skip_swim
  severity: moderate
  text: "Rip currents in effect. Keep your dog out of the water through {valid_to}."

- event_type: High Surf Advisory
  dog_impact_class: skip_swim
  severity: moderate
  text: "Large surf expected. Skip the swim and stay back from the wave line."

- event_type: Beach Hazards Statement
  dog_impact_class: review_required        # wildcard; needs human curation per instance
  severity: review_required
  text: null

- event_type: Special Weather Statement
  dog_impact_class: review_required
  severity: review_required
  text: null

- event_type: Heat Advisory
  dog_impact_class: paws_warning
  severity: moderate
  text: "Heat advisory. Sand will burn paws — go at dawn or dusk."
```

**~30 NWS event types.** Mostly mechanical; ~3-5 wildcards require human review per instance (Beach Hazards Statement, Special Weather Statement, Marine Weather Statement). Wildcards surface in an admin queue rather than auto-rendering.

---

## Threshold layer (Open-Meteo edge bands)

```yaml
sst:
  too_cold_swim_c: 13          # SST < this → "swim not advised"
  paw_cold_c: 5                # ambient sand/water → "cold paws on damp sand"
  too_warm_swim_c: 27          # mostly inland lakes; algal-bloom risk

wave_height:
  swim_advisory_m: 1.2         # > this → "skip the swim"

sand_surface:
  paw_burn_c: 50               # > this → "hot asphalt" pattern (existing precedent)

wind:
  small_dog_advisory_ms: 12    # > this → "blowing sand / small-dog windchill"
```

Open question: per-dog-size thresholds? V1 = single thresholds; later versions could personalize by registered dog size. Keep the YAML structure extensible.

---

## Effort estimate (tier-1 CA pilot)

| Track | Effort |
|---|---|
| Open-Meteo marine ingestion + storage | ~½ day |
| NWS alerts ingestion + polygon PIP + translation table | 1-2 days |
| Threshold layer + advisory-engine input adapters | ~½ day |
| Pleasantness-only real-time appeal layer (middle-band data) | ~½ day |
| Surfacing in beach.html + mobile-beach.html | ~1 day |
| Pilot review (30 tier-1 CA, visual + sanity) | ~½ day |
| **Total** | **~4-5 days** |

Scaling to remaining CA + OR/WA after pilot: config extension only, no new architecture.

---

## Pattern this seeds

Once the translation + threshold layer + adapter pattern exists, adding sources gets cheap:

| Future source | Pattern reuse |
|---|---|
| EPA BEACON bacteria | Point-sample → nearest-station match → translation table → advisory engine |
| State water board algal blooms | Polygon or county-level advisory → PIP → translation → advisory engine |
| Domoic acid (CDPH) | Same as algal blooms |
| Park-specific closures (operator pages, NPS Compendium) | Existing codify channel feeds advisory engine for transient closures |
| NOAA tides | Time-series → threshold layer → "very high tide = no usable beach" advisory |

**The NWS work is partly an investment in this pattern**, not just in the NWS coverage itself.

---

## Open design calls

1. **Find the advisory engine code path before coding** — grep for hot-asphalt / advisory generation to confirm exact extension point. The doc assumes a single deterministic-advisory module; verify.
2. **Pilot scope** — 30 tier-1 CA first, then expand to all CA, then OR/WA? Recommend pilot.
3. **Per-dog-size thresholds** — V1 single thresholds; structure YAML extensibly for later.
4. **Real-time appeal composite formula** — explicit for v1 ("pleasant" = SST in middle band AND waves small AND wind low). Learned model only if/when user-feedback labels exist.
5. **Inland lake beaches** — Open-Meteo marine is ocean-only. Inland lakes get regular Open-Meteo weather + water-temp heuristic. Acceptable for v1?
6. **Forecast history retention** — keep forever (lets us validate forecast skill over time, sand-temp derivation, etc.) or rolling 30-day? Storage is cheap; recommend forever.
7. **Wildcard NWS event types** — Beach Hazards Statement, Special Weather Statement, Marine Weather Statement. Admin queue for human translation per instance, or attempt LLM auto-translation with confidence gate?

---

## Related

- [[nws-beach-hazards-integration]] — original deferred pin; this doc reframes it as advisory-engine extension
- [[open-meteo-expansion]] — original killer-add pin; this doc structures the marine pass
- [[entity-modeling]] — measurements-class designation
- [[dog-social-mission]] — why dog-impact framing matters (every translation routes through "what does this mean for the dog")
- [[trust-the-indirection]] — why we don't conflate appeal + safety into one score (each surface preserves its own indirection)
- [[time-aware-today-surface]] — temporal pattern for hour-forward filtering of forecast data
- `docs/codify_cascade_v1_runbook.md` — separate track; advisory engine extension is independent
