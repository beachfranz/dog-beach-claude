// beach-chat/index.ts
// Supabase Edge Function — conversational assistant for beach conditions.
// Accepts POST { location_id, question, conversation_history }
// Returns { answer: string }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ANTHROPIC_API_KEY    = Deno.env.get("anthropic_api_key")!;
const ANTHROPIC_API_URL    = "https://api.anthropic.com/v1/messages";
const MODEL                = "claude-sonnet-4-6";
const MAX_TOKENS           = 1024;

// ─── Entry point ──────────────────────────────────────────────────────────────

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: cors });
  }

  if (req.method !== "POST") {
    return json({ error: "Method not allowed" }, 405);
  }

  // ── Rate limiting: 20 requests per IP per hour ──────────────────────────────
  const forwarded = req.headers.get("x-forwarded-for") ?? "";
  const ip        = forwarded.split(",").at(-1)?.trim() || "unknown";
  const hour = new Date(Math.floor(Date.now() / 3_600_000) * 3_600_000).toISOString();
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data: rateCount } = await supabase.rpc("increment_chat_rate", { p_ip: ip, p_hour: hour });
  if ((rateCount ?? 0) > 20) {
    return json({ answer: "I'm taking a quick break — try again in a little while." }, 429);
  }

  // Occasionally clean up old rate limit rows (older than 24 hours)
  if (Math.random() < 0.1) {
    await supabase.from("chat_rate_limits")
      .delete()
      .lt("hour", new Date(Date.now() - 86_400_000).toISOString());
  }

  let body: { location_id?: string; arena_group_id?: number; question?: string; conversation_history?: ConversationTurn[]; local_date?: string; cross_beach?: boolean; location_ids?: string[] };
  try {
    body = await req.json();
  } catch {
    return json({ error: "Invalid JSON body" }, 400);
  }

  let { location_id, arena_group_id, question, conversation_history = [], local_date, cross_beach, location_ids } = body;

  if (!question || (!location_id && !arena_group_id && !cross_beach)) {
    return json({ error: "question and (location_id, arena_group_id, or cross_beach) are required" }, 400);
  }

  // Resolve location_id ↔ arena_group_id via the spine. Scoring-table
  // queries use arena_group_id (now the PK); the legacy slug is just
  // here to keep input flexible.
  if (!arena_group_id && location_id) {
    const { data: row } = await supabase
      .from("beaches_gold")
      .select("fid")
      .eq("location_id", location_id)
      .limit(1);
    arena_group_id = row?.[0]?.fid ?? undefined;
  }
  if (!location_id && arena_group_id) {
    const { data: row } = await supabase
      .from("beaches_gold")
      .select("location_id")
      .eq("fid", arena_group_id)
      .limit(1);
    location_id = row?.[0]?.location_id ?? undefined;
  }
  if (!arena_group_id && !cross_beach) {
    return json({ error: `Beach not found in spine for input` }, 404);
  }
  try {
    let systemPrompt: string;

    // local_date scopes the chat to a single beach + day (used by detail.html).
    // Comparative-question routing is bypassed: when the user is on a specific
    // day's detail page, they want answers about THAT day, not "go to a
    // different beach." cross_beach (find.html) forces this branch
    // unconditionally — Scout there is never anchored to one beach.
    if ((cross_beach || isComparativeQuestion(question)) && !local_date) {
      // ── Cross-beach mode: summary data across multiple beaches ───────
      // All beaches are in California — use Pacific time for "today"
      const todayPacific = localDateForTimezone(new Date(), "America/Los_Angeles");

      // The 7-day forecast window, as local_date strings.
      const [ty, tm, td] = todayPacific.split("-").map(Number);
      const weekDates = Array.from({ length: 7 }, (_, i) =>
        new Date(Date.UTC(ty, tm - 1, td + i)).toISOString().slice(0, 10)
      );

      const scopedLocationIds = (location_ids ?? []).filter((id): id is string => !!id);

      // deno-lint-ignore no-explicit-any
      let beachesRaw: any[] | null;
      // deno-lint-ignore no-explicit-any
      let allDays: any[];

      if (scopedLocationIds.length) {
        // Caller (e.g. find.html) told us exactly which beaches are on
        // screen — scope Scout to that set so it never surfaces beaches
        // outside the caller's search radius/filters. That set is already
        // small (≤~50), so pull the whole week in one shot per table
        // instead of the per-day ranking used in the unscoped fallback.
        const weekStart = weekDates[0];
        const weekEnd   = weekDates[weekDates.length - 1];
        const [{ data: gold }, { data: days }] = await Promise.all([
          supabase
            .from("beaches_gold")
            .select("fid, location_id, name, display_name_override, timezone")
            .in("location_id", scopedLocationIds),
          supabase
            .from("beach_day_recommendations")
            .select("location_id, local_date, composite_score_v2, composite_score_v3, best_window_label, best_window_text, avg_temp, avg_wind, avg_uv, avg_tide_height, lowest_tide_height, busyness_category, go_hours_count, caution_hours_count, no_go_hours_count, caution_text, risk_reason_codes, positive_reason_codes, summary_weather, bacteria_risk, precip_72h_mm")
            .in("location_id", scopedLocationIds)
            .gte("local_date", weekStart)
            .lte("local_date", weekEnd),
        ]);
        beachesRaw = gold;
        allDays = days ?? [];
      } else {
        // No caller-provided scope — rank top beaches per day nationwide.
        // A single query ordered by (local_date, ...) + one global
        // .limit() would starve every day after the first once beach
        // count exceeds the limit (Scout would only ever see "today"), so
        // query per-day with its own ORDER BY + LIMIT instead.
        // Gold is fetched AFTER the day ranking, scoped to the ranked
        // location_ids — an unbounded active-scored select exceeds the
        // PostgREST 1000-row cap and silently drops ranked beaches.
        const PER_DAY_LIMIT = 15;
        const dayResults = await Promise.all(
          weekDates.map((date) =>
            supabase
              .from("beach_day_recommendations")
              .select("location_id, local_date, composite_score_v2, composite_score_v3, best_window_label, best_window_text, avg_temp, avg_wind, avg_uv, avg_tide_height, lowest_tide_height, busyness_category, go_hours_count, caution_hours_count, no_go_hours_count, caution_text, risk_reason_codes, positive_reason_codes, summary_weather, bacteria_risk, precip_72h_mm")
              .eq("local_date", date)
              .order("composite_score_v3", { ascending: false, nullsFirst: false })
              .order("composite_score_v2", { ascending: false, nullsFirst: false })
              .limit(PER_DAY_LIMIT)
          ),
        );
        allDays = dayResults.flatMap((r) => r.data ?? []);
        const rankedIds = [...new Set(allDays.map((d) => d.location_id as string))];
        const { data: gold } = rankedIds.length
          ? await supabase
              .from("beaches_gold")
              .select("fid, location_id, name, display_name_override, timezone")
              .in("location_id", rankedIds)
              .eq("is_active", true)
          : { data: [] };
        beachesRaw = gold;
      }

      // Only include beaches that actually have a day in the window above —
      // otherwise the roster gets listed with "(no data)" for every beach
      // that didn't come back, bloating the prompt.
      const presentLocationIds = new Set(allDays.map((d) => d.location_id as string));
      const presentGold = (beachesRaw ?? [])
        .filter((g: { location_id: string | null }) => g.location_id && presentLocationIds.has(g.location_id));

      // Dog policy / zone rules — same source single-beach mode uses
      // (beach_dog_policy.zone_rules, with the flat fields as fallback).
      // Amenities — same beach_amenities table beach.html's Amenities
      // card reads. Cross-beach mode was previously missing both
      // entirely, so Scout had nothing to answer with when asked
      // comparatively about zones or facilities.
      // Advisories (beach_advisory) — rip current, high surf, water
      // quality, policy windows, plover, etc. Same unified table
      // beach.html's Cautions card reads.
      const advisoryRangeEnd = new Date(
        new Date(`${weekDates[weekDates.length - 1]}T00:00:00Z`).getTime() + 2 * 86_400_000
      ).toISOString();

      const fids = presentGold.map((g: { fid: number }) => g.fid);
      const [{ data: policyRows }, { data: amenityRows }, { data: advisoryRows }] = fids.length
        ? await Promise.all([
            supabase
              .from("beach_dog_policy")
              .select("arena_group_id, dogs_allowed, leash_policy, has_on_leash, has_off_leash, off_leash_flag, zone_rules")
              .in("arena_group_id", fids),
            supabase
              .from("beach_amenities")
              .select("arena_group_id, has_restrooms, has_showers, has_lifeguards, has_drinking_water, has_disabled_access, has_food, has_fire_pits, has_picnic_area, parking_type")
              .in("arena_group_id", fids),
            supabase
              .from("beach_advisory")
              .select("beach_fid, event_type, severity, valid_from, valid_to, dog_impact_text, label")
              .in("beach_fid", fids)
              .gte("valid_to", new Date().toISOString())
              .lte("valid_from", advisoryRangeEnd),
          ])
        : [{ data: [] as Record<string, unknown>[] }, { data: [] as Record<string, unknown>[] }, { data: [] as Record<string, unknown>[] }];
      const policyByFid = new Map((policyRows ?? []).map((p: { arena_group_id: number }) => [p.arena_group_id, p]));
      const amenitiesByFid = new Map((amenityRows ?? []).map((a: { arena_group_id: number }) => [a.arena_group_id, a]));
      const advisoriesByFid = new Map<number, Record<string, unknown>[]>();
      for (const a of (advisoryRows ?? []) as { beach_fid: number }[]) {
        const arr = advisoriesByFid.get(a.beach_fid) ?? [];
        arr.push(a);
        advisoriesByFid.set(a.beach_fid, arr);
      }

      // Reshape gold rows into the {location_id, display_name, timezone,
      // zoneSummary, flatPolicyLine, amenitiesLine} shape that
      // buildCrossBeachPrompt expects.
      const beaches = presentGold
        .map((g: { fid: number; location_id: string | null; name: string; display_name_override: string | null; timezone: string }) => {
          const p = policyByFid.get(g.fid) as Record<string, unknown> | undefined;
          const zoneSummary = buildZoneSummary((p?.zone_rules as Record<string, unknown>) ?? null);
          const flatBits: string[] = [];
          if (p?.dogs_allowed != null)    flatBits.push(`dogs_allowed=${p.dogs_allowed}`);
          if (p?.leash_policy)            flatBits.push(`leash_policy=${p.leash_policy}`);
          if (p?.has_on_leash != null)    flatBits.push(`has_on_leash_zones=${p.has_on_leash}`);
          if (p?.has_off_leash != null)   flatBits.push(`has_off_leash_zones=${p.has_off_leash}`);

          const a = amenitiesByFid.get(g.fid) as Record<string, unknown> | undefined;
          const amenBits: string[] = [];
          if (a?.has_restrooms != null)       amenBits.push(`restrooms=${a.has_restrooms}`);
          if (a?.has_showers != null)         amenBits.push(`showers=${a.has_showers}`);
          if (a?.has_lifeguards != null)      amenBits.push(`seasonal_lifeguards=${a.has_lifeguards}`);
          if (a?.has_drinking_water != null)  amenBits.push(`drinking_water=${a.has_drinking_water}`);
          if (a?.has_disabled_access != null) amenBits.push(`ada_access=${a.has_disabled_access}`);
          if (a?.has_food != null)            amenBits.push(`food_nearby=${a.has_food}`);
          if (a?.has_fire_pits != null)       amenBits.push(`fire_pits=${a.has_fire_pits}`);
          if (a?.has_picnic_area != null)     amenBits.push(`picnic_area=${a.has_picnic_area}`);
          if (a?.parking_type)                amenBits.push(`parking=${a.parking_type}`);

          return {
            location_id:    g.location_id ?? null,
            display_name:   g.display_name_override ?? g.name,
            timezone:       g.timezone ?? "America/Los_Angeles",
            zoneSummary,
            flatPolicyLine: flatBits.join(", "),
            amenitiesLine:  amenBits.join(", "),
            advisories:     advisoriesByFid.get(g.fid) ?? [],
          };
        });
      systemPrompt = buildCrossBeachPrompt(beaches, allDays);

    } else {
      // ── Single-beach mode: full detail for current beach ─────────────
      // Single-beach mode: all on the spine.
      // Reaching this branch means cross_beach was falsy, so the earlier
      // guard already 404'd if arena_group_id were missing — re-checked
      // here only to narrow the type for TS.
      if (!arena_group_id) {
        return json({ error: `Beach not found in spine for input` }, 404);
      }
      const { data: goldRows, error: beachErr } = await supabase
        .from("beaches_gold")
        .select(`
          fid,
          location_id,
          name,
          display_name_override,
          timezone,
          open_time,
          close_time,
          address,
          website,
          description
        `)
        .eq("fid", arena_group_id)
        .limit(1);

      if (beachErr || !goldRows?.length) {
        return json({ error: `Beach not found in spine: arena_group_id=${arena_group_id}` }, 404);
      }
      const g = goldRows[0] as { fid: number; location_id: string | null; name: string;
                                  display_name_override: string | null;
                                  timezone: string; open_time: string | null; close_time: string | null;
                                  address: string | null; website: string | null; description: string | null };
      const beach = {
        location_id:    g.location_id,
        arena_group_id: g.fid,
        display_name:   g.display_name_override ?? g.name,
        timezone:       g.timezone ?? "America/Los_Angeles",
        open_time:      g.open_time,
        close_time:     g.close_time,
        address:        g.address,
        website:        g.website,
        description:    g.description,
      };

      // LLM-extracted policy metadata for this beach (leash rules, dog
      // zones, hours, etc.). Drives Scout's activity advice — Scout must
      // not suggest off-leash play if leash is required, must not
      // suggest sand/wave play if dogs aren't allowed on sand, etc.
      let metadata: Record<string, unknown> | null = null;
      let dogPolicy: Record<string, unknown> | null = null;
      let amenities: Record<string, unknown> | null = null;
      if (beach.arena_group_id) {
        const [metaRes, dpRes, amenRes] = await Promise.all([
          supabase
            .from("arena_beach_metadata")
            .select(
              "dogs_allowed, dogs_leash_required, dogs_off_leash_area, " +
              "dogs_seasonal_restrictions, dogs_time_restrictions, " +
              "dogs_policy_notes, dogs_allowed_areas, hours_text"
            )
            .eq("arena_group_id", beach.arena_group_id)
            .maybeSingle(),
          // Modern consensus-driven dog policy. Prefer over legacy
          // metadata when present (post-2026-05-07 binary-leash schema).
          supabase
            .from("beach_dog_policy")
            .select(
              "dogs_allowed, leash_policy, has_on_leash, has_off_leash, " +
              "off_leash_flag, zone_rules"
            )
            .eq("arena_group_id", beach.arena_group_id)
            .maybeSingle(),
          // Facility amenities — same table beach.html's Amenities card
          // reads. Lets Scout answer "does this beach have restrooms /
          // showers / parking / disabled access?" instead of guessing.
          supabase
            .from("beach_amenities")
            .select(
              "has_restrooms, has_showers, has_lifeguards, has_drinking_water, " +
              "has_disabled_access, has_food, has_fire_pits, has_picnic_area, " +
              "parking_type, parking_notes, hours_text"
            )
            .eq("arena_group_id", beach.arena_group_id)
            .maybeSingle(),
        ]);
        metadata = metaRes.data ?? null;
        dogPolicy = dpRes.data ?? null;
        amenities = amenRes.data ?? null;
      }

      // Get current local date + hour in the beach's timezone
      const nowUtc = new Date();
      const today  = localDateForTimezone(nowUtc, beach.timezone as string);
      const localParts = new Intl.DateTimeFormat("en-US", {
        timeZone: beach.timezone as string,
        hour: "2-digit", hour12: false,
      }).formatToParts(nowUtc);
      const currentLocalHour = parseInt(
        localParts.find(p => p.type === "hour")?.value ?? "0"
      ) % 24;
      // Hour-granular so the cache hash stays stable within the hour
      // (minute precision would bust beach_chat_cache every minute).
      const currentTimeLabel = new Intl.DateTimeFormat("en-US", {
        timeZone: beach.timezone as string,
        hour: "numeric", hour12: true,
      }).format(nowUtc);

      // Scope: if local_date is provided, fetch only that one day. Otherwise
      // fetch the next 7 days starting today.
      const dayQuery = supabase
        .from("beach_day_recommendations")
        .select("*")
        .eq("location_id", location_id);
      const hourQuery = supabase
        .from("beach_day_hourly_scores")
        .select("local_date, local_hour, hour_label, hour_score_v2, hour_score_v3, tide_height, wind_speed, temp_air, feels_like, sand_temp, asphalt_temp, busyness_score, precip_chance, uv_index, busyness_category, is_in_best_window, is_candidate_window")
        .eq("location_id", location_id);

      // Advisories (beach_advisory) — rip current, high surf, water
      // quality, policy windows, plover, etc. Same unified table
      // beach.html's Cautions card reads (get_beach_advisories RPC).
      // Fetched directly here (not via the RPC) so one query covers the
      // whole visible date range instead of one call per day; valid_to/
      // valid_from bucketing per day below is date-string overlap, which
      // is close enough for a narrative mention (not pixel-exact tile
      // rendering like the RPC does).
      const [advY, advM, advD] = (local_date ?? today).split("-").map(Number);
      const advisoryRangeEnd = new Date(Date.UTC(advY, advM - 1, advD + (local_date ? 2 : 9))).toISOString();
      const advisoryQuery = supabase
        .from("beach_advisory")
        .select("event_type, severity, valid_from, valid_to, dog_impact_text, label")
        .eq("beach_fid", beach.arena_group_id)
        .gte("valid_to", new Date().toISOString())
        .lte("valid_from", advisoryRangeEnd);

      const [{ data: days, error: daysErr }, { data: hours, error: hoursErr }, { data: advisories }] = await Promise.all([
        local_date
          ? dayQuery.eq("local_date", local_date)
          : dayQuery.gte("local_date", today).order("local_date", { ascending: true }).limit(7),
        local_date
          ? hourQuery.eq("local_date", local_date).order("local_hour", { ascending: true })
          : hourQuery.gte("local_date", today).order("local_date", { ascending: true }).order("local_hour", { ascending: true }),
        advisoryQuery,
      ]);

      if (daysErr) throw new Error(`Failed to load daily data: ${daysErr.message}`);
      if (hoursErr) throw new Error(`Failed to load hourly data: ${hoursErr.message}`);

      // Filter out past hours when the scoped day IS today.
      const filterDate = local_date ?? today;
      const remainingHours = (hours ?? []).filter(h =>
        h.local_date !== filterDate || Number(h.local_hour) >= currentLocalHour
      );

      systemPrompt = buildSystemPrompt(beach, days ?? [], remainingHours, currentTimeLabel, local_date ?? null, metadata, dogPolicy, amenities, advisories ?? []);
    }

    // ── 60-min cache (only for single-beach context + no chat history) ──
    // Cross-beach prompts and follow-up conversations skip the cache —
    // they're either rare or have unique state that wouldn't replay.
    const isCacheable =
      arena_group_id != null
      && (!conversation_history || conversation_history.length === 0);
    let cacheHash: string | null = null;
    if (isCacheable) {
      cacheHash = await sha256Hex(systemPrompt + "␟" + question);
      const { data: cached } = await supabase
        .from("beach_chat_cache")
        .select("answer, generated_at")
        .eq("arena_group_id", arena_group_id!)
        .eq("question_hash", cacheHash)
        .maybeSingle();
      if (cached?.answer && cached?.generated_at) {
        const ageMs = Date.now() - new Date(cached.generated_at).getTime();
        if (ageMs < 60 * 60 * 1000) {
          return json({ answer: cached.answer, cached: true });
        }
      }
    }

    const answer = await callAnthropic(systemPrompt, conversation_history, question);

    if (isCacheable && cacheHash && answer) {
      // Fire-and-forget upsert (don't block the response on cache write).
      supabase.from("beach_chat_cache").upsert({
        arena_group_id, question_hash: cacheHash,
        answer, generated_at: new Date().toISOString(),
      }, { onConflict: "arena_group_id,question_hash" }).then();
    }

    return json({ answer });

  } catch (err) {
    console.error("beach-chat error:", String(err));
    return json({ error: String(err) }, 500);
  }
});

async function sha256Hex(s: string): Promise<string> {
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
  return Array.from(new Uint8Array(buf))
    .map(b => b.toString(16).padStart(2, "0")).join("");
}

// ─── v2 status mapping ─────────────────────────────────────────────────────
// Inline copy of public.v2_signal_status thresholds so we don't need an
// RPC roundtrip per hour to flag risks for Scout's prompt. Single source
// of truth on the DB side is public.scoring_config_v2 / v2_signal_status
// (migration 20260530_v2_signal_status_helper.sql). Pin:
// [[v2-signal-status-mapping]]. Keep this in sync when bands change.
type V2Status = "clear" | "advisory" | "caution" | "no_go";
function v2StatusFor(signal: string, v: number | null | undefined): V2Status | null {
  if (v == null) return null;
  switch (signal) {
    case "uv":
      if (v >= 11) return "no_go";
      if (v >= 9)  return "caution";    // 9-10 ≥ 34% of max-band 8
      if (v >= 6)  return "advisory";   // 6-8 advisory band
      return "clear";
    case "asphalt":
      if (v >= 125) return "no_go";     // gate
      if (v >= 115) return "advisory";  // 115-125 advisory; gate sits at 125
      return "clear";
    case "sand":
      if (v >= 145) return "no_go";
      if (v >= 135) return "caution";   // 135-145 score 8 / max 10 = 80% no_go-leaning; call caution to leave headroom
      if (v >= 125) return "caution";
      if (v >= 115) return "advisory";
      return "clear";
    case "tide":
      // 20260615: capped at caution. Tide is an availability signal,
      // not a safety one. Keep in sync with public.v2_signal_status
      // (migration 20260615a_tide_demote_one_severity.sql).
      if (v >= 7) return "caution";
      if (v >= 5) return "advisory";
      return "clear";
    case "wind":
      if (v >= 35) return "no_go";
      if (v >= 19) return "caution";    // 19-25 score 4/10 = 40% caution
      if (v >= 13) return "advisory";
      return "clear";
    case "crowd":
      if (v >= 85) return "no_go";
      if (v >= 60) return "caution";    // 60-85 score 3/5 = 60% caution
      if (v >= 30) return "advisory";
      return "clear";
    case "precip":
      if (v >= 80) return "no_go";
      if (v >= 50) return "caution";
      if (v >= 30) return "advisory";
      return "clear";
    case "feels_hot":
      if (v >= 125) return "no_go";
      if (v >= 95)  return "caution";
      if (v >= 85)  return "advisory";
      return "clear";
    case "feels_cold":
      if (v <= 20) return "no_go";
      if (v <= 32) return "caution";
      if (v <= 50) return "advisory";
      return "clear";
    default:
      return null;
  }
}
// Worst-of two statuses, used for hour-level rollup
function v2Worst(a: V2Status | null, b: V2Status | null): V2Status | null {
  const rank = (s: V2Status | null) =>
    s === "no_go" ? 3 : s === "caution" ? 2 : s === "advisory" ? 1 : s === "clear" ? 0 : -1;
  return rank(a) >= rank(b) ? a : b;
}

// ─── Types ────────────────────────────────────────────────────────────────────

interface ConversationTurn {
  role: "user" | "assistant";
  content: string;
}

// ─── Prompt builder ───────────────────────────────────────────────────────────

function buildSystemPrompt(
  beach: Record<string, unknown>,
  days: Record<string, unknown>[],
  hours: Record<string, unknown>[],
  currentTimeLabel?: string,
  scopedDate: string | null = null,
  metadata: Record<string, unknown> | null = null,
  dogPolicy: Record<string, unknown> | null = null,
  amenities: Record<string, unknown> | null = null,
  // deno-lint-ignore no-explicit-any
  advisories: any[] = [],
): string {
  const hoursByDate = new Map<string, Record<string, unknown>[]>();
  for (const h of hours) {
    const date = h.local_date as string;
    const arr = hoursByDate.get(date) ?? [];
    arr.push(h);
    hoursByDate.set(date, arr);
  }

  // Off-leash availability gates fetch/retrieve tips: you cannot play fetch
  // with a leashed dog. Walk zone_rules first; fall back to the legacy
  // has_off_leash boolean. Tips that plant "fetch" in Scout's context will
  // make Scout suggest "leashed fetch" — incoherent, so suppress them.
  let _anyOffLeash = false;
  const _zr = (dogPolicy?.zone_rules ?? null) as Record<string, unknown> | null;
  const _regions = Array.isArray(_zr?.regions) ? _zr!.regions as Record<string, unknown>[]
    : Array.isArray((_zr as Record<string, unknown>)?.seasons)
      ? ((_zr as Record<string, unknown>).seasons as Record<string, unknown>[]).flatMap(s => (s?.regions as Record<string, unknown>[]) || [])
      : [];
  for (const reg of _regions) {
    const secs = (reg?.sections ?? {}) as Record<string, Record<string, unknown>>;
    for (const sd of Object.values(secs)) {
      if (sd?.rule === "off_leash") { _anyOffLeash = true; break; }
      const tws = (sd?.time_windows ?? []) as Record<string, unknown>[];
      if (tws.some(tw => tw?.rule === "off_leash")) { _anyOffLeash = true; break; }
    }
    if (_anyOffLeash) break;
  }
  if (!_anyOffLeash && dogPolicy?.has_off_leash === true) _anyOffLeash = true;

  const daysContext = days.map((d) => {
    const date = d.local_date as string;
    const dayHours = hoursByDate.get(date) ?? [];

    // is_weekend
    const jsDate = new Date(`${date}T12:00:00`);
    const dayOfWeek = jsDate.toLocaleDateString("en-US", { weekday: "long" });
    const isWeekend = dayOfWeek === "Saturday" || dayOfWeek === "Sunday";

    // feelsLike wind chill
    const avgTemp = d.avg_temp !== null ? parseFloat(String(d.avg_temp)) : null;
    const avgWind = d.avg_wind !== null ? parseFloat(String(d.avg_wind)) : null;
    let feelsLike: number | null = null;
    if (avgTemp !== null && avgWind !== null) {
      if (avgTemp <= 50 || avgWind >= 3) {
        feelsLike = Math.round(
          35.74 + 0.6215 * avgTemp - 35.75 * Math.pow(avgWind, 0.16) + 0.4275 * avgTemp * Math.pow(avgWind, 0.16)
        );
      } else {
        feelsLike = Math.round(avgTemp);
      }
    }

    // tide direction from best window hours
    const bestWindowHours = dayHours.filter((h) => h.is_in_best_window);
    const tideSeries = bestWindowHours
      .map((h) => h.tide_height !== null ? parseFloat(String(h.tide_height)) : null)
      .filter((v): v is number => v !== null);
    let tideDirection = "steady";
    if (tideSeries.length >= 2) {
      const diff = tideSeries[tideSeries.length - 1] - tideSeries[0];
      if (diff > 0.15)       tideDirection = "rising";
      else if (diff < -0.15) tideDirection = "falling";
    }

    // practical tips — dog needs first, human comfort secondary
    const tips: string[] = [];
    const avgUv = d.avg_uv !== null ? parseFloat(String(d.avg_uv)) : null;
    const lowestTide = d.lowest_tide_height !== null ? parseFloat(String(d.lowest_tide_height)) : null;

    // Dog essentials
    tips.push("fresh water and a bowl for the dog");
    if (lowestTide !== null && lowestTide <= 1.0) {
      tips.push(_anyOffLeash
        ? "low tide = great fetch/swim — bring a ball and towel for the dog"
        : "low tide = wide leashed walk with tide pools to sniff — bring a towel");
    }
    if (avgUv !== null && avgUv >= 6) tips.push(`dog sunscreen for nose/ears (UV ${Math.round(avgUv)})`);
    if (avgTemp !== null && avgTemp >= 80) tips.push("hot sand — dog booties or arrive early before it heats up");
    if (d.busyness_category === "dog_party" || d.busyness_category === "too_crowded") tips.push("long leash for crowded beach");

    // Human comfort (secondary)
    if (feelsLike !== null && feelsLike < 62) tips.push(`layer up — feels like ${feelsLike}°F`);

    // reason codes
    const positives = Array.isArray(d.positive_reason_codes) ? (d.positive_reason_codes as string[]).join(", ") : "";
    const risks     = Array.isArray(d.risk_reason_codes)     ? (d.risk_reason_codes     as string[]).join(", ") : "";

    // All daylight hours with v2 status flags. Switched from v1 *_status
    // columns to v2-derived statuses per Franz 2026-05-30 v1-retirement
    // task #8 — v1 noise (e.g. asphalt 111°F "advisory") was producing
    // false-positive cautions in Scout's prompt and narration.
    const allDaylightHours = dayHours.filter((h) => h.is_candidate_window || h.is_in_best_window);
    const hourLines = allDaylightHours.map((h) => {
      const sTide    = v2StatusFor("tide",       (h.tide_height as number | null));
      const sWind    = v2StatusFor("wind",       (h.wind_speed as number | null));
      const sRain    = v2StatusFor("precip",     (h.precip_chance as number | null));
      const sCrowd   = v2StatusFor("crowd",      (h.busyness_score as number | null));
      const sFeelsH  = v2StatusFor("feels_hot",  (h.feels_like as number | null));
      const sFeelsC  = v2StatusFor("feels_cold", (h.feels_like as number | null));
      const sUv      = v2StatusFor("uv",         (h.uv_index as number | null));
      const sSand    = v2StatusFor("sand",       (h.sand_temp as number | null));
      const sAsphalt = v2StatusFor("asphalt",    (h.asphalt_temp as number | null));
      const sTemp    = v2Worst(sFeelsH, sFeelsC);
      const hourV2   = [sTide, sWind, sRain, sCrowd, sTemp, sUv, sSand, sAsphalt]
                        .reduce<V2Status | null>((w, s) => v2Worst(w, s), null) ?? "clear";
      const notClear = (s: V2Status | null) => s && s !== "clear";
      const flags = [
        notClear(sTide)    ? `tide:${sTide}`       : null,
        notClear(sWind)    ? `wind:${sWind}`       : null,
        notClear(sRain)    ? `rain:${sRain}`       : null,
        notClear(sCrowd)   ? `crowd:${sCrowd}`     : null,
        notClear(sTemp)    ? `temp:${sTemp}`       : null,
        notClear(sUv)      ? `uv:${sUv}`           : null,
        notClear(sSand)    ? `sand:${sSand}`       : null,
        notClear(sAsphalt) ? `asphalt:${sAsphalt}` : null,
      ].filter(Boolean).join(", ");
      const marker = h.is_in_best_window ? " ★" : "";
      return `    ${h.hour_label}${marker}: tide=${fmtNum(h.tide_height, "ft")} wind=${fmtNum(h.wind_speed, "mph")} feels=${fmtNum(h.feels_like, "°F")} rain=${fmtNum(h.precip_chance, "%")} crowd=${h.busyness_category ?? "?"} [${hourV2}]${flags ? ` flags: ${flags}` : ""}`;
    }).join("\n");

    const bacteriaRisk = d.bacteria_risk ?? "none";
    const bacteriaLine = bacteriaRisk === "high"     ? `  ⚠️ BACTERIA RISK HIGH: ${d.precip_72h_mm ?? 0}mm rain in past 72h — advise against swimming`
                       : bacteriaRisk === "moderate" ? `  ⚠️ BACTERIA ADVISORY: ${d.precip_72h_mm ?? 0}mm rain in past 72h — above 2.5mm advisory threshold`
                       : bacteriaRisk === "low"      ? `  Note: ${d.precip_72h_mm ?? 0}mm rain in past 72h (below advisory threshold)`
                       : "";

    // Advisories active on this date (rip current, high surf, water
    // quality, plover closures, NWS alerts, etc.) — date-string overlap
    // against valid_from/valid_to, same beach_advisory rows beach.html's
    // Cautions card renders.
    const dayAdvisories = advisories.filter((a) => {
      const df = String(a.valid_from).slice(0, 10);
      const dt = String(a.valid_to).slice(0, 10);
      return date >= df && date <= dt;
    });
    const advisoryLines = dayAdvisories.map((a) => {
      const label  = a.label || a.event_type || "Advisory";
      const impact = a.dog_impact_text ? `: ${trim(a.dog_impact_text, 200)}` : "";
      return `  ⚠️ ADVISORY [${a.severity}] ${label}${impact}`;
    }).join("\n");

    return `
  ${date} ${dayOfWeek.toUpperCase()} ${isWeekend ? "[WEEKEND]" : "[WEEKDAY]"}
  Hours: ${d.go_hours_count ?? 0} go / ${d.caution_hours_count ?? 0} caution / ${d.no_go_hours_count ?? 0} no-go
  Best window: ${d.best_window_label ?? "none"} | Weather: ${d.summary_weather ?? "unknown"} | Tide: ${fmtNum(d.avg_tide_height, "ft")} avg, ${fmtNum(lowestTide, "ft")} low, ${tideDirection} | Wind: ${fmtNum(d.avg_wind, "mph")} | Temp: ${fmtNum(d.avg_temp, "°F")}${feelsLike !== null ? ` (feels ${feelsLike}°F)` : ""} | UV: ${fmtNum(d.avg_uv, "")} | Crowds: ${d.busyness_category ?? "unknown"}
  ${positives ? `Positives: ${positives}` : ""}
  ${risks ? `Risks: ${risks}` : ""}
  ${bacteriaLine}
  ${advisoryLines}
  ${tips.length ? `Tips: ${tips.join("; ")}` : ""}
  Best window note: ${d.best_window_text ?? "n/a"}
  ${d.caution_text ? `Caution: ${d.caution_text}` : ""}${d.no_go_text ? `No-go reason: ${d.no_go_text}` : ""}
  Hourly breakdown (★ = best window):
${hourLines || "    (none)"}`;
  }).join("\n");

  // ── Dog policy block ─────────────────────────────────────────────────
  // Scout MUST respect these rules. They drive activity recommendations.
  // Prefers beach_dog_policy (consensus-driven, with binary has_on_leash /
  // has_off_leash from the 2026-05-07 schema migration) when present.
  // Falls back to legacy arena_beach_metadata extraction for beaches not
  // yet promoted.
  const dogPolicyLines: string[] = [];
  let dogAdviceConstraints = "";

  // Pull from modern source first, fall back to legacy
  const modern = dogPolicy ?? null;
  const dogsAllowed = (modern?.dogs_allowed as string | null)
    ?? (metadata?.dogs_allowed as string | null) ?? null;
  const hasOnLeash  = modern && typeof modern.has_on_leash  === "boolean" ? modern.has_on_leash  as boolean : null;
  const hasOffLeash = modern && typeof modern.has_off_leash === "boolean" ? modern.has_off_leash as boolean : null;
  // dogs_allowed_areas was retired from beach_dog_policy 2026-05-07 (pin #19);
  // legacy arena_beach_metadata view still carries the field for back-compat.
  const allowedAreas = (metadata?.dogs_allowed_areas as string | null) ?? null;
  const seasonal     = (metadata?.dogs_seasonal_restrictions as string | null) || null;
  const timeRules    = (metadata?.dogs_time_restrictions as string | null) || null;
  const policyNotes  = (metadata?.dogs_policy_notes as string | null) || null;
  const offLeashArea = (metadata?.dogs_off_leash_area as string | null) || null;

  // Zone rules are the authoritative source for what the dog can do
  // and where. When zone_rules is present, we send ONLY zone_rules
  // and a single per-section rule — the flat dog_policy binaries
  // (dogs_allowed/has_on_leash/has_off_leash) are derived noise that
  // misleads Scout for beaches like Newport Municipal where the
  // binary has_on_leash=true reflects TRAILS being on-leash but the
  // SAND is prohibited. The section grid is unambiguous; the flat
  // fields aren't.
  //
  // Whole beach is the first-order zone. When a beach has just one
  // zone (no named sub-zones), zone_rules represents it as a region
  // with name=null whose sections describe the entire beach.
  const zoneRules = (modern?.zone_rules ?? null) as Record<string, unknown> | null;
  const zoneSummary = buildZoneSummary(zoneRules);
  const globalNotes = (zoneRules?.global_notes as string | null) || null;
  const haveZoneRules = zoneSummary.length > 0;

  const constraints: string[] = [];

  if (haveZoneRules) {
    // Primary path: zone_rules is the truth. One clean rule.
    constraints.push(
      "ZONE RULES are the authoritative source for what the dog can do and where. " +
      "Each zone (named or 'Whole beach') lists sections (sand, water_swim, trails, " +
      "parking_lot, picnic_area, restrooms, showers, playground, dunes, tide_pools, " +
      "boardwalk, bluff, campground) with one of these rules: " +
      "  - off_leash: dog can be off-leash in this section " +
      "  - on_leash: dog must be on-leash in this section " +
      "  - not_allowed: dog cannot enter this section AT ALL — never suggest activity here. " +
      "Recommend ONLY activities in sections marked off_leash or on_leash, and ONLY at the " +
      "leash level the section specifies. If the user asks about an activity that requires " +
      "a not_allowed section (e.g. swim when water_swim=not_allowed, fetch when " +
      "sand=not_allowed), say so directly and suggest an allowed alternative on this beach " +
      "or none if nothing fits. " +
      "If a section has time_windows, the rule changes by time of day — apply the window " +
      "matching the user's intended visit hour."
    );
  } else {
    // Fallback path: no zone_rules. Use the legacy flat fields.
    if (dogsAllowed)            dogPolicyLines.push(`- dogs_allowed: ${dogsAllowed}`);
    if (hasOnLeash !== null)    dogPolicyLines.push(`- has_on_leash zones: ${hasOnLeash}`);
    if (hasOffLeash !== null)   dogPolicyLines.push(`- has_off_leash zones: ${hasOffLeash}`);
    if (offLeashArea)           dogPolicyLines.push(`- off_leash_area: ${trim(offLeashArea, 200)}`);
    if (allowedAreas)           dogPolicyLines.push(`- allowed_areas: ${trim(allowedAreas, 200)}`);
    if (seasonal)               dogPolicyLines.push(`- seasonal: ${trim(seasonal, 200)}`);
    if (timeRules)              dogPolicyLines.push(`- time_rules: ${trim(timeRules, 200)}`);
    if (policyNotes)            dogPolicyLines.push(`- notes: ${trim(policyNotes, 400)}`);

    if (dogsAllowed === "no" || (hasOnLeash === false && hasOffLeash === false)) {
      constraints.push("Dogs are NOT allowed at this beach. Do NOT suggest fetch, swim, or any sand/wave activity. If there is an allowed_area (parking lot, multi-use trail), point the user there. If no allowed area exists, gently say this isn't a dog beach today.");
    } else if (hasOnLeash === true && hasOffLeash === true) {
      constraints.push("This beach has BOTH on-leash and off-leash zones. Off-leash activity is OK only in the designated off-leash area; outside it, the dog must be leashed.");
    } else if (hasOffLeash === true && hasOnLeash !== true) {
      constraints.push("This is an off-leash beach. The dog can run free, fetch in the waves, swim.");
    } else if (hasOnLeash === true && hasOffLeash !== true) {
      constraints.push("Leash is required at this beach. Never suggest off-leash play or unrestrained swimming.");
    } else if (hasOnLeash === null && hasOffLeash === null && dogsAllowed === "yes") {
      constraints.push("Policy specifics aren't fully extracted. Default to leashed advice and recommend the user verify on-site signage.");
    }
    if (dogsAllowed === "seasonal" || seasonal) {
      constraints.push("Seasonal restrictions apply. Confirm the current date is within the dog-friendly window before recommending off-leash play.");
    }
  }

  dogAdviceConstraints = constraints.length
    ? `\nDOG POLICY CONSTRAINTS (HARD RULES — never violate):\n${constraints.map(c => `- ${c}`).join("\n")}\n`
    : "";
  const dogPolicyBlock = (haveZoneRules || dogPolicyLines.length)
    ? `\nDOG POLICY:\n`
      + (haveZoneRules
          ? `ZONE RULES (section-by-section, authoritative):\n${zoneSummary}\n`
          + (globalNotes ? `\nPOLICY NOTES: ${trim(globalNotes, 600)}\n` : "")
          : `${dogPolicyLines.join("\n")}\n`)
    : "";

  // ── Amenities block ──────────────────────────────────────────────────
  // Same beach_amenities table beach.html's Amenities card reads.
  const amenityBits: string[] = [];
  if (amenities) {
    if (amenities.has_restrooms != null)       amenityBits.push(`restrooms: ${amenities.has_restrooms ? "yes" : "no"}`);
    if (amenities.has_showers != null)         amenityBits.push(`showers: ${amenities.has_showers ? "yes" : "no"}`);
    if (amenities.has_lifeguards != null)      amenityBits.push(`lifeguards: ${amenities.has_lifeguards ? "seasonal" : "no"}`);
    if (amenities.has_drinking_water != null)  amenityBits.push(`drinking water: ${amenities.has_drinking_water ? "yes" : "no"}`);
    if (amenities.has_disabled_access != null) amenityBits.push(`disabled/ADA access: ${amenities.has_disabled_access ? "yes" : "no"}`);
    if (amenities.has_food != null)            amenityBits.push(`food nearby: ${amenities.has_food ? "yes" : "no"}`);
    if (amenities.has_fire_pits != null)       amenityBits.push(`fire pits: ${amenities.has_fire_pits ? "yes" : "no"}`);
    if (amenities.has_picnic_area != null)     amenityBits.push(`picnic area: ${amenities.has_picnic_area ? "yes" : "no"}`);
    if (amenities.parking_type)                amenityBits.push(`parking: ${amenities.parking_type}${amenities.parking_notes ? ` (${trim(amenities.parking_notes as string, 150)})` : ""}`);
    if (amenities.hours_text)                  amenityBits.push(`posted hours: ${trim(amenities.hours_text as string, 150)}`);
  }
  const amenitiesBlock = amenityBits.length
    ? `\nAMENITIES:\n${amenityBits.map(b => `- ${b}`).join("\n")}\n`
    : `\nAMENITIES: no data on file — if asked, say you don't have that and suggest checking on-site signage.\n`;

  return `You are Scout — a local surfer who's been bringing your dog to ${beach.display_name} for years. You know every sandbar, every swell window, when the kooks show up, and when it's firing. You text like a surfer — laid back, uses surf/beach slang naturally (swell, glassy, onshore, sectiony, blown out, dawn patrol, dropping in, firing, going off, closeout, mushy, punchy, clean, choppy, overhead, waist-high), first-person, never formal. You're stoked to help but keep it real — if it's blown out, say it's blown out.

BEACH: ${beach.display_name}
${beach.address ? `Address: ${beach.address}` : ""}
${beach.open_time ? `Hours: ${beach.open_time} – ${beach.close_time}` : ""}
${beach.description ? `About: ${beach.description}` : ""}
${beach.website ? `Website: ${beach.website}` : ""}
Timezone: ${beach.timezone}
${dogPolicyBlock}${dogAdviceConstraints}${amenitiesBlock}

${currentTimeLabel ? `Current local time: ${currentTimeLabel} — only today's remaining hours are shown in the hourly data below.` : ""}
${scopedDate
  ? `SCOPE: This conversation is about ${scopedDate} ONLY. The data below is for that single day. If the user asks about another date, the weather a different day, or whether they should go on a different day, say you can only speak to ${scopedDate} on this screen and tell them to switch to that day's view.\n\nFORECAST DATA (single day):`
  : `7-DAY FORECAST DATA:`}
${daysContext}

Rules:
- Answer questions about conditions, timing, crowds, tides, weather using the data above
- Reference specific hours, dates, and numbers when relevant — but weave them in naturally, don't just list them
${scopedDate
  ? `- DO NOT mention or recommend any other day. Only ${scopedDate}.`
  : `- If the user asks about a day not in the data, say you only have 7 days ahead`}
- Keep answers to 2 sentences max, 3 only if a third sentence meaningfully adds context to your answer
- Lead with a direct answer to the question — no preamble, no restating the question
- No emojis, no markdown formatting, plain text only — EXCEPT: when the question explicitly asks you to bold something (e.g. a time window), wrap it in **double asterisks** so the client can convert it to bold. Only bold what the question asks; never bold anything else.
- If conditions are bad, say so honestly — don't sugarcoat it
- Crowd terms: quiet = few people, moderate = getting busy, dog_party = packed with dogs, too_crowded = avoid
- Never mention numeric scores (hour_score, tide_score, etc.) unless the user explicitly asks about them — use the conditions and statuses to inform your language instead
- When giving pack advice, lead with the dog's needs (water, towel, fetch ball, sunscreen, booties, leash) — but the kahu (the human handler) is ALSO a body on the beach. When cautions are active (high UV, heat, cold, wind, high tide, bacteria), address kahu safety + comfort alongside the dog's: sunscreen for the kahu when UV's punching, a layer when it's chilly, hydration in heat, "stick to the upper beach, watch for kids and dogs being pushed toward the cliffs" on high tide. The dog comes first; the kahu is a close second.
- Always assume the user is bringing their dog; frame all advice through that lens
- LIFEGUARDS ARE SEASONAL — never say "lifeguards on duty" or "lifeguards on staff" (implies year-round staffing, which is false at almost every US beach). Say "seasonal lifeguards" or just "lifeguards". If a specific window is needed, default to "roughly Memorial Day to Labor Day" unless the data above explicitly gives one.
- DOG POLICY is non-negotiable — never suggest activities that violate the leash rule or "no dogs on sand" rule above. If the policy says leash required, the dog stays leashed; if dogs aren't allowed on sand, point the user to the allowed zone (parking lot / multi-use trail) and make the most of that. Don't argue with the policy or hedge — Scout knows the local rules cold and respects them.
- AMENITIES: answer facility questions (restrooms, showers, parking, ADA access, etc.) only from the AMENITIES data above. Never invent or assume a facility exists. If it's not listed, say you don't have that on file.
- ADVISORY lines (rip current, high surf, water quality, closures, NWS alerts, etc.) are real active hazards for that date. Mention them proactively and plainly, the same honest way you'd name bad sand temp or bacteria risk — don't wait to be asked. moderate/severe/extreme severity should shape your recommendation (e.g. don't suggest swimming or off-leash water play through a rip current or high surf advisory); minor severity is lower-priority but still worth a mention if relevant to the question.
- FETCH AND RETRIEVE ARE OFF-LEASH ACTIVITIES. You cannot throw a ball for a leashed dog — the leash physically prevents it. Never suggest fetch, "leashed fetch", chase-the-ball, frisbee, retrieve, or any throw-and-chase game when the dog must be leashed. Swimming is similarly off-leash unless the dog is in shallow shore-break under direct restraint. Leashed-dog activities are: walks, sniff-tours, tide-pool exploring, sit-with-you-on-the-towel, wade in ankle-deep water with a long lead.
- NEVER recommend other apps, websites, or services for beach or dog info. This includes (but is not limited to) BringFido, Yelp, TripAdvisor, AllTrails, Google reviews, county/city parks websites, state park sites, surf forecast sites (Surfline, Magicseaweed), or "the park's official site." If the user asks about a beach you don't have data for on this screen, tell them to use the app's find or compare view to see other beaches Scout knows about. Stay in-house — Scout is the source. If something genuinely isn't in your data (e.g. event schedules, current closure notices), say so plainly and stop there — do NOT direct them off-app.`;
}

function trim(s: string, n: number): string {
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}

// Build a compact human-readable section summary from zone_rules JSON.
// Handles both v1 (regions[]) and v2 (seasons[].regions[]) shapes.
function buildZoneSummary(zr: Record<string, unknown> | null): string {
  if (!zr) return "";
  const seasons = Array.isArray(zr.seasons) && (zr.seasons as unknown[]).length
    ? (zr.seasons as Record<string, unknown>[])
    : (Array.isArray(zr.regions)
       ? [{ name: null, regions: zr.regions } as Record<string, unknown>]
       : []);
  if (!seasons.length) return "";
  const lines: string[] = [];
  for (const s of seasons) {
    const seasonName = (s.name as string) || "All year";
    const regions = (Array.isArray(s.regions) ? s.regions : []) as Record<string, unknown>[];
    for (const r of regions) {
      const sections = (r.sections as Record<string, Record<string, unknown>>) || {};
      const entries = Object.entries(sections);
      if (!entries.length) continue;
      const zoneName = (r.name as string) || "Whole beach";
      const sectionParts: string[] = [];
      for (const [sName, sec] of entries) {
        const rule = (sec?.rule as string) || "unknown";
        const tw = sec?.time_windows as Array<Record<string, unknown>> | undefined;
        if (Array.isArray(tw) && tw.length) {
          const winText = tw.map(w =>
            `${w.start}-${w.end}=${w.rule || rule}`).join(", ");
          sectionParts.push(`${sName}: ${rule} (time: ${winText})`);
        } else {
          sectionParts.push(`${sName}: ${rule}`);
        }
      }
      const seasonSuffix = (seasons.length > 1) ? ` [${seasonName}]` : "";
      lines.push(`  ${zoneName}${seasonSuffix}: ${sectionParts.join("; ")}`);
    }
  }
  return lines.join("\n");
}

// Returns true if ANY of the listed sections is explicitly not_allowed
// in any region of zone_rules. Used to detect "trails-only" beaches
// where the binary has_on_leash is true but sand/water are prohibited.
function sectionsAreNotAllowed(
  zr: Record<string, unknown> | null,
  sectionNames: string[],
): boolean {
  if (!zr) return false;
  const seasons = Array.isArray(zr.seasons) && (zr.seasons as unknown[]).length
    ? (zr.seasons as Record<string, unknown>[])
    : (Array.isArray(zr.regions)
       ? [{ regions: zr.regions } as Record<string, unknown>]
       : []);
  for (const s of seasons) {
    const regions = (Array.isArray(s.regions) ? s.regions : []) as Record<string, unknown>[];
    for (const r of regions) {
      const sections = (r.sections as Record<string, Record<string, unknown>>) || {};
      for (const name of sectionNames) {
        if (sections[name]?.rule === "not_allowed") return true;
      }
    }
  }
  return false;
}

// ─── Comparative question detection ──────────────────────────────────────────

function isComparativeQuestion(question: string): boolean {
  const q = question.toLowerCase();
  return /\b(which beach|what beach|best beach|other beach|all beach|compare|versus|vs\.?|least crowded|most crowded|quietest|busiest|better beach|anywhere else|other option|other spot)\b/.test(q);
}

// ─── Cross-beach prompt builder ───────────────────────────────────────────────

function buildCrossBeachPrompt(
  beaches: Record<string, unknown>[],
  allDays: Record<string, unknown>[],
): string {
  const daysByBeach = new Map<string, Record<string, unknown>[]>();
  for (const d of allDays) {
    const loc = d.location_id as string;
    const arr = daysByBeach.get(loc) ?? [];
    arr.push(d);
    daysByBeach.set(loc, arr);
  }

  const beachContext = beaches.map((b) => {
    const days = daysByBeach.get(b.location_id as string) ?? [];
    // deno-lint-ignore no-explicit-any
    const beachAdvisories = (Array.isArray(b.advisories) ? b.advisories : []) as any[];
    const dayLines = days.map((d) => {
      const risks = Array.isArray(d.risk_reason_codes) ? (d.risk_reason_codes as string[]).join(", ") : "";
      const bacteria = d.bacteria_risk && d.bacteria_risk !== "none"
        ? ` bacteria_risk=${d.bacteria_risk}(${d.precip_72h_mm ?? 0}mm rain/72h)`
        : "";
      const date = d.local_date as string;
      const dayAdvisories = beachAdvisories.filter((a) => {
        const df = String(a.valid_from).slice(0, 10);
        const dt = String(a.valid_to).slice(0, 10);
        return date >= df && date <= dt;
      });
      const advisoryText = dayAdvisories.length
        ? ` advisories=[${dayAdvisories.map((a) =>
            `${a.label || a.event_type}:${a.severity}${a.dog_impact_text ? ` (${trim(a.dog_impact_text as string, 120)})` : ""}`
          ).join("; ")}]`
        : "";
      return `    ${d.local_date}: window=${d.best_window_label ?? "none"} weather=${d.summary_weather ?? "?"} wind=${fmtNum(d.avg_wind, "mph")} temp=${fmtNum(d.avg_temp, "°F")} crowds=${d.busyness_category ?? "?"} hours=${d.go_hours_count ?? 0}go/${d.caution_hours_count ?? 0}caution/${d.no_go_hours_count ?? 0}nogo${risks ? ` risks=${risks}` : ""}${d.caution_text ? ` caution="${d.caution_text}"` : ""}${bacteria}${advisoryText}`;
    }).join("\n");
    const zoneSummary = (b.zoneSummary as string) || "";
    const flatPolicyLine = (b.flatPolicyLine as string) || "";
    const policyBlock = zoneSummary
      ? `  DOG POLICY (zones):\n${zoneSummary}`
      : flatPolicyLine
      ? `  DOG POLICY: ${flatPolicyLine}`
      : `  DOG POLICY: unknown`;
    const amenitiesLine = (b.amenitiesLine as string) || "";
    const amenitiesBlock = `  AMENITIES: ${amenitiesLine || "unknown"}`;
    return `\n${b.display_name} (${b.location_id}):\n${policyBlock}\n${amenitiesBlock}\n${dayLines || "    (no forecast data)"}`;
  }).join("\n");

  return `You are Scout — a local surfer who knows every dog beach on the West Coast. You've scouted all of them and know their differences — which ones get crowded on weekends, which have the best low tides, which get blown out in the afternoon. Casual surfer tone, first-person, no fluff.

ALL BEACHES — DOG POLICY + AMENITIES + 7-DAY SUMMARY:
${beachContext}

Rules:
- Answer cross-beach comparison questions using the data above
- DOG POLICY (zones) is the authoritative source for what the dog can do and where at each beach. Sections (sand, water_swim, trails, parking_lot, picnic_area, restrooms, showers, playground, dunes, tide_pools, boardwalk, bluff, campground) are marked off_leash / on_leash / not_allowed. Never suggest an activity in a not_allowed section, and never suggest off-leash play where a section is on_leash. If a section has a time window, the rule changes by time of day — apply the window matching the user's intended visit hour.
- If DOG POLICY is "unknown" for a beach, say plainly you don't have policy data for it rather than guessing.
- AMENITIES: answer facility questions (restrooms, showers, parking, ADA access, etc.) only from the AMENITIES line for that beach. Never invent a facility that isn't listed there — if it's "unknown", say you don't have that on file for that beach.
- CAUTIONS: risks=, caution=, and bacteria_risk= in a day's line are real hazards flagged for that beach/day — surface them plainly when relevant (e.g. don't recommend swimming on a day with bacteria_risk=high or bacteria_risk=moderate) rather than only citing them if asked.
- ADVISORIES: an advisories=[...] entry on a day's line (rip current, high surf, water quality, closures, NWS alerts, etc.) is a real active hazard for that beach on that date. Mention it proactively and plainly, the same honest way you'd name bad sand temp — don't wait to be asked. moderate/severe/extreme severity should shape your recommendation away from swimming or off-leash water play that day; minor is lower-priority but still worth a mention if relevant.
- Recommend specific beaches and days with reasons — be direct
- Use descriptive language for conditions, not raw numbers where possible
- Keep answers to 2-3 sentences
- Lead with the direct answer, no preamble
- No emojis, no markdown, plain text only
- Never mention numeric scores unless asked
- Crowd terms: quiet = few people, moderate = getting busy, dog_party = packed with dogs, too_crowded = avoid
- LIFEGUARDS ARE SEASONAL — never say "lifeguards on duty" or "lifeguards on staff". Say "seasonal lifeguards" or just "lifeguards".
- NEVER recommend other apps, websites, or services (BringFido, Yelp, TripAdvisor, AllTrails, Surfline, county/city/state park sites, etc.). Scout is the source. If something isn't in the data above, say so plainly and stop there.`;
}

// ─── Anthropic call ───────────────────────────────────────────────────────────

async function callAnthropic(
  systemPrompt: string,
  history: ConversationTurn[],
  question: string,
): Promise<string> {
  const messages = [
    ...history.map((t) => ({ role: t.role, content: t.content })),
    { role: "user", content: question },
  ];

  const res = await fetch(ANTHROPIC_API_URL, {
    method: "POST",
    headers: {
      "Content-Type":      "application/json",
      "x-api-key":         ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model:      MODEL,
      max_tokens: MAX_TOKENS,
      system:     systemPrompt,
      messages,
    }),
  });

  if (!res.ok) {
    throw new Error(`Anthropic API error ${res.status}: ${await res.text()}`);
  }

  const data = await res.json();
  const text = (data.content ?? [])
    .map((b: { type: string; text?: string }) => b.type === "text" ? b.text ?? "" : "")
    .filter(Boolean)
    .join("\n");

  if (!text) throw new Error("Anthropic returned empty response");
  return text;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function fmtNum(val: unknown, unit: string): string {
  if (val === null || val === undefined) return "n/a";
  const n = typeof val === "number" ? val : parseFloat(String(val));
  return isNaN(n) ? "n/a" : `${n}${unit}`;
}

function localDateForTimezone(date: Date, timezone: string): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric", month: "2-digit", day: "2-digit",
  }).formatToParts(date);
  const get = (t: string) => parts.find(p => p.type === t)?.value ?? "";
  return `${get("year")}-${get("month")}-${get("day")}`;
}

