# Quality Anchors Audit — 2026-05-06

Total anchors: **23**

## Summary by Tier

| Tier | OK | MISSING | NAME_MISMATCH | NOT_SCOREABLE | NO_DOG_POLICY | DRIFT |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3 | 0 | 0 | 0 | 0 | 2 |
| 2 | 5 | 0 | 1 | 0 | 0 | 3 |
| 3 | 5 | 0 | 3 | 1 | 0 | 0 |

## Tier 1 — Marquee (must-be-correct)

| Anchor | County | Status | fid | Matched | dogs / leash / off | Notes |
|---|---|---|---:|---|---|---|
| Huntington Dog Beach | Orange | ✅ | 9717 | Huntington Beach Dog Beach | yes / off_leash / True |  |
| Ocean Beach Dog Beach | San Diego | 🔴 DRIFT | 6238 | Ocean Beach Dog Beach | yes / mixed / True | leash_policy: expected='off_leash' actual='mixed' |
| Coronado Dog Beach | San Diego | 🔴 DRIFT | 6202 | Coronado Dog Beach | mixed / on_leash / — | dogs_allowed: expected='yes' actual='mixed'; leash_policy: expected='off_leash' actual='on_leash' |
| Fiesta Island | San Diego | ✅ | 9716 | Fiesta Island | yes / off_leash / True |  |
| Rosie's Dog Beach | Los Angeles | ✅ | 6411 | Rosie's Dog Beach | yes / off_leash / True |  |

## Tier 2 — Regional anchors

| Anchor | County | Status | fid | Matched | dogs / leash / off | Notes |
|---|---|---|---:|---|---|---|
| Del Mar Dog Beach | San Diego | 🔴 DRIFT | 8560 | Del Mar Dog Beach | yes / mixed / True | dogs_allowed: expected='mixed' actual='yes'; leash_policy: expected='on_leash' actual='mixed' |
| Arroyo Burro Beach | Santa Barbara | ✅ | 8779 | Arroyo Burro Beach | yes / off_leash / True |  |
| Cardiff State Beach | San Diego | ✅ | 8341 | Cardiff State Beach | yes / on_leash / False |  |
| Leo Carrillo State Beach | Los Angeles | 🔴 DRIFT | 3671 | Leo Carrillo State Beach | no / mixed / False | dogs_allowed: expected='yes' actual='no'; leash_policy: expected='on_leash' actual='mixed' |
| Doheny State Beach | Orange | ✅ | 9718 | Doheny State Beach | mixed / on_leash / False |  |
| San Onofre State Beach | San Diego | ✅ | 9719 | San Onofre State Beach | mixed / on_leash / False |  |
| Pacific Beach | San Diego | ⚠️ NAME | 8354 | North Pacific Beach | yes / on_leash / False | sim=0.70 |
| Mission Beach | San Diego | 🔴 DRIFT | 8356 | Mission Beach | yes / on_leash / False | dogs_allowed: expected='mixed' actual='yes' |
| La Jolla Shores | San Diego | ✅ | 8347 | La Jolla Shores | mixed / on_leash / False |  |

## Tier 3 — Long-tail (coverage-only)

| Anchor | County | Status | fid | Matched | dogs / leash / off | Notes |
|---|---|---|---:|---|---|---|
| Tide Beach Park | San Diego | ✅ | 9721 | Tide Beach Park | yes / on_leash / — |  |
| Hollywood Beach | Ventura | ✅ | 8589 | Hollywood Beach | yes / on_leash / False |  |
| Silver Strand Beach | Ventura | ⚠️ NAME | 8588 | Silver Strand State Beach | yes / on_leash / False | sim=0.83 |
| Sunset Cliffs | San Diego | ⚠️ NAME | 8642 | Sunset Cliffs Beach | mixed / on_leash / False | sim=0.70 |
| Mandalay Beach | Ventura | ✅ | 8591 | Mandalay Beach | no / on_leash / False |  |
| Point Dume State Beach | Los Angeles | ⚠️ NOT_SCOREABLE | 9720 | Point Dume State Beach | no / on_leash / False |  |
| Little Dume Beach | Los Angeles | ✅ | 9369 | Little Dume Beach | no / on_leash / False |  |
| West Ellwood Beach | Santa Barbara | ⚠️ NAME | 8946 | Ellwood Beach | yes / on_leash / False | sim=0.74 |
| Jalama Beach | Santa Barbara | ✅ | 5199 | Jalama Beach | yes / on_leash / False |  |
