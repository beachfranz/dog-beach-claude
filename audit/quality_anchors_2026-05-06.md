# Quality Anchors Audit — 2026-05-06

Total anchors: **23**

## Summary by Tier

| Tier | OK | MISSING | NAME_MISMATCH | NOT_SCOREABLE | NO_DOG_POLICY | DRIFT |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 4 | 0 | 0 | 0 | 0 | 1 |
| 2 | 4 | 0 | 0 | 1 | 0 | 4 |
| 3 | 7 | 0 | 0 | 2 | 0 | 0 |

## Tier 1 — Marquee (must-be-correct)

| Anchor | County | Status | fid | Matched | dogs / leash / off | Notes |
|---|---|---|---:|---|---|---|
| Huntington Dog Beach | Orange | ✅ | 9717 | Huntington Beach Dog Beach | yes / off_leash / True |  |
| Ocean Beach Dog Beach | San Diego | 🔴 DRIFT | 6238 | Ocean Beach Dog Beach | yes / mixed / True | leash_policy: expected='off_leash' actual='mixed' |
| Coronado Dog Beach | San Diego | ✅ | 6202 | Coronado Dog Beach | yes / off_leash / True |  |
| Fiesta Island | San Diego | ✅ | 9716 | Fiesta Island | yes / off_leash / True |  |
| Rosie's Dog Beach | Los Angeles | ✅ | 6411 | Rosie's Dog Beach | yes / off_leash / True |  |

## Tier 2 — Regional anchors

| Anchor | County | Status | fid | Matched | dogs / leash / off | Notes |
|---|---|---|---:|---|---|---|
| Del Mar Dog Beach | San Diego | 🔴 DRIFT | 8560 | Del Mar Dog Beach | yes / mixed / True | dogs_allowed: expected='mixed' actual='yes'; leash_policy: expected='on_leash' actual='mixed' |
| Arroyo Burro Beach | Santa Barbara | ✅ | 8779 | Arroyo Burro Beach | yes / off_leash / True |  |
| Cardiff State Beach | San Diego | ✅ | 8341 | Cardiff State Beach | yes / on_leash / False |  |
| Leo Carrillo State Beach | Los Angeles | ⚠️ NOT_SCOREABLE | 3671 | Leo Carrillo State Beach | no / mixed / False |  |
| Doheny State Beach | Orange | 🔴 DRIFT | 9718 | Doheny State Beach | no / on_leash / False | dogs_allowed: expected='mixed' actual='no' |
| San Onofre State Beach | San Diego | ✅ | 9719 | San Onofre State Beach | mixed / on_leash / False |  |
| Pacific Beach | San Diego | 🔴 DRIFT | 8354 | North Pacific Beach | yes / on_leash / False | dogs_allowed: expected='mixed' actual='yes' |
| Mission Beach | San Diego | 🔴 DRIFT | 8356 | Mission Beach | yes / on_leash / False | dogs_allowed: expected='mixed' actual='yes' |
| La Jolla Shores | San Diego | ✅ | 8347 | La Jolla Shores | mixed / on_leash / False |  |

## Tier 3 — Long-tail (coverage-only)

| Anchor | County | Status | fid | Matched | dogs / leash / off | Notes |
|---|---|---|---:|---|---|---|
| Tide Beach Park | San Diego | ✅ | 9721 | Tide Beach Park | yes / on_leash / — |  |
| Hollywood Beach | Ventura | ✅ | 8589 | Hollywood Beach | yes / on_leash / False |  |
| Silver Strand Beach | Ventura | ✅ | 8588 | Silver Strand State Beach | yes / on_leash / False |  |
| Sunset Cliffs | San Diego | ✅ | 8642 | Sunset Cliffs Beach | mixed / on_leash / False |  |
| Mandalay Beach | Ventura | ✅ | 8591 | Mandalay Beach | no / on_leash / False |  |
| Point Dume State Beach | Los Angeles | ⚠️ NOT_SCOREABLE | 9720 | Point Dume State Beach | no / on_leash / False |  |
| Little Dume Beach | Los Angeles | ⚠️ NOT_SCOREABLE | 9369 | Little Dume Beach | no / on_leash / False |  |
| West Ellwood Beach | Santa Barbara | ✅ | 8946 | Ellwood Beach | yes / on_leash / False |  |
| Jalama Beach | Santa Barbara | ✅ | 5199 | Jalama Beach | yes / on_leash / False |  |
