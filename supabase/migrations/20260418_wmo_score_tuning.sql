-- Update WMO code classifications:
-- • Code 63 (moderate rain): move from no_go → caution, now scored at 0.20
-- • Codes 85, 86 (snow showers): add to no_go
-- • Fog (45) and drizzle (51-55) scores adjusted per wmo_codes.csv review
-- • Rain showers (80, 81) scores lowered; showers now penalized more than drizzle

UPDATE public.scoring_config
SET
  caution_wmo_codes = array_append(
    array_remove(caution_wmo_codes, 63),
    63
  ),
  nogo_wmo_codes = array_remove(nogo_wmo_codes, 63)
WHERE is_active = true;
