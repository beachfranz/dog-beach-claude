-- 20260508_pad_us_agency_code_aliases.sql
--
-- PAD-US uses 4-letter agency codes (FWS, NPS, USACE, DOD, BOEM, USBR,
-- USCG, BLM, USFS, SPR) in pad_us_units.mng_name, while operators table
-- uses full names (e.g. "United States Fish and Wildlife Service").
--
-- The pad_us_mng_name array seed (canonical_name only) misses these
-- code variants, so populate_from_pad_us_gold's operator_id lookup
-- returned NULL for ~99% of CA pad_us BEP rows.
--
-- This migration appends the 4-letter codes to the pad_us_mng_name array
-- for the federal/state agencies that use them. Future emit calls will
-- resolve operator_id correctly.
--
-- Mappings derived from PAD-US v3 source-data conventions:
--   FWS   = US Fish and Wildlife Service
--   NPS   = US National Park Service
--   USACE = US Army Corps of Engineers
--   USBR  = US Bureau of Reclamation
--   DOD   = Department of Defense
--   USCG  = US Coast Guard
--   BLM   = US Bureau of Land Management
--   USFS  = US Forest Service
--   SPR   = State Parks and Recreation (CA: California Department of Parks and Recreation)

begin;

update public.operators
   set pad_us_mng_name = array_append(coalesce(pad_us_mng_name, '{}'::text[]), code)
  from (values
    ('FWS',   1681),
    ('NPS',   1683),
    ('USACE', 1676),
    ('USBR',  1678),
    ('DOD',   1680),
    ('USCG',  1679),
    ('BLM',   1677),
    ('USFS',  1682),
    ('SPR',   551)
  ) as map(code, op_id)
 where operators.id = map.op_id
   and not (code = any(coalesce(operators.pad_us_mng_name, '{}'::text[])));

-- Note: NRCS, BOEM, OTHS, OTHF, RWD, JNT, UNK, TRIB, SDC, SDNR are also
-- in PAD-US's CA data but don't have corresponding agency-level operators
-- in our table. Add as we onboard those operators.

commit;
