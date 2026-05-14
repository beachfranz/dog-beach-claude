-- 20260514_oh_state_seed_correct_citation.sql
--
-- Correct OH state_dogs_policy seed after deep-read of the actual OAC rule.
--
-- WRONG (earlier seed today, 20260514_oh_state_dogs_policy_seed.sql):
--   ordinance_ref = 'OAC 1501:31-23-04'  ← non-existent rule (1501:31-23
--   chapter is endangered species, not pets)
--   source_url    = .../rule-1501:31-23-04  ← 404
--   source_quote  = paraphrased ("prohibited from designated swimming areas")
--
-- CORRECT (per WebFetch of the actual rule, effective 2023-06-30):
--   ordinance_ref = 'OAC 1501:46-3-06'  ← the actual ODNR animal leash rule
--   source_url    = .../rule-1501:46-3-06
--   source_quote  = verbatim text of paragraph (B): "It is unlawful for any
--   person who is the owner or handler of any animal to refuse or fail to
--   keep such animal on a leash in hand and under control at all times
--   while such animal is within any area administered by the division.
--   Said leash is not to exceed six feet in length from hand to animal."
--
-- Encoding decisions:
--   * default_rule='mixed' — the statewide CODIFIED default is on-leash
--     (no statewide swim-beach prohibition in the rule itself), but
--     per-park signage may prohibit pets from beach areas. Mixed captures
--     "default permissive, park overrides may exist" reality. Matches MI.
--   * has_on_leash=true, has_off_leash=false. Off-leash exists ONLY in
--     chief-designated "dog exercise" areas at specific parks — those
--     are operator-level overrides, NOT statewide default. Same as MI.

begin;

delete from public.state_dogs_policy where state_code = 'OH';

insert into public.state_dogs_policy
  (state_code, state_name, dogs_allowed, default_rule,
   has_on_leash, has_off_leash,
   source_quote, source_url, ordinance_ref,
   scope_notes, confidence, scraped_at)
values
  ('OH', 'Ohio', 'yes', 'mixed',
   true, false,
   'It is unlawful for any person who is the owner or handler of any animal to refuse or fail to keep such animal on a leash in hand and under control at all times while such animal is within any area administered by the division. Said leash is not to exceed six feet in length from hand to animal.',
   'https://codes.ohio.gov/ohio-administrative-code/rule-1501:46-3-06',
   'OAC 1501:46-3-06',
   'Ohio Administrative Code 1501:46-3-06 (Animal Leash Requirement, eff. 2023-06-30) governs all ODNR-administered land. Statewide default = 6-ft leash, in hand, at all times. Two narrow exceptions captured at operator level, NOT statewide: (1) ODNR chief may designate "dog exercise" areas at specific parks where leash is not required; (2) hunting dogs in designated public hunting areas during lawful hunting season. The rule itself does NOT prohibit pets from swim beaches — per-park signage handles that, surfaces as operator-level rules. Geneva State Park and Mosquito Lake operate designated dog-beach zones; those are operator overrides.',
   0.55,
   now());

commit;
