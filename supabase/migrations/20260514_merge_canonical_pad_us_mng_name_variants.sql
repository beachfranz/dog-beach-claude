-- 20260514_merge_canonical_pad_us_mng_name_variants.sql
--
-- Companion to 20260514_operator_aliases_stopgap.sql.
--
-- The aliases stopgap set parent_operator_id on 14 dupe operators (OPRD,
-- ODFW, ODOT, ODSL variants) but the polygon_to_operator dispatcher still
-- looks up via pad_us_mng_name = ANY array containment against the
-- CANONICAL operator. If the canonical's array doesn't include the variant
-- strings the polygons carry, the dispatcher returns NULL operator_id
-- even with the parent chain consolidated.
--
-- This migration merges all variant pad_us_mng_name values + alias texts
-- INTO the canonical operator's pad_us_mng_name array, so the dispatcher
-- finds the canonical directly.
--
-- Idempotent: re-running is a no-op on already-merged rows (uses
-- array_agg(DISTINCT) so duplicate strings collapse).

begin;

update public.operators o
   set pad_us_mng_name = (
     select array_agg(distinct n)
       from (
         select unnest(o.pad_us_mng_name) as n
         union
         select unnest(child.pad_us_mng_name) as n
           from public.operators child
          where child.parent_operator_id = o.id
            and child.pad_us_mng_name is not null
         union
         select alias_text from public.operator_aliases where canonical_operator_id = o.id
         union
         select pad_us_mng_name_variant from public.operator_aliases
          where canonical_operator_id = o.id and pad_us_mng_name_variant is not null
       ) x
      where n is not null
   )
 where o.id in (1742, 1743, 1744, 1745);

commit;
