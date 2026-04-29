-- Passthrough for public.ccc_access_points. Marts reference this via ref().

select * from {{ source('public', 'ccc_access_points') }}
