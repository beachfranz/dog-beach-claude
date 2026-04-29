-- Passthrough for public.us_beach_points. Marts reference this via ref().

select * from {{ source('public', 'us_beach_points') }}
