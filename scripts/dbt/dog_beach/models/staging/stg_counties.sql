-- Passthrough for public.counties. Marts reference this via ref().

select * from {{ source('public', 'counties') }}
