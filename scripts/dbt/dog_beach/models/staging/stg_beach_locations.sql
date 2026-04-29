-- Passthrough for public.beach_locations. Marts reference this via ref().

select * from {{ source('public', 'beach_locations') }}
