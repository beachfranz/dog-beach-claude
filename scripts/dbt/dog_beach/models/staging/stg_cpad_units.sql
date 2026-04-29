-- Passthrough for public.cpad_units. Marts reference this via ref().

select * from {{ source('public', 'cpad_units') }}
