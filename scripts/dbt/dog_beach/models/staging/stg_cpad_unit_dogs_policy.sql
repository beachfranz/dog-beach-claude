-- Passthrough for public.cpad_unit_dogs_policy. Marts reference this via ref().

select * from {{ source('public', 'cpad_unit_dogs_policy') }}
