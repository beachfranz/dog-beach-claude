-- Passthrough for public.operator_dogs_policy. Marts reference this via ref().

select * from {{ source('public', 'operator_dogs_policy') }}
