-- Passthrough for public.operators. Marts reference this via ref().

select * from {{ source('public', 'operators') }}
