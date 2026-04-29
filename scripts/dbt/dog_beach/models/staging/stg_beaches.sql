-- Passthrough for public.beaches (consumer-app metadata, 5 rows). Marts reference this via ref().

select * from {{ source('public', 'beaches') }}
