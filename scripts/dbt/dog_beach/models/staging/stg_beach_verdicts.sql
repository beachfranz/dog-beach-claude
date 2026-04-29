-- Passthrough for public.beach_verdicts. Marts reference this via ref().

select * from {{ source('public', 'beach_verdicts') }}
