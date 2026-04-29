-- Passthrough for public.truth_external. Marts reference this via ref().

select * from {{ source('public', 'truth_external') }}
