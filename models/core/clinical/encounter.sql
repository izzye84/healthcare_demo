{{ config(dist = 'subject') }}

with all_client_encounters as (

    select *
    from {{ ref('int_encounters__unioned') }}

)

select *
from all_client_encounters
