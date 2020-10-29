{{
    config(
        dist = 'identifier_claim_header'
    )
}}

with claim_line as (

    select *
    from {{ ref('claim_line__union') }}

)

select *
from claim_line
