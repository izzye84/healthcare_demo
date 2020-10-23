with final as (

    select *
    from {{ ref('stg_humana__claim_header') }}

    union all

    select *
    from {{ ref('stg_ssm__claim_header') }}

)

select *
from final