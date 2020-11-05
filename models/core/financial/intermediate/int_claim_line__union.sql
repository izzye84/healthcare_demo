with final as (

    select *
    from {{ ref('stg_humana__claim_line') }}

    union all

    select *
    from {{ ref('stg_ssm__claim_line') }}

)

select *
from final
