with final as (

    select *
    from {{ ref('stg_ssm__lab_observation') }}

)

select *
from final