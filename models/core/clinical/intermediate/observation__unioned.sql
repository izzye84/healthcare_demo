with final as (

    select *
    from {{ ref('stg_ssm__lab_observation') }}

    union all

    select *
    from {{ ref('stg_ssm__vital_observation') }}

    union all

    select *
    from {{ ref('stg_humana__lab_observation') }}

)

select *
from final