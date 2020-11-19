with conditions_unioned as (

    select *
    from {{ ref('stg_ssm__clinical_diagnoses') }}

    union all

    select *
    from {{ ref('stg_humana__claim_diagnoses') }}

)

select *
from conditions_unioned
