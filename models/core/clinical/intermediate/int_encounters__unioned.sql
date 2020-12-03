{{ config(dist = 'subject') }}

with encounters_unioned as (

    select *
    from {{ ref('stg_ssm__clinical_encounters') }}

    union all

    select *
    from {{ ref('stg_ssm__claim_encounters') }}

    union all

    select *
    from {{ ref('stg_humana__claim_encounters') }}

)

select *
from encounters_unioned
