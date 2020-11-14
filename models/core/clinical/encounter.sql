{{ config(dist = 'subject') }}

with

ssm_clinical_encounters as (

    select *
    from {{ ref('stg_ssm__clinical_encounters') }}
)

select *
from ssm_clinical_encounters
