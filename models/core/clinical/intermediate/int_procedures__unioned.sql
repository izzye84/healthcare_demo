{{ config(dist = 'identifier_external_source') }}

with procedures_unioned as (

    select *
    from {{ ref('stg_ssm__clinical_procedure') }}

    union all

    select *
    from {{ ref('stg_humana__claim_procedures') }}

    union all

    select *
    from {{ ref('stg_ssm__claim_procedures') }}

)

select *
from procedures_unioned
