{{
    config(
        dist = 'identifier_external_source'
    )
}}

with

ssm_clinical_procedures as (
    select * from {{ ref('int_procedures__unioned') }}
)

select * from ssm_clinical_procedures
