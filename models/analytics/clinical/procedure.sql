with

ssm_clinical_procedures as (
    select * from {{ ref('stg_ssm__clinical_procedure') }}
)

select * from ssm_clinical_procedures