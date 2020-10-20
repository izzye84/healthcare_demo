with

ssm_clinical_diagnoses as (
    select * from {{ ref('stg_ssm__clinical_diagnoses') }}
)

select * from ssm_clinical_diagnoses