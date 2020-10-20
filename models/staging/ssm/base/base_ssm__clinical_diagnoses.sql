{{
    config(
        materialized = 'ephemeral'
    )
}}

with

clinical_diagnoses_source as (
    select * from {{ source('ssm_clinical','diagnosis') }}
),

clinical_diagnoses_add_rownum as (
    select row_number() over(partition by patient_account_number, clinical_encounter_id, diagnosis_code order by ingest_date desc, modified_timestamp desc) as row_num
        ,*
    from clinical_diagnoses_source
),

clinical_diagnoses_dedup as (
    select *
    from clinical_diagnoses_add_rownum
    where row_num = 1
),

renamed as (
    select 
        {{ empty_string_to_null('patient_id') }} as patient_id
        ,{{ empty_string_to_null('patient_account_number') }} as patient_account_number
        ,{{ empty_string_to_null('clinical_encounter_id') }} as encounter
        ,diagnosis_date as recorded_date
        ,{{ empty_string_to_null('diagnosis_code') }} as code
        ,onset_date as onset_datetime
        ,resolved_date as abatement_datetime
        ,{{ empty_string_to_null('code_set') }} as code_system
        ,service_date as service_date
        ,created_timestamp as created_timestamp
        ,modified_timestamp as modified_timestamp
        ,{{ empty_string_to_null('client_id') }} as client_id
        ,ingest_date as ingest_date
    from clinical_diagnoses_dedup
)

select * from renamed