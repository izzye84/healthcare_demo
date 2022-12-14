{{
    config(
        materialized = 'ephemeral'
    )
}}

with

clinical_diagnoses_source as (
    select * from {{ source('ssm_clinical','diagnosis') }} {{ limit_dev_data() }}
),

coalesce_timestamps as (
    select
        patient_id
        ,patient_account_number
        ,clinical_encounter_id
        ,diagnosis_date
        ,diagnosis_code
        ,onset_date
        ,resolved_date
        ,code_set
        ,service_date
        ,coalesce(modified_timestamp,created_timestamp) as coalesced_timestamp
        ,client_id
        ,ingest_date
    from clinical_diagnoses_source
),

clinical_diagnoses_add_rownum as (
    select row_number() over(partition by patient_account_number, clinical_encounter_id, diagnosis_code order by ingest_date desc, coalesced_timestamp desc) as row_num
        ,*
    from coalesce_timestamps
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
        ,{{ empty_string_to_null('client_id') }} as client_id
        ,ingest_date as ingest_date
    from clinical_diagnoses_add_rownum
    where row_num = 1
)

select * from renamed