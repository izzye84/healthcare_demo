{{ 
    config(
        materialized = 'ephemeral'
    )
}}

with

clinical_procedure_source as (
    select * from {{ source('ssm_clinical','procedure') }}
),

clinical_procedure_add_rownum as (
    select *
        ,row_number() over(partition by patient_account_number, clinical_encounter_id, procedure_code order by ingest_date desc, modified_timestamp desc) as row_num
    from clinical_procedure_source
),

clinical_procedure_dedup as (
    select *
    from clinical_procedure_add_rownum
    where row_num = 1
),

renamed as (
    select
        {{ empty_string_to_null('patient_id') }} as patient_id
        ,{{ empty_string_to_null('patient_account_number') }} as patient_account_number
        ,{{ empty_string_to_null('clinical_encounter_id') }} as encounter
        ,{{ empty_string_to_null('procedure_code') }} as code
        ,{{ empty_string_to_null('procedure_code_type') }} as code_system
        ,created_timestamp
        ,modified_timestamp
        ,{{ empty_string_to_null('client_id') }} as client_id
        ,ingest_date
    from clinical_procedure_dedup
)

select * from renamed