{{
    config(
        materialized = 'ephemeral'
    )
}}

with

vital_source as (
    select * from {{ source('ssm_clinical','vitals') }}
),

vital_add_rownum as (
    select row_number() over(partition by patient_account_number, clinical_encounter_id, vitals_name, vitals_value order by ingest_date desc, modified_timestamp desc) as row_num
        ,*
    from vital_source
),

vital_dedup as (
    select *
    from vital_add_rownum
    where row_num = 1
),

renamed as (
    select {{ empty_string_to_null('patient_id') }} as patient_id
        ,{{ empty_string_to_null('patient_account_number') }} as patient_account_number
        ,{{ empty_string_to_null('clinical_encounter_id') }} as identifier_encounter
        ,{{ empty_string_to_null('vitals_captured_by') }} as vitals_captured_by
        ,{{ empty_string_to_null('vitals_name') }} as code_source
        ,vitals_value as value_quantity
        ,{{ empty_string_to_null('vitals_unit') }} as value_quantity_unit
        ,created_timestamp as created_timestamp
        ,modified_timestamp as modified_timestamp
        ,{{ empty_string_to_null('client_id') }} as client_id
        ,ingest_date as ingest_date
    from vital_dedup
)

select * from renamed