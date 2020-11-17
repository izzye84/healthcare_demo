{{
    config(
        materialized = 'ephemeral'
    )
}}

with

lab_source as (
    select * from {{ source('ssm_clinical','labs') }}
),

lab_add_rownum as (
    select row_number() over(partition by patient_account_number, clinical_encounter_id, lab_id, lab_code, result_description order by ingest_date desc, modified_timestamp desc) as row_num
        ,*
    from lab_source
),

lab_dedup as (
    select *
    from lab_add_rownum
    where row_num = 1
),

renamed as (
    select {{ empty_string_to_null('patient_id') }} as patient_id
        ,{{ empty_string_to_null('patient_account_number') }} as patient_account_number
        ,{{ empty_string_to_null('clinical_encounter_id') }} as identifier_encounter
        ,{{ empty_string_to_null('lab_id') }} as lab_id
        ,{{ empty_string_to_null('lab_code_set') }} as code_source_system
        ,{{ empty_string_to_null('lab_code') }} as code_source
        ,lab_order_date as order_date
        ,{{ empty_string_to_null('lab_order_description') }} as detail
        ,{{ empty_string_to_null('service_code') }} as reason_code
        ,{{ empty_string_to_null('service_code_modifier') }} as service_code_modifier
        ,{{ empty_string_to_null('service_code_type') }} as reason_system
        ,{{ empty_string_to_null('service_code_desc') }} as reason_display
        ,{{ empty_string_to_null('result_description') }} as code_source_display
        ,collection_date as collection_date
        ,result_date as effective_date
        ,{{ empty_string_to_null('result_text') }} as value_string
        ,{{ empty_string_to_null('result_numeric') }} as value_quantity
        ,{{ empty_string_to_null('result_pos_neg') }} as value_boolean
        ,{{ empty_string_to_null('unit') }} as value_quantity_unit
        ,{{ empty_string_to_null('reference_range') }} as reference_range_text
        ,{{ empty_string_to_null('abnormal_flag') }} as interpretation_code
        ,{{ empty_string_to_null('result_status') }} as status_code
        ,created_timestamp as created_timestamp
        ,modified_timestamp as modified_timestamp
        ,{{ empty_string_to_null('client_id') }} as client_id
        ,ingest_date as ingest_date
    from lab_dedup
)

select * from renamed
