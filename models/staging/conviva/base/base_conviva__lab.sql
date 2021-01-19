{{
  config(
    materialized = 'ephemeral'
    )
}}

with

source as (
    select *
    from {{ source('conviva_clinical', 'lab') }}
    {{ limit_dev_data() }}
),

add_rownnum as (
        select
            row_number() over(partition by 
                                patient_id
                                ,clinical_encounter_id
                                ,lab_id
                                ,lab_code
                                ,service_code
                                ,result_description
                                ,result_text
                                ,result_numeric
                              order by ingest_date desc, modified_timestamp desc, unit desc) as row_num
            ,*
        from source
),

renamed as (
  select
    {{ dbt_utils.surrogate_key(['patient_id']) }} as identifier_external_source
    ,{{ empty_string_to_null('patient_account_number') }} as patient_account_number
    ,{{ empty_string_to_null('clinical_encounter_id') }} as identifier_encounter
    ,{{ empty_string_to_null('lab_id') }} as lab_id
    ,{{ empty_string_to_null('lab_code_set') }} as code_source_system
    ,{{ empty_string_to_null('lab_code') }} as code_source
    ,{{ empty_string_to_null('lab_order_date') }}::date as order_date
    ,{{ empty_string_to_null('lab_order_description') }} as detail
    ,{{ empty_string_to_null('service_code') }} as reason_code
    ,{{ empty_string_to_null('service_code_modifier') }} as service_code_modifier
    ,{{ empty_string_to_null('service_code_type') }} as reason_system
    ,{{ empty_string_to_null('service_code_desc') }} as reason_display
    ,{{ empty_string_to_null('result_description') }} as code_source_display
    ,{{ empty_string_to_null('collection_date') }}::date as collection_date
    ,{{ empty_string_to_null('result_date') }}::date as effective_date
    ,{{ empty_string_to_null('result_text') }} as value_string
    ,{{ empty_string_to_null('result_numeric') }} as value_quantity
    ,{{ empty_string_to_null('result_pos_neg') }} as value_boolean
    ,{{ empty_string_to_null('unit') }} as value_quantity_unit
    ,{{ empty_string_to_null('reference_range') }} as reference_range_text
    ,{{ empty_string_to_null('abnormal_flag') }} as interpretation_code
    ,{{ empty_string_to_null('result_status') }} as status_code
    ,{{empty_string_to_null('client_id') }} as client_id
    ,ingest_date
  from add_rownnum
  where row_num = 1
)

select * from renamed