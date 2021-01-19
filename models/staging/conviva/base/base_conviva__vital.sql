{{
  config(
    materialized = 'ephemeral'
    )
}}

with

source as (
    select * 
    from {{ source('conviva_clinical', 'vitals') }}
    {{ limit_dev_data() }}
),

add_rownnum as (
    select
        row_number() over(partition by patient_id,clinical_encounter_id,vitals_name,created_timestamp order by ingest_date desc, modified_timestamp desc) as row_num
        ,*
    from source
),

renamed as (
    select
        {{ dbt_utils.surrogate_key(['patient_id']) }} as identifier_external_source
        ,{{ empty_string_to_null('patient_account_number') }} as patient_account_number
        ,{{ empty_string_to_null('clinical_encounter_id') }} as identifier_encounter
        ,{{ empty_string_to_null('vitals_captured_by') }} as vitals_captured_by
        ,{{ empty_string_to_null('vitals_name') }} as code_source
        ,{{ empty_string_to_null('vitals_value') }} as value_quantity
        ,{{ empty_string_to_null('vitals_unit') }} as value_quantity_unit
        ,{{ empty_string_to_null('client_id') }} as client_id
        ,date( {{ empty_string_to_null('created_timestamp') }}) as created_date
        ,ingest_date
    from add_rownnum
    where row_num = 1
)

select * from renamed