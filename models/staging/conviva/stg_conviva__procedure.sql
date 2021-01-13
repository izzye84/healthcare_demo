with

source as (
    select *
    from {{ source('conviva_clinical', 'procedure') }}
),

coalesce_timestamps_add_rownum as (
    select
        row_number() over(partition by patient_id, procedure_code, clinical_encounter_id order by ingest_date desc) as row_num
        ,patient_id
        ,patient_account_number
        ,clinical_encounter_id
        ,procedure_code
        ,procedure_description
        ,procedure_code_type
        ,modifier1
        ,modifier2
        ,modifier3
        ,modifier4
        ,coalesce(modified_timestamp,created_timestamp) as coalesced_timestamp
        ,client_id
        ,ingest_date
    from source
),

renamed as (
        select
            {{ dbt_utils.surrogate_key(['patient_id','procedure_code','clinical_encounter_id']) }} as identifier
            ,{{ dbt_utils.surrogate_key(['patient_id']) }} as identifier_external_source
            ,{{ empty_string_to_null('clinical_encounter_id') }} as encounter
            ,'clinical' as category_code
            ,{{ empty_string_to_null('procedure_code') }} as code
            ,{{ empty_string_to_null('procedure_code_type') }} as code_system
            ,{{ empty_string_to_null('procedure_description') }} as code_display
            ,date(coalesced_timestamp) as performed_datetime
            ,{{ empty_string_to_null('client_id') }} as client_id
            ,ingest_date
        from coalesce_timestamps_add_rownum
        where row_num = 1
)

select * from renamed