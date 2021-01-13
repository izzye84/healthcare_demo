with

source as (
    select *
    from {{ source('conviva_clinical', 'diagnosis') }}
    {{ limit_dev_data() }}
),

coalesce_timestamps_add_rownum as (
    select
        row_number() over(partition by patient_id,diagnosis_code,clinical_encounter_id order by ingest_date desc) as row_num
        ,patient_id
        ,patient_account_number
        ,clinical_encounter_id
        ,diagnosis_date
        ,diagnosis_code
        ,onset_date
        ,resolved_date
        ,code_set
        ,diagnosis_name
        ,service_date
        ,coalesce(modified_timestamp,created_timestamp) as coalesced_timestamp
        ,client_id
        ,ingest_date
    from source
),

renamed as (
    select 
        {{ dbt_utils.surrogate_key(['patient_id','diagnosis_code','clinical_encounter_id']) }} as identifier_condition
        ,{{ dbt_utils.surrogate_key(['patient_id']) }} as identifier_external_source
        ,{{ empty_string_to_null('clinical_encounter_id') }} as encounter
        ,'clinical' as category_code
        ,{{ empty_string_to_null('diagnosis_code') }} as code
        ,{{ empty_string_to_null('code_set') }} as code_system
        ,{{ empty_string_to_null('diagnosis_name') }} as code_display
        ,date({{ empty_string_to_null('diagnosis_date') }}) as recorded_date
        ,date({{ empty_string_to_null('onset_date') }}) as onset_datetime
        ,date({{ empty_string_to_null('resolved_date') }}) as abatement_datetime
        ,{{ empty_string_to_null('client_id')  }} as client_id
        ,ingest_date
    from coalesce_timestamps_add_rownum
    where row_num = 1
)

 select * from renamed