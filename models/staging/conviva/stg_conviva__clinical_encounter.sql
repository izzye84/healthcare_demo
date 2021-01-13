with

source as (
    select *
    from {{ source('conviva_clinical', 'clinical_encounter') }}
    {{ limit_dev_data () }}
),

add_row_num as (
    select 
        row_number() over(partition by patient_id, clinical_encounter_id order by ingest_date desc) as row_num
        ,patient_id
        ,patient_account_number
        ,clinical_encounter_date
        ,clinical_encounter_facility_name
        ,clinical_encounter_id
        ,clinical_encounter_reason
        ,clinical_encounter_begin_datetime
        ,clinical_encounter_status
        ,clinical_encounter_visit_type
        ,clinical_encounter_end_datetime
        ,provider_id
        ,provider_npi
        ,user_id
        ,place_of_service_code
        ,place_of_service_description
        ,coalesce(modified_timestamp,created_timestamp) as coalesced_timestamp
        ,client_id
        ,ingest_date
    from source

    /* Bad data in the source is causing Redshfit Sprctrum to not parse every row correctly */
    /* Adding this where statement for the time being until we can fix the parsing problem
        by converting csv to parquet */
    /* Once the conversion is done, this where clause can be removed */
    where regexp_count(patient_account_number, '^\\-?[0-9]\\d*(\\.\\d+)?$') > 0
),

renamed as (
    select
        {{ dbt_utils.surrogate_key(['patient_id']) }} as subject
        ,{{ empty_string_to_null('patient_account_number') }} as patient_account_number
        ,date(clinical_encounter_date) as period
        ,{{ empty_string_to_null('clinical_encounter_facility_name') }} as clinical_encounter_facility_name
        ,{{ empty_string_to_null('clinical_encounter_id') }} as identifier_external_encounter
        ,{{ empty_string_to_null('clinical_encounter_reason') }} as reason_code_text
        ,(clinical_encounter_date || ' ' || split_part(clinical_encounter_begin_datetime,'T',2))::timestamp as period_start
        ,{{ empty_string_to_null('clinical_encounter_status') }} as status
        ,{{ empty_string_to_null('clinical_encounter_visit_type') }} as service_type_code
        ,(clinical_encounter_date || ' ' || split_part(clinical_encounter_end_datetime,'T',2))::timestamp as period_end
        ,{{ empty_string_to_null('provider_id') }} as provider_id
        ,{{ empty_string_to_null('provider_npi') }} as participant_individual
        ,{{ empty_string_to_null('user_id') }} as user_id
        ,{{ empty_string_to_null('place_of_service_code') }} as place_of_service_code
        ,{{ empty_string_to_null('place_of_service_description') }} as place_of_service_description
        ,client_id
        ,ingest_date
    from add_row_num
    where row_num = 1
),

final as (
    select
        {{ dbt_utils.surrogate_key(['subject','identifier_external_encounter']) }} as identifier_encounter
        ,identifier_external_encounter
        ,status
        ,'clinical' as type
        ,service_type_code
        ,subject
        ,period
        ,reason_code_text
        ,participant_individual
        ,client_id
        ,ingest_date
    from renamed
)

select * from final