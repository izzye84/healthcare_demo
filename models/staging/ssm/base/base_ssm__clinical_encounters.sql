{{ config(materialized = 'ephemeral') }}

with source as (

    select *
    from {{ source('ssm_clinical', 'encounter') }}
    {{ limit_dev_data() }}

),

add_row_num as (

    select row_number() over(partition by patient_account_number,
                                          clinical_encounter_id,
                                          clinical_encounter_reason
                             order by ingest_date desc,
                                      modified_timestamp desc) as row_num,
           *

    from source

),

source_renamed as (

    select {{ empty_string_to_null('clinical_encounter_facility_name') }} as clinical_encounter_facility_name,
           {{ empty_string_to_null('clinical_encounter_id') }} as identifier_external_encounter,
           {{ empty_string_to_null('provider_npi') }} as participant_individual,
           {{ empty_string_to_null('patient_account_number') }} as patient_account_number,
           {{ empty_string_to_null('patient_id') }} as patient_id,
           clinical_encounter_date as period,
           clinical_encounter_end_time as period_end,
           clinical_encounter_begin_time as period_start,
           {{ empty_string_to_null('place_of_service_code') }} as place_of_service_code,
           {{ empty_string_to_null('place_of_service_description') }} as place_of_service_description,
           {{ empty_string_to_null('provider_id') }} as provider_id,
           {{ empty_string_to_null('clinical_encounter_reason') }} as reason_code_text,
           {{ empty_string_to_null('clinical_encounter_visit_type') }} as service_type_code,
           {{ empty_string_to_null('clinical_encounter_status') }} as status,
           {{ empty_string_to_null('user_id') }} as user_id,
           created_timestamp,
           modified_timestamp,
           client_id,
           ingest_date

    from add_row_num
    where row_num = 1

)

select *
from source_renamed
