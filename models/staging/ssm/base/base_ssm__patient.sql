{{
    config(
        materialized = 'ephemeral'
    )
}}

with 

source_patient as (
    select * from {{ source('ssm_clinical','patient') }}
),

source_member_crosswalk as (
    select distinct 
        enterprise_mrn
        ,person_id
        ,lob
        ,insurance_name
    from {{ source ('ssm_claims','member_crosswalk') }}
),

joined as (
    select row_number() over(partition by person_id, lob, insurance_name order by ingest_date desc) as row_num
        ,*
    from source_patient join source_member_crosswalk
        on source_patient.patient_account_number = source_member_crosswalk.enterprise_mrn
)

select 
    {{ dbt_utils.surrogate_key(['person_id','lob','insurance_name']) }} as identifier_strive_id
    ,{{ empty_string_to_null('social_security_number') }} as identifier_social_security_number
    ,initcap({{ empty_string_to_null('first_Name') }}) as given_first_name
    ,initcap({{ empty_string_to_null('last_name') }}) as family_name
    ,initcap({{ empty_string_to_null('preferred_name') }}) given_nickname
    ,initcap({{ empty_string_to_null('salutation') }}) name_prefix
    ,date_of_birth as birth_date
    ,{{ empty_string_to_null('gender') }} as gender
    ,{{ empty_string_to_null('race') }} as race
    ,{{ empty_string_to_null('language_spoken') }} as language_preferred
    ,{{ empty_string_to_null('secondary_language_spoken') }} language_secondary
    ,death_date as deceased_date
    ,{{ empty_string_to_null('country_of_birth') }} as birth_country
    ,initcap({{ empty_string_to_null('address_line_1') }}) as address_line1
    ,initcap({{ empty_string_to_null('address_line_2') }}) as address_line2
    ,initcap({{ empty_string_to_null('city') }}) as address_city
    ,{{ empty_string_to_null('state') }} as address_state
    ,{{ empty_string_to_null('zip_code') }} as address_postal_code
    ,{{ empty_string_to_null('zip_code_last_4') }} as address_postal_code_4
    ,initcap({{ empty_string_to_null('mailing_country') }}) as address_country
    ,{{ empty_string_to_null('address_type') }} as address_type
    ,{{ empty_string_to_null('phone_number') }} as telecom_phone_number
    ,{{ empty_string_to_null('contact_type') }} as telecom_phone_use
    ,{{ empty_string_to_null('email_address') }} as telecom_email_address
    ,{{ empty_string_to_null('pcp_provider_id') }} as practitioner_identifier
    ,{{ empty_string_to_null('pcp_npi') }} as general_practitioner
    ,created_timestamp
    ,modified_timestamp
    ,{{ empty_string_to_null('client_id') }} as client_id
    ,ingest_date
from joined
where row_num = 1