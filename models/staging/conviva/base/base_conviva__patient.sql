{{
  config(
    materialized = 'ephemeral'
    )
}}

with

source_patient as (
    select
        row_number() over(partition by patient_id order by ingest_date desc) as row_num
        ,patient_id
        ,patient_account_number
        ,social_security_number
        ,first_name
        ,last_name
        ,preferred_name
        ,salutation
        ,date_of_birth
        ,gender
        ,race
        ,language_spoken
        ,secondary_language_spoken
        ,death_date
        ,country_of_birth
        ,address_line_1
        ,address_line_2
        ,city
        ,state
        ,zip_code
        ,zip_code_last_4
        ,mailing_country
        ,address_type
        ,phone_number
        ,contact_type
        ,email_address
        ,pcp_provider_id
        ,pcp_npi
        ,created_timestamp
        ,modified_timestamp
        ,client_id
        ,ingest_date
    from {{ source('conviva_clinical', 'patient') }}
)

select
    {{ dbt_utils.surrogate_key(['patient_id']) }} as identifier_external_source
    ,{{ empty_string_to_null('patient_account_number') }} as patient_account_number
    ,{{ empty_string_to_null('social_security_number') }} as identifier_social_security_number
    ,initcap({{ empty_string_to_null('first_name') }}) as name_given_first
    ,initcap({{ empty_string_to_null('last_name') }}) as name_family
    ,initcap({{ empty_string_to_null('preferred_name') }}) as name_given_nickname
    ,initcap({{ empty_string_to_null('salutation') }}) as name_prefix
    ,date_of_birth::date as birth_date
    ,lower({{ empty_string_to_null('gender') }}) as gender
    ,{{ empty_string_to_null('race') }} as race
    ,{{ empty_string_to_null('language_spoken') }} as language_preferred
    ,{{ empty_string_to_null('secondary_language_spoken') }} as language_secondary
    ,date({{ empty_string_to_null('death_date') }}) as deceased_date
    ,{{ empty_string_to_null('country_of_birth') }} as birth_country
    ,initcap({{ empty_string_to_null('address_line_1') }}) as address_line1
    ,initcap({{ empty_string_to_null('address_line_2') }}) as address_line2
    ,initcap({{ empty_string_to_null('city') }}) as address_city
    ,{{ empty_string_to_null('state') }} as address_state
    ,{{ empty_string_to_null('zip_code') }} as address_postal_code
    ,{{ empty_string_to_null('zip_code_last_4') }} as address_postal_code_4
    ,initcap({{ empty_string_to_null('mailing_country') }}) as address_country
    ,{{ empty_string_to_null('address_type') }} as address_type
    ,{{ empty_string_to_null('phone_number') }} as telecom_phone_number_home
    ,{{ empty_string_to_null('contact_type') }} as telecom_phone_use
    ,{{ empty_string_to_null('email_address') }} as telecom_email_address
    ,{{ empty_string_to_null('pcp_provider_id') }} as practitioner_identifier
    ,{{ empty_string_to_null('pcp_npi') }} as general_practitioner
    ,created_timestamp
    ,modified_timestamp
    ,{{ empty_string_to_null('client_id') }} as client_id
    ,ingest_date
from source_patient
where row_num = 1