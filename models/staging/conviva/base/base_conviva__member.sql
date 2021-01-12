{{
  config(
    materialized = 'ephemeral'
    )
}}

with

source_member as (
    select
        row_number() over(partition by patient_id order by ingest_date desc) as row_num
        ,patient_id
        ,social_security_number
        ,first_name
        ,last_name
        ,date_of_birth
        ,gender
        ,race
        ,relationship
        ,address_line_1
        ,address_line_2
        ,city
        ,state
        ,zip_code
        ,zip_code_last_4
        ,mailing_country
        ,phone_number
        ,email_address
        ,primary_care_provider_npi
        ,market
        ,primary_care_location
        ,client_id
        ,ingest_date
    from {{ source('conviva_claims', 'member') }}
    {{ limit_dev_data() }}
)

select distinct
    {{ dbt_utils.surrogate_key(['patient_id']) }} as identifier_external_source
    ,{{ empty_string_to_null('patient_id') }} as identifier_external_subscriber_id
    ,{{ empty_string_to_null('social_security_number') }} as identifier_social_security_number
    ,initcap({{ empty_string_to_null('first_name') }}) as name_given_first
    ,initcap({{ empty_string_to_null('last_name') }}) as name_family
    ,date_of_birth::date as birth_date
    ,lower({{ empty_string_to_null('gender') }}) as gender
    ,initcap({{ empty_string_to_null('race') }}) as race
    ,{{ empty_string_to_null('relationship') }} as relationship
    ,initcap({{ empty_string_to_null('address_line_1') }}) as address_line1
    ,initcap({{ empty_string_to_null('address_line_2') }}) as address_line2
    ,initcap({{ empty_string_to_null('city') }}) as address_city
    ,{{ empty_string_to_null('state') }} as address_state
    ,{{ empty_string_to_null('zip_code') }} as address_postal_code
    ,{{ empty_string_to_null('zip_code_last_4') }} as address_postal_code_4
    ,initcap({{ empty_string_to_null('mailing_country') }}) as address_country
    ,{{ empty_string_to_null('phone_number') }} as telecom_phone_number_home
    ,{{ empty_string_to_null('email_address') }} as telecom_email_address
    ,{{ empty_string_to_null('primary_care_provider_npi') }} as general_practitioner
    ,{{ empty_string_to_null('market') }} as market
    ,{{ empty_string_to_null('primary_care_location') }} as primary_care_location
    ,{{ empty_string_to_null('client_id') }} as client_id
    ,ingest_date
from source_member
where row_num = 1