{{
    config(
        materialized = 'ephemeral'
    )
}}

with

source as ( 
    select * from {{ source('reference_data','npi') }} 
),

renamed as (
    select 
        {{ empty_string_to_null('npi') }} as identifier_npi
        ,{{ empty_string_to_null('entity_type_code') }} as entity_type_code
        ,{{ empty_string_to_null('replacement_npi') }} as replacement_npi
        ,{{ empty_string_to_null('provider_organization_name_legal_business_name') }} as provider_organization_name_business
        ,{{ empty_string_to_null('provider_last_name_legal_name') }} as name_family
        ,{{ empty_string_to_null('provider_first_name') }} as name_given_first
        ,{{ empty_string_to_null('provider_middle_name') }} as name_given_middle
        ,{{ empty_string_to_null('provider_name_prefix_text') }} as name_prefix
        ,{{ empty_string_to_null('provider_name_suffix_text') }} as name_suffix
        ,{{ empty_string_to_null('provider_credential_text') }} as provider_credential_text
        ,{{ empty_string_to_null('provider_first_line_business_mailing_address') }} as address_line1
        ,{{ empty_string_to_null('provider_second_line_business_mailing_address') }} as address_line2
        ,{{ empty_string_to_null('provider_business_mailing_address_city_name') }} as address_city
        ,{{ empty_string_to_null('provider_business_mailing_address_state_name') }} as address_state
        ,{{ empty_string_to_null('provider_business_mailing_address_postal_code') }} as address_postal_code
        ,{{ empty_string_to_null('provider_business_mailing_address_telephone_number') }} as telecom_phone_number_work
        ,{{ empty_string_to_null('provider_business_mailing_address_fax_number') }} as telecom_fax_number_work
        ,{{ empty_string_to_null('provider_first_line_business_practice_location_address') }} as provider_practice_address_line1
        ,{{ empty_string_to_null('provider_second_line_business_practice_location_address') }} as provider_practice_address_line2
        ,{{ empty_string_to_null('provider_business_practice_location_address_city_name') }} as provider_practice_address_city
        ,{{ empty_string_to_null('provider_business_practice_location_address_state_name') }} as provider_practice_address_state
        ,{{ empty_string_to_null('provider_business_practice_location_address_postal_code') }} as provider_practice_address_postal_code
        ,{{ empty_string_to_null('provider_business_practice_location_address_telephone_number') }} as provider_practice_telephone_number
        ,{{ empty_string_to_null('provider_business_practice_location_address_fax_number') }} as provider_practice_fax_number
        ,{{ empty_string_to_null('provider_gender_code') }} as gender
        ,{{ empty_string_to_null('healthcare_provider_taxonomy_code_1') }} as provider_taxonomy_code_1
        ,{{ empty_string_to_null('healthcare_provider_primary_taxonomy_switch_1') }} as provider_taxonomy_switch_1
        ,{{ empty_string_to_null('healthcare_provider_taxonomy_code_1') }} as provider_taxonomy_code_2
        ,{{ empty_string_to_null('healthcare_provider_primary_taxonomy_switch_1') }} as provider_taxonomy_switch_2
        ,{{ empty_string_to_null('healthcare_provider_taxonomy_code_1') }} as provider_taxonomy_code_3
        ,{{ empty_string_to_null('healthcare_provider_primary_taxonomy_switch_1') }} as provider_taxonomy_switch_3
        ,{{ empty_string_to_null('healthcare_provider_taxonomy_code_1') }} as provider_taxonomy_code_4
        ,{{ empty_string_to_null('healthcare_provider_primary_taxonomy_switch_1') }} as provider_taxonomy_switch_4
        ,{{ empty_string_to_null('healthcare_provider_taxonomy_code_1') }} as provider_taxonomy_code_5
        ,{{ empty_string_to_null('healthcare_provider_primary_taxonomy_switch_1') }} as provider_taxonomy_switch_5
        ,{{ empty_string_to_null('healthcare_provider_taxonomy_code_1') }} as provider_taxonomy_code_6
        ,{{ empty_string_to_null('healthcare_provider_primary_taxonomy_switch_1') }} as provider_taxonomy_switch_6
        ,{{ empty_string_to_null('healthcare_provider_taxonomy_code_1') }} as provider_taxonomy_code_7
        ,{{ empty_string_to_null('healthcare_provider_primary_taxonomy_switch_1') }} as provider_taxonomy_switch_7
        ,{{ empty_string_to_null('healthcare_provider_taxonomy_code_1') }} as provider_taxonomy_code_8
        ,{{ empty_string_to_null('healthcare_provider_primary_taxonomy_switch_1') }} as provider_taxonomy_switch_8
        ,{{ empty_string_to_null('healthcare_provider_taxonomy_code_1') }} as provider_taxonomy_code_9
        ,{{ empty_string_to_null('healthcare_provider_primary_taxonomy_switch_1') }} as provider_taxonomy_switch_9
        ,{{ empty_string_to_null('healthcare_provider_taxonomy_code_1') }} as provider_taxonomy_code_10
        ,{{ empty_string_to_null('healthcare_provider_primary_taxonomy_switch_1') }} as provider_taxonomy_switch_10
        ,{{ empty_string_to_null('healthcare_provider_taxonomy_code_1') }} as provider_taxonomy_code_11
        ,{{ empty_string_to_null('healthcare_provider_primary_taxonomy_switch_1') }} as provider_taxonomy_switch_11
        ,{{ empty_string_to_null('healthcare_provider_taxonomy_code_1') }} as provider_taxonomy_code_12
        ,{{ empty_string_to_null('healthcare_provider_primary_taxonomy_switch_1') }} as provider_taxonomy_switch_12
        ,{{ empty_string_to_null('healthcare_provider_taxonomy_code_1') }} as provider_taxonomy_code_13
        ,{{ empty_string_to_null('healthcare_provider_primary_taxonomy_switch_1') }} as provider_taxonomy_switch_13
        ,{{ empty_string_to_null('healthcare_provider_taxonomy_code_1') }} as provider_taxonomy_code_14
        ,{{ empty_string_to_null('healthcare_provider_primary_taxonomy_switch_1') }} as provider_taxonomy_switch_14
        ,{{ empty_string_to_null('healthcare_provider_taxonomy_code_1') }} as provider_taxonomy_code_15
        ,{{ empty_string_to_null('healthcare_provider_primary_taxonomy_switch_1') }} as provider_taxonomy_switch_15
        ,{{ empty_string_to_null('is_sole_proprietor') }} as is_sole_proprietor
        ,certification_date
    from source
)

select * from renamed