{{
    config(
        materialized = 'ephemeral'
    )
}}

with 

source_member as (
    select row_number() over(partition by patient_id,lob,insurance_name order by ingest_date desc, eff_date desc) as row_num
        ,trim(patient_id) as patient_id
        ,trim(lob) as lob
        ,trim(insurance_name) as insurance_name
        ,first_name
        ,middle_name
        ,last_name
        ,social_security
        ,dob
        ,gender
        ,race
        ,ethnic_group
        ,primary_phone
        ,primary_email
        ,address_line1
        ,address_line2
        ,city
        ,state
        ,zip
        ,primary_care_prov_id
        ,client_id
        ,ingest_date                
    from {{ source('ssm_claims','member_eligibility') }}
)

select distinct
    {{ dbt_utils.surrogate_key(['patient_id','lob','insurance_name']) }} as identifier_strive_id
    ,initcap({{ empty_string_to_null('first_name') }}) as given_first_name
    ,initcap({{ empty_string_to_null('middle_name') }}) as given_middle_name
    ,initcap({{ empty_string_to_null('last_name') }}) as family_name
    ,{{ empty_string_to_null('social_security') }} as identifier_social_security_number
    ,dob as birth_date
    ,{{ empty_string_to_null('patient_id') }} as identifier_external_subscriber_id
    ,{{ empty_string_to_null('gender') }} as gender
    ,{{ empty_string_to_null('race') }} as race
    ,{{ empty_string_to_null('ethnic_group') }} as ethnic_group
    ,{{ empty_string_to_null('primary_phone') }} as telecom_home_phone_number
    ,{{ empty_string_to_null('primary_email') }} as telecom_email_address
    ,initcap({{ empty_string_to_null('address_line1') }}) as address_line1
    ,initcap({{ empty_string_to_null('address_line2') }}) as address_line2
    ,{{ empty_string_to_null('city') }} as address_city
    ,{{ empty_string_to_null('state') }} as address_state
    ,{{ empty_string_to_null('zip') }} as address_postal_code
    ,{{ empty_string_to_null('primary_care_prov_id') }} as general_practitioner
    ,{{ empty_string_to_null('client_id') }} as client_id
    ,ingest_date
from source_member
where row_num = 1