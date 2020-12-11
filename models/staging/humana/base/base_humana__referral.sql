{{
    config(
        materialized = 'ephemeral'
    )
}}

with 

source_referral as (
    select row_number() over(partition by mbr_pers_gen_key order by ingest_date desc) as row_num
        ,* 
    from {{ source('humana_src','referral') }}
    {{ limit_dev_data() }}
)

select {{ dbt_utils.surrogate_key(['mbr_pers_gen_key']) }} as identifier_external_source
    ,mbr_pers_gen_key as identifier_external_subscriber_id
    ,initcap({{ empty_string_to_null('pers_first_name') }}) as name_given_first
    ,initcap({{ empty_string_to_null('pers_last_name') }}) as name_family
    ,date(birth_date) as birth_date
    ,{{ empty_string_to_null('gender') }} as gender
    ,{{ empty_string_to_null('home_phone_nbr') }} as telecom_phone_number_home
    ,{{ empty_string_to_null('work_phone_nbr') }} as telecom_phone_number_work
    ,{{ empty_string_to_null('primary_email_addr') }} as telecom_email_address
    ,initcap({{ empty_string_to_null('addr_line1') }}) as address_line1
    ,initcap({{ empty_string_to_null('addr_line2') }}) as address_line2
    ,initcap({{ empty_string_to_null('city_name') }}) as address_city
    ,{{ empty_string_to_null('state_of_residence') }} as address_state
    ,{{ empty_string_to_null('zip_cd') }} as address_postal_code
    ,initcap({{ empty_string_to_null('county_of_residence') }}) as address_district
    ,{{ empty_string_to_null('pcp_npi') }} as general_practitioner
    ,{{ empty_string_to_null('client_id') }} as client_id
    ,ingest_date
from source_referral
where row_num = 1