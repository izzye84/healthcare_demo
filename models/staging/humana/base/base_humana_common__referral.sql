{{
    config(
        materialized = 'table'
    )
}}

with

source_referral as (
    select row_number() over(partition by mbr_pers_gen_key order by ingest_date desc) as row_num
        ,*
    from {{ source('humana_src','referral') }}
    {{ limit_dev_data() }}
)

select MBR_PERS_GEN_KEY as member_id
    ,initcap({{ empty_string_to_null('pers_first_name') }}) as first_name
    ,initcap({{ empty_string_to_null('pers_last_name') }}) as last_name
    ,date(birth_date) as date_of_birth
    ,{{ empty_string_to_null('gender') }} as gender
    ,initcap({{ empty_string_to_null('addr_line1') }}) as address_line_1
    ,initcap({{ empty_string_to_null('addr_line2') }}) as address_line_2
    ,initcap({{ empty_string_to_null('city_name') }}) as city
    ,{{ empty_string_to_null('state_of_residence') }} as state
    ,{{ empty_string_to_null('zip_cd') }} as zip_code
    ,{{ empty_string_to_null('zip_plus_cd') }} AS zip_code_last_4
    ,{{ empty_string_to_null('home_phone_nbr') }} as phone_number
    ,{{ empty_string_to_null('primary_email_addr') }} as email_address
    ,{{ empty_string_to_null('pcp_npi') }} as primary_care_provider_npi
    ,{{ empty_string_to_null('lob') }} as insurance_type
    ,ingest_date as ingest_timestamp_utc
from source_referral
where row_num = 1
