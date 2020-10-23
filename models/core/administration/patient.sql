{{
    config(
        dist = 'identifier_external_source'
    )
}}

with

ssm_patient as (
    select * from {{ ref('member_patient__joined') }}
),

humana_patient as (
    select * from {{ ref('referral_demographics__joined') }}
)

select identifier_external_source
    ,identifier_external_subscriber_id
    ,identifier_sh_uid
    ,name_given_first
    ,name_given_middle
    ,name_family
    ,name_given_nickname
    ,name_prefix
    ,identifier_social_security_number
    ,birth_date
    ,gender
    ,race
    ,ethnic_group
    ,language_preferred
    ,language_secondary
    ,deceased_date
    ,birth_country
    ,address_line1
    ,address_line2
    ,address_city
    ,address_state
    ,address_postal_code
    ,null as address_district
    ,address_country
    ,telecom_phone_number_home
    ,null as telecom_work_phone_number
    ,telecom_email_address
    ,general_practitioner
    ,client_id
    ,ingest_date
from ssm_patient

union all

select identifier_external_source
    ,identifier_external_subscriber_id
    ,identifier_sh_uid
    ,name_given_first
    ,null as name_given_middle
    ,name_family
    ,null as name_given_nickname
    ,null as name_prefix
    ,null as identifier_social_security_number
    ,birth_date
    ,gender
    ,null as race
    ,null as ethnic_group
    ,null as language_preferred
    ,null as language_secondary
    ,deceased_date
    ,null as birth_country
    ,address_line1
    ,address_line2
    ,address_city
    ,address_state
    ,address_postal_code
    ,address_district
    ,null as address_country
    ,telecom_phone_number_home
    ,telecom_phone_number_work
    ,telecom_email_address
    ,general_practitioner
    ,client_id
    ,ingest_date
from humana_patient