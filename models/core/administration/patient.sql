{{
    config(
        dist = 'identifier_external_source'
    )
}}

with

source as (
    select
        identifier_external_source
        ,identifier_external_subscriber_id
        ,identifier_sh_uid
        ,active
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
        ,address_district
        ,address_country
        ,telecom_phone_number_home
        ,telecom_phone_number_work
        ,telecom_email_address
        ,general_practitioner
        ,client_id
        ,ingest_date
    from {{ ref('int_patient__unioned') }}
)

select * from source