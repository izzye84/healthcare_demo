with

base_patient as (
    
    select *
    from {{ ref('base_ssm__patient') }}

)

select identifier_strive_id
    ,identifier_social_security_number
    ,given_first_name
    ,family_name
    ,given_nickname
    ,name_prefix
    ,birth_date
    
    ,case
        when gender = 'Male' then 'M'
        when gender = 'Female' then 'F'
        else 'Review'
     end as gender

    ,case
        when race = 'North American Native' then 'American Indian or Alaska Native'
        when race not in ('Unknown','Hispanic','Black or African American','White') then 'Review'
        else race
     end as race

    ,language_preferred
    ,language_secondary
    ,deceased_date
    ,birth_country
    ,address_line1
    ,address_line2
    ,address_city
    ,address_state
    ,address_postal_code
    ,address_country
    
    ,case
        when telecom_phone_use = 'Home' then telecom_phone_number
     end as telecom_home_phone_number
    
    ,telecom_email_address
    ,general_practitioner
    ,client_id
    ,ingest_date
from base_patient
