with

base_member as (
    
    select *
    from {{ ref('base_ssm__member_eligibility') }}

)

select identifier_strive_id
    ,name_given_first
    ,name_given_middle
    ,name_family
    ,identifier_social_security_number
    ,birth_date
    ,identifier_external_subscriber_id

    ,case
        when gender not in ('M','F','U') then 'Review Value: ' || gender
        else gender
    end as gender

    ,case
        when (race = '0' or race is null) then 'Unknown'
        when race = '1' then 'White'
        when race = '2' then 'Black or African American'
        when race = '3' then 'Other Race'
        when race = '4' then 'Asian'
        when race = '5' then 'Hispanic'
        when race = '6' then 'American Indian or Alaska Native'
        else 'Review Value: ' || race
    end as race

    ,ethnic_group
    ,telecom_phone_number_home
    ,telecom_email_address
    ,address_line1
    ,address_line2
    ,address_city
    ,address_state
    ,address_postal_code
    ,general_practitioner
    ,client_id
    ,ingest_date
from base_member