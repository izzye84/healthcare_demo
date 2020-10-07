with

referral as (
    select * from {{ ref('base_humana__referral') }}
)

select identifier_strive_id
    ,identifier_external_subscriber_id
    ,given_first_name
    ,family_name
    ,birth_date
    
    ,case
        when gender not in ('M','F','U') then 'Review Value: ' || gender
        else gender
    end as gender

    ,telecom_home_phone_number
    ,telecom_work_phone_number
    ,telecom_email_address
    ,address_line1
    ,address_line2
    ,address_city
    ,address_state
    ,address_postal_code
    ,address_district
    ,general_practitioner
    ,client_id
    ,ingest_date
from referral