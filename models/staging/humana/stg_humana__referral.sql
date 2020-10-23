with

referral as (
    select * from {{ ref('base_humana__referral') }}
)

select identifier_external_source
    ,identifier_external_subscriber_id
    ,name_given_first
    ,name_family
    ,birth_date
    
    ,case
        when gender not in ('M','F','U') then 'Review Value: ' || gender
        else gender
    end as gender

    ,telecom_phone_number_home
    ,telecom_phone_number_work
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