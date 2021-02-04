with

referral as (
    select * from {{ ref('base_humana_common__referral') }}
)

select {{ dbt_utils.surrogate_key(['member_id']) }} as dbt_shu_id
    , member_id
    , NULL as patient_id
    , NULL as eligibility_id
    , NULL as social_security_number
    , first_name
    , last_name
    , date_of_birth
    , case when gender = 'F'
                then 'Female'

                when gender = 'M'
                then 'Male'

                else 'Other'
           end as gender
    , NULL as race
    , NULL as relationship
    , address_line_1
    , address_line_2
    , city
    , state
    , zip_code
    , zip_code_last_4
    , NULL as mailing_country
    , case when phone_number like '999999%'
                then NULL

                when phone_number = '0'
                then NULL

                else phone_number
        end as phone_number
    , case when regexp_count(email_address, '^[A-Za-z0-9\.\+_-]+@[A-Za-z0-9\._-]+\.[a-zA-Z]*$') > 0
          		then email_address
        		else null 
        end as email_address
    , primary_care_provider_npi
    , NULL as death_date
    , NULL as patient_account_number
    , ingest_timestamp_utc
    , NULL as created_timestamp
    , NULL as modified_timestamp
from referral