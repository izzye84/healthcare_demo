with

referral as (
    select * from {{ ref('stg_humana__referral') }}
),

patient_demographics as (
    select * from {{ ref('stg_humana__patient_demographics') }}
)

select
    referral.identifier_strive_id
    ,referral.identifier_external_subscriber_id
    ,referral.given_first_name
    ,referral.family_name
    ,referral.birth_date
    ,referral.gender
    ,patient_demographics.deceased_date
    ,referral.address_line1
    ,referral.address_line2
    ,referral.address_city
    ,referral.address_state
    ,referral.address_postal_code
    ,referral.address_district
    ,referral.telecom_home_phone_number
    ,referral.telecom_work_phone_number
    ,referral.telecom_email_address
    ,referral.general_practitioner
    ,referral.client_id
    ,referral.ingest_date
from referral left join patient_demographics
    on referral.identifier_strive_id = patient_demographics.identifier_strive_id