with

referral as (
    select * from {{ ref('stg_humana__referral') }}
),

patient_demographics as (
    select * from {{ ref('stg_humana__patient_demographics') }}
),

platform_shuid as (
    select * from {{ ref('stg_platform__shuid') }}
)

select
    referral.identifier_strive_id
    ,referral.identifier_external_subscriber_id
    ,platform_shuid.identifier_sh_uid
    ,referral.name_given_first
    ,referral.name_family
    ,referral.birth_date
    ,referral.gender
    ,patient_demographics.deceased_date
    ,referral.address_line1
    ,referral.address_line2
    ,referral.address_city
    ,referral.address_state
    ,referral.address_postal_code
    ,referral.address_district
    ,referral.telecom_phone_number_home
    ,referral.telecom_phone_number_work
    ,referral.telecom_email_address
    ,referral.general_practitioner
    ,referral.client_id
    ,referral.ingest_date
from referral left join patient_demographics
    on referral.identifier_strive_id = patient_demographics.identifier_strive_id left join platform_shuid
    on referral.identifier_strive_id = platform_shuid.identifier_strive_id