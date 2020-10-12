with 

member as (
    select * from {{ ref('stg_ssm__member') }}
),

patient as (
    select * from {{ ref('stg_ssm__patient') }}
),

platform_shuid as (
    select * from {{ ref('stg_platform__shuid') }}
)

select 
    member.identifier_strive_id
    ,member.identifier_external_subscriber_id
    ,platform_shuid.identifier_sh_uid
    ,coalesce(patient.given_first_name,member.given_first_name) as given_first_name
    ,member.given_middle_name
    ,coalesce(patient.family_name,member.family_name) as family_name
    ,patient.given_nickname
    ,patient.name_prefix
    ,coalesce(patient.identifier_social_security_number,member.identifier_social_security_number) as identifier_social_security_number
    ,coalesce(patient.birth_date,member.birth_date) as birth_date
    ,coalesce(patient.gender,member.gender) as gender
    ,coalesce(patient.race,member.race) as race
    ,member.ethnic_group
    ,patient.language_preferred
    ,patient.language_secondary
    ,patient.deceased_date
    ,patient.birth_country
    ,coalesce(patient.address_line1,member.address_line1) as address_line1
    ,coalesce(patient.address_line2,member.address_line2) as address_line2
    ,coalesce(patient.address_city,member.address_city) as address_city
    ,coalesce(patient.address_state,member.address_state) as address_state
    ,coalesce(patient.address_postal_code,member.address_postal_code) as address_postal_code
    ,patient.address_country
    ,coalesce(patient.telecom_home_phone_number,member.telecom_home_phone_number) as telecom_home_phone_number
    ,coalesce(patient.telecom_email_address,member.telecom_email_address) as telecom_email_address
    ,coalesce(patient.general_practitioner,member.general_practitioner) as general_practitioner
    ,member.client_id
    ,coalesce(patient.ingest_date,member.ingest_date) as ingest_date
from member left join patient
    on member.identifier_strive_id = patient.identifier_strive_id left join platform_shuid
    on member.identifier_strive_id = platform_shuid.identifier_strive_id
