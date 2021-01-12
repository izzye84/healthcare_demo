with

base_member as (
    select * from {{ ref('base_conviva__member') }}
),

base_patient as (
    select * from {{ ref('base_conviva__patient') }}
),

base_platform_shuid as (
    select * from {{ ref('stg_platform__shuid') }}
),

base_platform_admissibility as (
    select * from {{ ref('stg_platform__current_admissibility') }}
),

clean_member as (
    select
        identifier_external_source
        ,identifier_external_subscriber_id
        ,identifier_social_security_number
        ,name_given_first
        ,name_family
        ,birth_date
        
        ,case gender
            when 'male' then 'M'
            when 'female' then 'F'
            when 'unknown' then 'U'
            when 'other' then 'U'
            else 'Review Value: ' || gender
        end as gender

        ,case 
            when race is null or race in ('-'
                                        ,'declined to specify'
                                        ,'female'
                                        ,'no value'
                                        ,'unreported/refused to report') then 'Unknown'
            else race
        end as race
        
        ,address_line1
        ,address_line2
        ,address_city
        ,address_state
        ,address_postal_code
        ,address_country
        ,telecom_phone_number_home
        ,telecom_email_address
        ,general_practitioner
        ,client_id
        ,ingest_date
    from base_member
),

clean_patient as (
    select
        identifier_external_source
        ,identifier_social_security_number
        ,name_given_first
        ,name_family
        ,name_given_nickname
        ,name_prefix
        ,birth_date
        
        ,case gender
            when 'male' then 'M'
            when 'female' then 'F'
            when 'unknown' then 'U'
            when 'other' then 'U'
            else 'Review Value: ' || gender
        end as gender

        ,case 
            when race is null or race in ('-'
                                        ,'declined to specify'
                                        ,'female'
                                        ,'no value'
                                        ,'unreported/refused to report') then 'Unknown'
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
        ,telecom_phone_number_home
        ,telecom_email_address
        ,general_practitioner
        ,client_id
        ,ingest_date
    from base_patient
),

member_patient_joined as (
    select
        clean_member.identifier_external_source
        ,clean_member.identifier_external_subscriber_id
        ,base_platform_shuid.identifier_sh_uid
        ,base_platform_admissibility.active
        ,coalesce(clean_member.name_given_first,clean_patient.name_given_first) as name_given_first
        ,coalesce(clean_member.name_family,clean_patient.name_family) as name_family
        ,clean_patient.name_given_nickname
        ,clean_patient.name_prefix
        ,coalesce(clean_member.identifier_social_security_number,clean_patient.identifier_social_security_number) as identifier_social_security_number
        ,coalesce(clean_member.birth_date,clean_patient.birth_date) as birth_date
        ,coalesce(clean_member.gender,clean_patient.gender) as gender
        ,coalesce(clean_member.race,clean_patient.race) as race
        ,clean_patient.language_preferred
        ,clean_patient.language_secondary
        ,clean_patient.deceased_date
        ,clean_patient.birth_country
        ,coalesce(clean_member.address_line1,clean_patient.address_line1) as address_line1
        ,coalesce(clean_member.address_line2,clean_patient.address_line2) as address_line2
        ,coalesce(clean_member.address_city,clean_patient.address_city) as address_city
        ,coalesce(clean_member.address_state,clean_patient.address_state) as address_state
        ,coalesce(clean_member.address_postal_code,clean_patient.address_postal_code) as address_postal_code
        ,coalesce(clean_member.address_country,clean_patient.address_country) as address_country
        ,coalesce(clean_member.telecom_phone_number_home,clean_patient.telecom_phone_number_home) as telecom_phone_number_home
        ,coalesce(clean_member.telecom_email_address,clean_patient.telecom_email_address) as telecom_email_address
        ,coalesce(clean_member.general_practitioner,clean_patient.general_practitioner) as general_practitioner
        ,clean_member.client_id
        ,coalesce(clean_member.ingest_date,clean_patient.ingest_date) as ingest_date
    from clean_member 
        left join clean_patient on clean_member.identifier_external_source = clean_patient.identifier_external_source
        left join base_platform_shuid on clean_member.identifier_external_source = base_platform_shuid.identifier_external_source
        left join base_platform_admissibility on base_platform_shuid.identifier_sh_uid = base_platform_admissibility.identifier_sh_uid
)

select * from member_patient_joined