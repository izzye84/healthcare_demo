with member as {{
    dbt_utils.union_relations(
        relations=[
            ref('stg_humana_common__referral')
        ]
    )
}}


select dbt_shu_id
, member_id
, patient_id
, eligibility_id
, social_security_number
, first_name
, last_name
, date_of_birth
, gender
, race
, relationship
, address_line_1
, address_line_2
, city
, state
, zip_code
, zip_code_last_4
, mailing_country
, phone_number
, email_address
, primary_care_provider_npi
, death_date
, patient_account_number
, ingest_timestamp_utc
, created_timestamp
, modified_timestamp
from member
