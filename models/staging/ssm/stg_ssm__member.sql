with 

member as (
    select * from {{ source('ssm_claims','member_eligibility') }}
),

final as (
    select distinct
      {{ dbt_utils.surrogate_key(['patient_id','lob','insurance_name']) }} as identifier_strive_id
      ,first_name as given_first_name
      ,middle_name as given_middle_name
      ,last_name as family_name
      ,social_security as identifier_social_security_number
      ,dob::date as birth_date
      ,patient_id as identifier_external_subscriber_id
      ,gender as gender
      ,race
      ,ethnic_group
      ,primary_phone as telecom_home_phone_number
      ,primary_email as telecom_email_address
      ,address_line1
      ,address_line2
      ,city as address_city
      ,state as address_state
      ,zip as address_postal_code
      ,null as general_practitioner
      ,member.client_id
      ,member.ingest_date
    from member
)

select * from final