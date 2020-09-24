/* 
Use this model for:
  - Fields have been renamed and recast in a consistent way.¹
  - Data types, such as timezones, are consistent.
  - Light cleansing, such as replacing empty string with NULL values, has occurred.
  - If useful, flattening of objects might have occurred.
  - There is a primary key that is both unique and not null (and tested).

*/

with 

member as (
    select * from {{ source('ssm_claims','member_eligibility') }}
),

crosswalk as (
    select * from {{ source('ssm_claims', 'member_crosswalk') }}
),

final as (
    select
      first_name
      ,middle_name
      ,last_name
      ,social_security
      ,dob::date as date_of_birth
      ,patient_id as member_id
      ,crosswalk.enterprise_mrn
      ,relation
      ,gender
      ,race /* standardize race values here */
      ,ethnic_group /* standardize ethnicity values here */
      ,primary_phone
      ,primary_email
      ,address_line1
      ,address_line2
      ,city
      ,state
      ,zip as zip_code
      ,eff_date::date as coverage_start_date
      ,term_date::date as coverage_end_date
      ,member.lob
      ,member.contract
      ,member.insurance_name
      ,primary_care_prov_id
    from member left join crosswalk
      on member.patient_id = crosswalk.person_id
      and member.lob = crosswalk.lob
      and member.insurance_name = crosswalk.insurance_name
)

select * from final