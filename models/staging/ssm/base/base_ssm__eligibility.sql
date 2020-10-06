{{
  config(
    enabled = false
  )
}}

with 

eligibility as (
    select trim(patient_id) as patient_id
      ,trim(lob) as lob
      ,trim(insurance_name) as insurance_name
      ,eff_date
      ,term_date
      ,patient_id
      ,relation
      ,lob
      ,insurance_name
      ,client_id
      ,ingest_date 
    from {{ source('ssm_claims','member_eligibility') }}
),

final as (
    select distinct
      {{ dbt_utils.surrogate_key(['patient_id','lob','insurance_name','eff_date']) }} as identifier
      ,{{ dbt_utils.surrogate_key(['patient_id','lob','insurance_name']) }} as identifier_strive_id
      ,eff_date as period_coverage_start
      ,term_date as period_coverage_end
      ,patient_id as subscriber_id
      ,relation as relationship
      ,lob as network
      ,insurance_name as payor
      ,client_id
      ,ingest_date
    from eligibility
)

select * from final