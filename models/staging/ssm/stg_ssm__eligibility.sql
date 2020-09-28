with 

eligibility as (
    select * from {{ source('ssm_claims','member_eligibility') }}
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