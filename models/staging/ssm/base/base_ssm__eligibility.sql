{{
    config(
        materialized = 'ephemeral'
    )
}}

with 

eligibility as (
    select row_number() over(partition by patient_id,lob,insurance_name order by ingest_date desc, eff_date desc) as row_num
      ,trim(patient_id) as patient_id
      ,trim(lob) as lob
      ,trim(insurance_name) as insurance_name
      ,eff_date
      ,term_date
      ,relation
      ,client_id
      ,ingest_date 
    from {{ source('ssm_claims','member_eligibility') }}
)

select distinct
  {{ dbt_utils.surrogate_key(['patient_id','lob','insurance_name','eff_date']) }} as identifier
  ,{{ dbt_utils.surrogate_key(['patient_id','lob','insurance_name']) }} as identifier_strive_id
  ,eff_date as period_coverage_start
  ,term_date as period_coverage_end
  ,{{ empty_string_to_null('patient_id') }} as subscriber_id
  ,{{ empty_string_to_null('relation') }} as relationship
  ,{{ empty_string_to_null('lob') }} as network
  ,{{ empty_string_to_null('insurance_name') }} as payor
  ,{{ empty_string_to_null('client_id') }} as client_id
  ,ingest_date
from eligibility
where row_num = 1