with claim_header as (

    select *
    from {{ ref('base_humana__medical_claims') }}

),

claim_header_total as (

    select identifier_claim_header,
           identifier_claim_line,
           'professional' as type,
           sub_type,
           patient,
           billable_period_start,
           billable_period_end,
           'humana' as insurer,
           npi_id as provider,
           npi_id as facility,
           sum(chrg_amt) over (partition by identifier_claim_header) as total,
           client_id,
           ingest_date

    from claim_header

),

final as (

  select identifier_claim_header,
         type,
         sub_type,
         patient,
         billable_period_start,
         billable_period_end,
         insurer,
         provider,
         facility,
         total,
         client_id,
         ingest_date

  from claim_header_total
  where identifier_claim_line = '1'

)

select *
from final
