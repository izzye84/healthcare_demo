with claim_header as (

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
         --client_id,
         ingest_date
         
    from {{ ref('int_claim_header__union') }}

)

select *
from claim_header
