with claim_header as (

    select *
    from {{ ref('base_conviva__facility_claim_header') }}

),

final as (

    select identifier_claim_header,
           'institutional' as type,
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

    from claim_header

)

select *
from final
