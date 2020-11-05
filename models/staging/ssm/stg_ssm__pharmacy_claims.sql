with pharmacy_claims as (

    select *
    from {{ ref('base_ssm__pharmacy_claims') }}

),

final as (

    select identifier_claim_header,
           'pharmacy' as type,
           sub_type,
           patient,
           billable_period AS billable_period_start,
           billable_period AS billable_period_end,
           insurer,
           provider,
           facility,
           total,
           client_id,
           ingest_date

    from pharmacy_claims

)

select *
from final
