with claim_line as (

    select *
    from {{ ref('base_humana__medical_claims') }}

),

final as (

    select identifier_claim_header,
           identifier_claim_line,
           revenue,
           product_or_service,
           modifier_1,
           modifier_2,
           serviced_period_start,
           serviced_period_end,
           pot_cd as location,
           quantity,
           case when quantity <> 0
                then chrg_amt / quantity
                else 0
           end as unit_price,
           chrg_amt as net,
           client_id,
           ingest_date

    from claim_line

)

select *
from final
