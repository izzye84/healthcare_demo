with claim_line as (

    select *
    from {{ ref('base_conviva__facility_claim_line') }}

),

final as (

    select identifier_claim_header,
           identifier_claim_line,
           {{ dbt_utils.surrogate_key(['patient_id']) }} as identifer_external_source,
           revenue,
           product_or_service,
           modifier_1,
           modifier_2,
           serviced_period_start,
           serviced_period_end,
           location,
           quantity,
           case when quantity <> 0
                then net / quantity
                else 0
           end as unit_price,
           net,
           client_id,
           ingest_date

    from claim_line

)

select *
from final
