with claim_line as (

    select identifier_claim_header,
           identifier_claim_line,
           identifer_external_source,
           revenue,
           product_or_service,
           modifier_1,
           modifier_2,
           serviced_period_start,
           serviced_period_end,
           location,
           quantity,
           unit_price,
           net,
           client_id,
           ingest_date

    from {{ ref('int_claim_line__union') }}

)

select *
from claim_line
