with claim_line as (

    select *
    from {{ ref('base_ssm__claim_detail') }}

),

final as (

    select identifier_claim_header,
           identifier_claim_line,
           patient as identifier_external_source,
           revenue,
           product_or_service,
           null as modifier_1,
           null as modifier_2,
           serviced_period_start,
           serviced_period_end,
           location,
           quantity,
           null::numeric(18,2) as unit_price,
           net,
           client_id,
           ingest_date

    from claim_line

)

select *
from final
