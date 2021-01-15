with source as (

    select *
    from {{ source('conviva_claims','facility_claim_line') }}
         {{ limit_dev_data() }}

),

add_row_num as (
    select row_number() over(partition by facility_header_claim_id,
                                          line_number
                             order by ingest_date desc) as row_num,
            *
    from source
),

source_renamed as (

    select facility_header_claim_id as identifier_claim_header,
           line_number as identifier_claim_line,
           allowed_amount,
           {{ empty_string_to_null('billing_provider_npi') }} as billing_provider_npi,
           charge_amount :: numeric(18,2) as net,
           member_id,
           member_liability,
           {{ empty_string_to_null('ndc_code') }} as ndc_code,
           patient_id,
           plan_payment,
           procedure_code as product_or_service,
           {{ empty_string_to_null('procedure_modifier_1') }} as modifier_1,
           {{ empty_string_to_null('procedure_modifier_2') }} as modifier_2,
           {{ empty_string_to_null('procedure_modifier_3') }} as modifier_3,
           {{ empty_string_to_null('procedure_modifier_4') }} as modifier_4,
           revenue_code as revenue,
           service_date_from as serviced_period_start,
           service_date_to as serviced_period_end,
           service_units :: numeric(18,2) as quantity,
           {{ empty_string_to_null('servicing_provider_npi') }} as location,
           standard_cost,
           total_paid,
           client_id,
           ingest_date

    from add_row_num
    where row_num = 1

)

select *
from source_renamed
