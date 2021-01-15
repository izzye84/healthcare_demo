with claim_procedure as (

    select *
    from {{ ref('base_conviva__facility_claim_procedure') }}

),

claim_line as (

    select identifier_claim_header,
           patient_id,
           product_or_service,
           serviced_period_start

    from {{ ref('base_conviva__facility_claim_line') }}

),

add_date_of_service as (

    select claim_procedure.identifier_claim_header,
           claim_procedure.member_id,
           claim_procedure.patient_id,
           claim_procedure.code,
           claim_procedure.procedure_modifier,
           claim_procedure.code_system,
           claim_procedure.procedure_code_rank,
           claim_procedure.client_id,
           claim_procedure.ingest_date,
           claim_line.serviced_period_start

    from claim_procedure
    left join claim_line
           on claim_procedure.identifier_claim_header = claim_line.identifier_claim_header and
              claim_procedure.patient_id = claim_line.patient_id and
              claim_procedure.code = claim_line.product_or_service

),

final as (

    select distinct {{ dbt_utils.surrogate_key(['code',
                                                'identifier_claim_header',
                                                'patient_id',
                                                'serviced_period_start']) }} as identifier_procedure,
                    {{ dbt_utils.surrogate_key(['patient_id']) }} as identifier_external_source,
                    identifier_claim_header as encounter,
                    'claim'::varchar as category_code,
                    code,
                    code_system,
                    serviced_period_start as performed_datetime,
                    client_id,
                    ingest_date

    from add_date_of_service

)

select *
from final
