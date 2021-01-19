with claim_diagnosis as (

    select *
    from {{ ref('base_conviva__facility_claim_diagnosis') }}

),

claim_header as (

    select identifier_claim_header,
           billable_period_start
    from {{ ref('base_conviva__facility_claim_header') }}
),

add_date_of_service as (

    select claim_diagnosis.identifier_claim_header,
           claim_diagnosis.member_id,
           claim_diagnosis.patient_id,
           claim_diagnosis.code,
           claim_diagnosis.code_system,
           claim_diagnosis.client_id,
           claim_diagnosis.payer_lob,
           claim_diagnosis.ingest_date,
           claim_header.billable_period_start as serviced_period_start
    from claim_diagnosis
    left join claim_header
           on claim_diagnosis.identifier_claim_header = claim_header.identifier_claim_header
),


final as (

    select distinct {{ dbt_utils.surrogate_key(['identifier_claim_header',
                                                'patient_id',
                                                'code','serviced_period_start']) }} as identifier_condition,
                    {{ dbt_utils.surrogate_key(['patient_id']) }} as identifier_external_source,
                    identifier_claim_header as encounter,
                    serviced_period_start as recorded_date,
                    'claim' as category_code,
                    code,
                    code_system,
                    null::date as onset_datetime,
                    null::date as abatement_datetime,
                    client_id,
                    ingest_date

    from add_date_of_service
    where code is not null

)

SELECT *
FROM final
