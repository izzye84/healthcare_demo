with base_claim_detail as (

    select *
    from {{ ref('base_ssm__claim_detail') }}

),

base_claim_header as (

    select *
    from {{ ref('base_ssm__claim_header') }}

),

add_missing_fields as (

    select {{ dbt_utils.surrogate_key(['patient',
                                       'identifier_claim_header',
                                       'identifier_claim_line']) }} as identifier_encounter,
           base_claim_detail.identifier_claim_header as identifier_external_encounter,
           null as status,
           'claim' as type,
           product_or_service as service_type_code,
           patient as subject,
           serviced_period_start as period,
           null as reason_code_text,
           performing_npi as participant_individual,
           client_id,
           ingest_date

    from base_claim_detail

),

final as (

    select add_missing_fields.identifier_encounter,
           add_missing_fields.identifier_external_encounter,
           base_claim_header.admission_source as hospitalization_admit_source,
           base_claim_header.discharge_status as hospitalization_discharge_disposition_code,
           add_missing_fields.status,
           add_missing_fields.type,
           add_missing_fields.service_type_code,
           add_missing_fields.subject,
           add_missing_fields.period,
           add_missing_fields.reason_code_text,
           add_missing_fields.participant_individual,
           base_claim_header.facility as service_provider,
           add_missing_fields.client_id,
           add_missing_fields.ingest_date

    from add_missing_fields

    left join base_claim_header
           on add_missing_fields.identifier_external_encounter = base_claim_header.identifier_claim_header

)

select *
from final
