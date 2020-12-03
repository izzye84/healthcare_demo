with base_medical_claims as (

    select *
    from {{ ref('base_humana__medical_claims') }}

),

final as (

    select {{ dbt_utils.surrogate_key(['patient',
                                       'identifier_claim_header',
                                       'identifier_claim_line']) }} as identifier_encounter,
           identifier_claim_header as identifier_external_encounter,
           admit_src_cd as hospitalization_admit_source,
           dischg_stat_cd as hospitalization_discharge_disposition_code,
           null as status,
           'claim' as type,
           product_or_service as service_type_code,
           patient as subject,
           serviced_period_start as period,
           null as reason_code_text,
           npi_id as participant_individual,
           pot_cd as service_provider,
           client_id,
           ingest_date

    from base_medical_claims

)

select *
from final
