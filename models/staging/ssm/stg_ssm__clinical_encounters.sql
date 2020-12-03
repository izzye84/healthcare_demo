with clinical_encounters_base as (

    select *
    from {{ ref('base_ssm__clinical_encounters') }}

),

crosswalk_base as (

    select *
    from {{ ref('base_ssm__crosswalk') }}

),

joined as (

    select clinical_encounters_base.identifier_external_encounter,
           clinical_encounters_base.status,
           clinical_encounters_base.service_type_code,
           clinical_encounters_base.period,
           clinical_encounters_base.reason_code_text,
           clinical_encounters_base.participant_individual,
           clinical_encounters_base.client_id,
           clinical_encounters_base.ingest_date,
           crosswalk_base.person_id,
           crosswalk_base.lob,
           crosswalk_base.insurance_name

    from clinical_encounters_base
    inner join crosswalk_base
            on clinical_encounters_base.patient_account_number = crosswalk_base.enterprise_mrn

),

list_agg as (

    select identifier_external_encounter,
           status,
           service_type_code,
           period,
           participant_individual,
           client_id,
           ingest_date,
           person_id,
           lob,
           insurance_name,
           listagg(reason_code_text, ' | ') within group (order by period) as reason_code_text

    from joined
    group by identifier_external_encounter,
             status,
             service_type_code,
             period,
             participant_individual,
             client_id,
             ingest_date,
             person_id,
             lob,
             insurance_name

),

final as (

    select {{ dbt_utils.surrogate_key(['person_id',
                                       'lob',
                                       'insurance_name',
                                       'identifier_external_encounter']) }} as identifier_encounter,
           identifier_external_encounter,
           null as hospitalization_admit_source,
           null as hospitalization_discharge_disposition_code,
           status,
           'clinical' as type,
           service_type_code,
           {{ dbt_utils.surrogate_key(['person_id',
                                       'lob',
                                       'insurance_name'])}} as subject,
           period,
           reason_code_text,
           participant_individual,
           null as service_provider,
           client_id,
           ingest_date

    from list_agg

)

select *
from final
