with

clinical_diagnoses_base as (
    select * from {{ ref('base_ssm__clinical_diagnoses') }}
),

crosswalk_base as (
    select * from {{ ref('base_ssm__crosswalk') }}
),

joined as (
    select crosswalk_base.person_id
        ,crosswalk_base.lob
        ,crosswalk_base.insurance_name
        ,clinical_diagnoses_base.encounter
        ,clinical_diagnoses_base.recorded_date
        ,clinical_diagnoses_base.code
        ,clinical_diagnoses_base.onset_datetime
        ,clinical_diagnoses_base.abatement_datetime
        ,clinical_diagnoses_base.code_system
        ,clinical_diagnoses_base.client_id
        ,clinical_diagnoses_base.ingest_date
    from clinical_diagnoses_base inner join crosswalk_base
        on clinical_diagnoses_base.patient_account_number = crosswalk_base.enterprise_mrn
)

select {{ dbt_utils.surrogate_key(['person_id','lob','insurance_name','code','encounter']) }} as identifier
    ,{{ dbt_utils.surrogate_key(['person_id','lob','insurance_name'])}} as identifier_external_source
    ,encounter
    ,recorded_date
    ,code
    ,code_system
    ,onset_datetime
    ,abatement_datetime
    ,client_id
    ,ingest_date
from joined