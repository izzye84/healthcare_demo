with

clinical_procedure_base as (
    select * from {{ ref('base_ssm__clinical_procedure') }}
),

crosswalk_base as (
    select * from {{ ref('base_ssm__crosswalk') }}
),

joined as (
    select crosswalk_base.person_id
        ,crosswalk_base.lob
        ,crosswalk_base.insurance_name
        ,clinical_procedure_base.encounter
        ,clinical_procedure_base.code
        ,clinical_procedure_base.code_system
        ,clinical_procedure_base.modified_timestamp
        ,clinical_procedure_base.client_id
        ,clinical_procedure_base.ingest_date
    from clinical_procedure_base join crosswalk_base
        on clinical_procedure_base.patient_account_number = crosswalk_base.enterprise_mrn
)

select {{ dbt_utils.surrogate_key(['person_id','lob','insurance_name','code','encounter']) }} as identifier
    ,{{ dbt_utils.surrogate_key(['person_id','lob','insurance_name']) }} as identifier_strive_id
    ,encounter
    ,code
    ,code_system
    ,date(modified_timestamp) as performed_datetime
    ,client_id
    ,ingest_date
from joined