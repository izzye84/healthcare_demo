with medical_claims as (

    select *
    from {{ ref('base_humana_common__medical_claims') }}

),

referral as (

    select member_id,
           insurance_type
    from {{ ref('base_humana_common__referral') }}

),

add_missing_fields as (

    select medical_claims.claim_id,
           medical_claims.member_id,
           medical_claims.place_of_service_code,
           medical_claims.claim_processed_date,
           medical_claims.transaction_type,
           referral.insurance_type,
           medical_claims.service_date_from,
           medical_claims.service_date_to,
           medical_claims.source_of_admission,
           medical_claims.bill_type,
           medical_claims.principal_diagnosis_code,
           medical_claims.discharge_status,
           medical_claims.drg_code,
           medical_claims.icd_primary_procedure_code,
           medical_claims.network_status,
           medical_claims.billing_provider_npi,
           medical_claims.provider_tin,
           medical_claims.admit_datetime,
           medical_claims.discharge_datetime,
           medical_claims.admit_diagnosis_code,
           sum(medical_claims.allowed_amount) over (partition by medical_claims.claim_id) as allowed_amount,
           sum(medical_claims.total_charges) over (partition by medical_claims.claim_id) as total_charges,
           sum(medical_claims.member_liability) over (partition by medical_claims.claim_id) as member_liability,
           sum(medical_claims.plan_payment) over (partition by medical_claims.claim_id) as plan_payment,
           'Facility' as claim_type,
           'Humana' as insurance_name,
           'ICD-10-PCS' as icd_primary_procedure_indicator,
           'ICD-10' as principal_diagnosis_icd_indicator,
           null as authorization_id,
           null as patient_id,
           null as place_of_service_description,
           null as admit_type,
           null as accident_date,
           null as total_standard_cost,
           null as servicing_provider_npi,
           null as admitting_provider_npi,
           null as referring_provider_npi,
           null as facility_npi,
           null as apc,
           null as apc_version,
           null as capitated_services_indicator,
           null as professional_claim_id,
           medical_claims.client_id,
           medical_claims.ingest_timestamp_utc,
           row_number() over (partition by claim_id
                              order by line_number) as row_num

    from medical_claims

    left join referral
           on medical_claims.member_id = referral.member_id

),

claim_header as (

    select {{ dbt_utils.surrogate_key(['claim_id',
                                       'client_id']) }} as sh_claim_id,
           claim_id,
           member_id,
           patient_id,
           place_of_service_code,
           place_of_service_description,
           claim_processed_date,
           transaction_type,
           insurance_name,
           insurance_type,
           service_date_from,
           service_date_to,
           admit_diagnosis_code,
           admit_datetime,
           admit_type,
           source_of_admission,
           discharge_datetime,
           discharge_status,
           accident_date,
           drg_code,
           network_status,
           member_liability + plan_payment as paid_amount,
           allowed_amount,
           plan_payment,
           member_liability,
           total_charges,
           total_standard_cost,
           servicing_provider_npi,
           admitting_provider_npi,
           bill_type,
           billing_provider_npi,
           referring_provider_npi,
           facility_npi,
           provider_tin,
           apc,
           apc_version,
           capitated_services_indicator,
           professional_claim_id,
           principal_diagnosis_code,
           principal_diagnosis_icd_indicator,
           icd_primary_procedure_code,
           icd_primary_procedure_indicator,
           claim_type,
           authorization_id,
           client_id,
           ingest_timestamp_utc

    from add_missing_fields
    where row_num = 1 --filtering to the claim header row

)

select *
from claim_header
