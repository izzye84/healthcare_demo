with source as (

    select *
    from {{ source('conviva_claims','facility_claim_header') }}
         {{ limit_dev_data() }}

),

add_row_num as (

    select row_number() over(partition by facility_header_claim_id
                             order by ingest_date desc) as row_num,
           *
    from source

),

source_renamed as (

    select facility_header_claim_id as identifier_claim_header,
           accident_date,
           admit_datetime,
           {{ empty_string_to_null('admit_diagnosis_code') }} as admit_diagnosis_code,
           {{ empty_string_to_null('admit_type') }} as admit_type,
           {{ empty_string_to_null('admitting_provider_npi') }} as admitting_provider_npi,
           allowed_amount,
           {{ empty_string_to_null('apc') }} as apc,
           {{ empty_string_to_null('apc_version') }} as apc_version,
           {{ empty_string_to_null('bill_type') }} as sub_type,
           {{ empty_string_to_null('billing_provider_npi') }} as provider,
           {{ empty_string_to_null('capitated_services_indicator') }} as capitated_services_indicator,
           claim_processed_date,
           discharge_datetime,
           {{ empty_string_to_null('discharge_status') }} as discharge_status,
           {{ empty_string_to_null('drg_code') }} as drg_code,
           {{ empty_string_to_null('facility_npi') }} as facility,
           {{ empty_string_to_null('icd_primary_procedure_code') }} as icd_primary_procedure_code,
           {{ empty_string_to_null('icd_primary_procedure_indicator') }} as icd_primary_procedure_indicator,
           {{ empty_string_to_null('insurance_product_type_desc') }} as insurer,
           {{ empty_string_to_null('line_of_business_desc') }} as line_of_business_desc,
           {{ empty_string_to_null('member_id') }} as member_id,
           {{ empty_string_to_null('member_liability') }} as member_liability,
           {{ empty_string_to_null('network_status') }} as network_status,
           paid_amount as total,
           {{ dbt_utils.surrogate_key(['patient_id']) }} as patient,
           patient_id,
           {{ empty_string_to_null('place_of_service_code') }} as place_of_service_code,
           {{ empty_string_to_null('place_of_service_description') }} as place_of_service_description,
           plan_payment,
           {{ empty_string_to_null('principal_diagnosis_code') }} as principal_diagnosis_code,
           {{ empty_string_to_null('principal_diagnosis_icd_indicator') }} as principal_diagnosis_icd_indicator,
           {{ empty_string_to_null('professional_claim_id') }} as professional_claim_id,
           {{ empty_string_to_null('provider_tin') }} as provider_tin,
           {{ empty_string_to_null('referring_provider_npi') }} as referring_provider_npi,
           service_date_from as billable_period_start,
           service_date_to as billable_period_end,
           {{ empty_string_to_null('source_of_admission') }} as source_of_admission,
           total_charges,
           total_standard_cost,
           {{ empty_string_to_null('transaction_type') }} as transaction_type,
           client_id,
           ingest_date

    from add_row_num
    where row_num = 1

)

select *
from source_renamed
