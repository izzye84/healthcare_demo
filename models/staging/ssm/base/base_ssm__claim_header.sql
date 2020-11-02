{{
    config(
        materialized = 'ephemeral'
    )
}}

with source as (

    select * from {{ source('ssm_claims','claim_header') }}

),

add_row_num as (
    select row_number() over(partition by claim_number
                             order by ingest_date desc) as row_num,
            *
    from source
),

source_renamed as (

    select {{ empty_string_to_null('claim_number') }} as identifier_claim_header,
           {{ dbt_utils.surrogate_key(['patient_external_id','lob','insurance_name']) }} as patient,
           {{ empty_string_to_null('subscriber_id') }} as subscriber_id,
           {{ empty_string_to_null('dependent_number') }} as dependent_number,
           {{ empty_string_to_null('place_of_service') }} as place_of_service,
           {{ empty_string_to_null('type_of_bill') }} as sub_type,
           service_date_time,
           date(claim_from_date_time) as billable_period_start,
           date(claim_thru_date_time) as billable_period_end,
           {{ empty_string_to_null('admission_type') }} as admission_type,
           {{ empty_string_to_null('admission_source') }} as admission_source,
           {{ empty_string_to_null('discharge_status') }} as discharge_status,
           {{ empty_string_to_null('rev_code') }} as rev_code,
           {{ empty_string_to_null('drg') }} as drg,
           {{ empty_string_to_null('first_name') }} as first_name,
           {{ empty_string_to_null('last_name') }} as last_name,
           {{ empty_string_to_null('gender') }} as gender,
           date_of_birth,
           {{ empty_string_to_null('procedure_code1') }} as procedure_code1,
           {{ empty_string_to_null('procedure_code2') }} as procedure_code2,
           {{ empty_string_to_null('procedure_code3') }} as procedure_code3,
           {{ empty_string_to_null('procedure_code4') }} as procedure_code4,
           {{ empty_string_to_null('procedure_code5') }} as procedure_code5,
           {{ empty_string_to_null('procedure_code6') }} as procedure_code6,
           {{ empty_string_to_null('procedure_code7') }} as procedure_code7,
           {{ empty_string_to_null('procedure_code8') }} as procedure_code8,
           {{ empty_string_to_null('procedure_code9') }} as procedure_code9,
           {{ empty_string_to_null('procedure_code10') }} as procedure_code10,
           {{ empty_string_to_null('procedure_code11') }} as procedure_code11,
           {{ empty_string_to_null('procedure_code12') }} as procedure_code12,
           {{ empty_string_to_null('procedure_code13') }} as procedure_code13,
           {{ empty_string_to_null('procedure_code14') }} as procedure_code14,
           {{ empty_string_to_null('procedure_code15') }} as procedure_code15,
           {{ empty_string_to_null('procedure_code16') }} as procedure_code16,
           {{ empty_string_to_null('procedure_code17') }} as procedure_code17,
           {{ empty_string_to_null('procedure_code18') }} as procedure_code18,
           {{ empty_string_to_null('procedure_code19') }} as procedure_code19,
           {{ empty_string_to_null('procedure_code20') }} as procedure_code20,
           {{ empty_string_to_null('procedure_code1_modifier') }} as procedure_code1_modifier,
           {{ empty_string_to_null('procedure_code2_modifier') }} as procedure_code2_modifier,
           {{ empty_string_to_null('procedure_code3_modifier') }} as procedure_code3_modifier,
           {{ empty_string_to_null('procedure_code4_modifier') }} as procedure_code4_modifier,
           {{ empty_string_to_null('procedure_code5_modifier') }} as procedure_code5_modifier,
           {{ empty_string_to_null('procedure_code6_modifier') }} as procedure_code6_modifier,
           {{ empty_string_to_null('procedure_code7_modifier') }} as procedure_code7_modifier,
           {{ empty_string_to_null('procedure_code8_modifier') }} as procedure_code8_modifier,
           {{ empty_string_to_null('procedure_code9_modifier') }} as procedure_code9_modifier,
           {{ empty_string_to_null('procedure_code10_modifier') }} as procedure_code10_modifier,
           {{ empty_string_to_null('procedure_code11_modifier') }} as procedure_code11_modifier,
           {{ empty_string_to_null('procedure_code12_modifier') }} as procedure_code12_modifier,
           {{ empty_string_to_null('procedure_code13_modifier') }} as procedure_code13_modifier,
           {{ empty_string_to_null('procedure_code14_modifier') }} as procedure_code14_modifier,
           {{ empty_string_to_null('procedure_code15_modifier') }} as procedure_code15_modifier,
           {{ empty_string_to_null('procedure_code16_modifier') }} as procedure_code16_modifier,
           {{ empty_string_to_null('procedure_code17_modifier') }} as procedure_code17_modifier,
           {{ empty_string_to_null('procedure_code18_modifier') }} as procedure_code18_modifier,
           {{ empty_string_to_null('procedure_code19_modifier') }} as procedure_code19_modifier,
           {{ empty_string_to_null('procedure_code20_modifier') }} as procedure_code20_modifier,
           {{ empty_string_to_null('diagnosis_code1') }} as diagnosis_code1,
           {{ empty_string_to_null('diagnosis_code2') }} as diagnosis_code2,
           {{ empty_string_to_null('diagnosis_code3') }} as diagnosis_code3,
           {{ empty_string_to_null('diagnosis_code4') }} as diagnosis_code4,
           {{ empty_string_to_null('diagnosis_code5') }} as diagnosis_code5,
           {{ empty_string_to_null('diagnosis_code6') }} as diagnosis_code6,
           {{ empty_string_to_null('diagnosis_code7') }} as diagnosis_code7,
           {{ empty_string_to_null('diagnosis_code8') }} as diagnosis_code8,
           {{ empty_string_to_null('diagnosis_code9') }} as diagnosis_code9,
           {{ empty_string_to_null('diagnosis_code10') }} as diagnosis_code10,
           {{ empty_string_to_null('diagnosis_code11') }} as diagnosis_code11,
           {{ empty_string_to_null('diagnosis_code12') }} as diagnosis_code12,
           {{ empty_string_to_null('diagnosis_code13') }} as diagnosis_code13,
           {{ empty_string_to_null('diagnosis_code14') }} as diagnosis_code14,
           {{ empty_string_to_null('diagnosis_code15') }} as diagnosis_code15,
           {{ empty_string_to_null('diagnosis_code16') }} as diagnosis_code16,
           {{ empty_string_to_null('diagnosis_code17') }} as diagnosis_code17,
           {{ empty_string_to_null('diagnosis_code18') }} as diagnosis_code18,
           {{ empty_string_to_null('diagnosis_code19') }} as diagnosis_code19,
           {{ empty_string_to_null('diagnosis_code20') }} as diagnosis_code20,
           {{ empty_string_to_null('icd_version_indicator') }} as icd_version_indicator,
           allowed_amount,
           charge_amount,
           payment_date_time,
           plan_payment_amount as total,
           member_payment_amount,
           {{ empty_string_to_null('claim_adjustment_type_code') }} as claim_adjustment_type_code,
           {{ empty_string_to_null('claim_adjustment_sequence_number') }} as claim_adjustment_sequence_number,
           claim_adjustment_date_time,
           {{ empty_string_to_null('submitting_provider_npi') }} as provider,
           {{ empty_string_to_null('rendering_provider_npi') }} as rendering_provider_npi,
           {{ empty_string_to_null('referring_provider_npi') }} as referring_provider_npi,
           {{ empty_string_to_null('pcp_provider_npi') }} as pcp_provider_npi,
           {{ empty_string_to_null('facility_npi') }} as facility,
           lob,
           insurance_name as insurer,
           client_id,
           ingest_date

    from add_row_num
    where row_num = 1

)

select *
from source_renamed
