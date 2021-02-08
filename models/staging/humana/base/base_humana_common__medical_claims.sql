with source as (

    select *
    from {{ source('humana_src','medical_claims') }}
         {{ limit_dev_data() }}

),

add_row_num as (

    select row_number() over(partition by medclm_key
                             order by ingest_date desc) as row_num,
           *
    from source
),

source_renamed as (

    select {{ empty_string_to_null('admit_diag_cd') }} as admit_diagnosis_code,
           {{ empty_string_to_null('admit_src_cd') }} as source_of_admission,
           --{{ empty_string_to_null('allow_icob_admit_cnt') }} as allow_icob_admit_cnt,
           {{ empty_string_to_null('allow_icob_amt') }} as allowed_amount,
           --{{ empty_string_to_null('allow_icob_days_cnt') }} as allow_icob_days_cnt,
           --{{ empty_string_to_null('allow_icob_visits_cnt') }} as allow_icob_visits_cnt,
           {{ empty_string_to_null('bill_type_cd') }} as bill_type,
           chrg_amt::numeric(18,2) as total_charges,
           --{{ empty_string_to_null('chrg_days_cnt') }} as chrg_days_cnt,
           --{{ empty_string_to_null('clm_aso_ind') }} as clm_aso_ind,
           --{{ empty_string_to_null('cob_paid_amt') }} as cob_paid_amt,
           --{{ empty_string_to_null('copay_amt') }} as copay_amt,
           --{{ empty_string_to_null('cov_month') }} as cov_month,
           {{ empty_string_to_null('cpt_mod_cd') }} as procedure_modifier_1,
           {{ empty_string_to_null('cpt_mod_cd2') }} as procedure_modifier_2,
           --{{ empty_string_to_null('ctrct_cat_cd') }} as ctrct_cat_cd,
           --{{ empty_string_to_null('deduct_amt') }} as deduct_amt,
           {{ empty_string_to_null('diag_cd2') }} as icd_diagnosis_code_2,
           {{ empty_string_to_null('diag_cd3') }} as icd_diagnosis_code_3,
           {{ empty_string_to_null('diag_cd4') }} as icd_diagnosis_code_4,
           {{ empty_string_to_null('diag_cd5') }} as icd_diagnosis_code_5,
           {{ empty_string_to_null('diag_cd6') }} as icd_diagnosis_code_6,
           {{ empty_string_to_null('diag_cd7') }} as icd_diagnosis_code_7,
           {{ empty_string_to_null('diag_cd8') }} as icd_diagnosis_code_8,
           {{ empty_string_to_null('diag_cd9') }} as icd_diagnosis_code_9,
           {{ empty_string_to_null('diag_cd10') }} as icd_diagnosis_code_10,
           {{ empty_string_to_null('diag_cd11') }} as icd_diagnosis_code_11,
           {{ empty_string_to_null('diag_cd12') }} as icd_diagnosis_code_12,
           {{ empty_string_to_null('diag_cd13') }} as icd_diagnosis_code_13,
           {{ empty_string_to_null('diag_cd14') }} as icd_diagnosis_code_14,
           {{ empty_string_to_null('diag_cd15') }} as icd_diagnosis_code_15,
           {{ empty_string_to_null('diag_cd16') }} as icd_diagnosis_code_16,
           {{ empty_string_to_null('diag_cd17') }} as icd_diagnosis_code_17,
           {{ empty_string_to_null('diag_cd18') }} as icd_diagnosis_code_18,
           {{ empty_string_to_null('diag_cd19') }} as icd_diagnosis_code_19,
           {{ empty_string_to_null('diag_cd20') }} as icd_diagnosis_code_20,
           {{ empty_string_to_null('diag_cd21') }} as icd_diagnosis_code_21,
           {{ empty_string_to_null('diag_cd22') }} as icd_diagnosis_code_22,
           {{ empty_string_to_null('diag_cd23') }} as icd_diagnosis_code_23,
           {{ empty_string_to_null('diag_cd24') }} as icd_diagnosis_code_24,
           {{ empty_string_to_null('diag_cd25') }} as icd_diagnosis_code_25,
           {{ empty_string_to_null('dischg_stat_cd') }} as discharge_status,
           {{ empty_string_to_null('drg_lclm_cd') }} as drg_code,
           --{{ empty_string_to_null('fin_prod_cd') }} as fin_prod_cd,
           {{ empty_string_to_null('hcpcs_cpt4_base_cd1') }} as procedure_code,
           {{ empty_string_to_null('icd_proc_01_cd') }} as icd_primary_procedure_code,
           {{ empty_string_to_null('icd_proc_02_cd') }} as icd_proc_02_cd,
           {{ empty_string_to_null('icd_proc_03_cd') }} as icd_proc_03_cd,
           {{ empty_string_to_null('icd_proc_04_cd') }} as icd_proc_04_cd,
           {{ empty_string_to_null('icd_proc_05_cd') }} as icd_proc_05_cd,
           {{ empty_string_to_null('in_plan_ntwk_ind') }} as network_status,
           mbr_pers_gen_key as member_id,
           {{ empty_string_to_null('mbr_cost_shr_amt') }} as member_liability,
           --{{ empty_string_to_null('mco_contract_nbr') }} as mco_contract_nbr,
           medclm_key as line_number,
           medclm_lclm_key as claim_id,
           date(medclm_lclm_to_date) as service_date_to,
           date(medclm_lclm_from_date) as service_date_from,
           {{ empty_string_to_null('ndc_id') }} as ndc_code,
           {{ empty_string_to_null('net_paid_amt') }} as plan_payment,
           --{{ empty_string_to_null('net_paid_days_cnt') }} as net_paid_days_cnt,
           {{ empty_string_to_null('npi_id') }} as billing_provider_npi,
           date(paid_date) as claim_processed_date,
           --{{ empty_string_to_null('pbp_segment_id') }} as pbp_segment_id,
           --{{ empty_string_to_null('plan_benefit_package_id') }} as plan_benefit_package_id,
           {{ empty_string_to_null('pot_cd') }} as place_of_service_code,
           {{ empty_string_to_null('primary_diag_cd') }} as principal_diagnosis_code,
           {{ empty_string_to_null('prov_tax_id') }} as provider_tin,
           {{ empty_string_to_null('pymt_cat_cd') }} as transaction_type,
           --{{ empty_string_to_null('raw_clm_billg_prov_key') }} as raw_clm_billg_prov_key,
           --{{ empty_string_to_null('raw_clm_refr1_prov_key') }} as raw_clm_refr1_prov_key,
           --{{ empty_string_to_null('raw_clm_rendr_prov_key') }} as raw_clm_rendr_prov_key,
           {{ empty_string_to_null('revenue_cd') }} as revenue_code,
           date(serv_to_date) as serv_to_date,
           date(serv_from_date) as serv_from_date,
           serv_unit_cnt::numeric(18,2) as service_units,
           date(src_admit_date) as admit_datetime,
           date(src_dischrg_date) as discharge_datetime,
           {{ empty_string_to_null('src_mbr_id') }} as src_mbr_id,
           --{{ empty_string_to_null('src_platform_cd') }} as src_platform_cd,
           --{{ empty_string_to_null('src_prov_specialty_cd') }} as src_prov_specialty_cd,
           client_id,
           ingest_date

    from add_row_num
    where row_num = 1

)

select *
from source_renamed
