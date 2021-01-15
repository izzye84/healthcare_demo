with unpivot_medical_claims as (

    {{ dbt_utils.unpivot(
      relation=ref('base_humana__medical_claims'),
      exclude=['identifier_claim_header',
               'identifier_external_source',
               'serviced_period_start',
               'medclm_key',
               'client_id',
               'ingest_date'],
      remove=['admit_diag_cd',
              'admit_src_cd',
              'allow_icob_admit_cnt',
              'allow_icob_amt',
              'allow_icob_days_cnt',
              'allow_icob_visits_cnt',
              'sub_type',
              'billable_period_end',
              'billable_period_start',
              'chrg_amt',
              'chrg_days_cnt',
              'clm_aso_ind',
              'cob_paid_amt',
              'copay_amt',
              'cov_month',
              'ctrct_cat_cd',
              'deduct_amt',
              'diagnosis_code_1',
              'diagnosis_code_2',
              'diagnosis_code_3',
              'diagnosis_code_4',
              'diagnosis_code_5',
              'diagnosis_code_6',
              'diagnosis_code_7',
              'diagnosis_code_8',
              'diagnosis_code_9',
              'diagnosis_code_10',
              'diagnosis_code_11',
              'diagnosis_code_12',
              'diagnosis_code_13',
              'diagnosis_code_14',
              'diagnosis_code_15',
              'diagnosis_code_16',
              'diagnosis_code_17',
              'diagnosis_code_18',
              'diagnosis_code_19',
              'diagnosis_code_20',
              'diagnosis_code_21',
              'diagnosis_code_22',
              'diagnosis_code_23',
              'diagnosis_code_24',
              'diagnosis_code_25',
              'dischg_stat_cd',
              'drg_lclm_cd',
              'fin_prod_cd',
              'identifier_claim_line',
              'in_plan_ntwk_ind',
              'mbr_cost_shr_amt',
              'mco_contract_nbr',
              'modifier_1',
              'modifier_2',
              'ndc_id',
              'net_paid_amt',
              'net_paid_days_cnt',
              'paid_date',
              'patient',
              'pbp_segment_id',
              'plan_benefit_package_id',
              'pot_cd',
              'prov_tax_id',
              'npi_id',
              'pymt_cat_cd',
              'raw_clm_billg_prov_key',
              'raw_clm_refr1_prov_key',
              'raw_clm_rendr_prov_key',
              'revenue',
              'serviced_period_end',
              'quantity',
              'admit_datetime',
              'discharge_datetime',
              'src_mbr_id',
              'src_platform_cd',
              'src_prov_specialty_cd'],
      field_name='code_system',
      value_name='code'
      ) }}

),

removing_invalid_codes as (

    select medclm_key,
           identifier_external_source,
           identifier_claim_header as encounter,
           'claim'::varchar as category_code,
           -- removing codes with less than 7 characters as ICD-10 codes are 7 characters in length
           case when len(code)::int < 7 and
                     code_system ilike 'icd%'
                then null
                else code
           end as code,
           case when code_system ilike 'icd%'
                then 'ICD'
                else 'HCPCS'
           end as code_system,
           serviced_period_start as performed_datetime,
           client_id,
           ingest_date

    from unpivot_medical_claims

),

distinct_claim_procedures as (

    select distinct {{ dbt_utils.surrogate_key(['code', 'medclm_key', 'identifier_external_source']) }} as identifier_procedure,
                    identifier_external_source,
                    encounter,
                    category_code,
                    code,
                    code_system,
                    performed_datetime,
                    client_id,
                    ingest_date

    from removing_invalid_codes
    where code is not null

)

select *
from distinct_claim_procedures
