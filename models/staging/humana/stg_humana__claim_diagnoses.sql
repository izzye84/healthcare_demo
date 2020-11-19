with unpivot_medical_claims as (

    {{ dbt_utils.unpivot(
      relation=ref('base_humana__medical_claims'),
      exclude=['identifier_claim_header',
               'identifier_external_source',
               'billable_period_start',
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
              'chrg_amt',
              'chrg_days_cnt',
              'clm_aso_ind',
              'cob_paid_amt',
              'copay_amt',
              'cov_month',
              'ctrct_cat_cd',
              'deduct_amt',
              'dischg_stat_cd',
              'drg_lclm_cd',
              'fin_prod_cd',
              'icd_proc_01_cd',
              'icd_proc_02_cd',
              'icd_proc_03_cd',
              'icd_proc_04_cd',
              'icd_proc_05_cd',
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
              'product_or_service',
              'prov_tax_id',
              'npi_id',
              'pymt_cat_cd',
              'raw_clm_billg_prov_key',
              'raw_clm_refr1_prov_key',
              'raw_clm_rendr_prov_key',
              'revenue',
              'serviced_period_end',
              'serviced_period_start',
              'quantity',
              'admit_datetime',
              'discharge_datetime',
              'src_mbr_id',
              'src_platform_cd',
              'src_prov_specialty_cd'],
      field_name='sequence',
      value_name='code'
      ) }}

),

final as (

    select distinct {{ dbt_utils.surrogate_key(['identifier_claim_header',
                                                'identifier_external_source',
                                                'code']) }} as identifier_condition,
                    identifier_external_source,
                    identifier_claim_header as encounter,
                    billable_period_start as recorded_date,
                    'claim' as category_code,
                    code,
                    'ICD-10' as code_system,
                    null::date as onset_datetime,
                    null::date as abatement_datetime,
                    client_id,
                    ingest_date

    from unpivot_medical_claims
    where identifier_claim_header = medclm_key and
          code is not null

)

SELECT *
FROM final
