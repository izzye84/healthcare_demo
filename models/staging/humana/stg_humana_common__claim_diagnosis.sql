with unpivot_medical_claims as (

    {{ dbt_utils.unpivot(
      relation=ref('base_humana_common__medical_claims'),
      exclude=['claim_id',
               'member_id',
               'serv_to_date',
               'serv_from_date',
               'client_id',
               'ingest_timestamp_utc'],
      remove=['admit_datetime',
              'admit_diagnosis_code',
              'allowed_amount',
              'bill_type',
              'billing_provider_npi',
              'claim_line',
              'claim_processed_date',
              'discharge_datetime',
              'discharge_status',
              'drg_code',
              'icd_primary_procedure_code',
              'icd_proc_02_cd',
              'icd_proc_03_cd',
              'icd_proc_04_cd',
              'icd_proc_05_cd',
              'line_number',
              'member_id',
              'member_liability',
              'ndc_code',
              'network_status',
              'place_of_service_code',
              'plan_payment',
              'procedure_code',
              'procedure_modifier_1',
              'procedure_modifier_2',
              'provider_tin',
              'revenue_code',
              'service_date_to',
              'service_date_from',
              'service_units',
              'source_of_admission',
              'src_mbr_id',
              'total_charges',
              'transaction_type',
              'client_id',
              'ingest_timestamp_utc'],
      field_name='icd_diagnosis_code_rank',
      value_name='icd_diagnosis_code'
      ) }}

),

claim_diagnoses as (

    select distinct {{ dbt_utils.surrogate_key(['claim_id',
                                                'icd_diagnosis_code',
                                                'icd_diagnosis_code_rank',
                                                'serv_from_date',
                                                'serv_to_date']) }} as sh_diagnosis_id,
                    claim_id,
                    member_id,
                    null as patient_id,
                    icd_diagnosis_code,
                    'ICD-10' AS icd_indicator,
                    case when icd_diagnosis_code_rank = 'principal_diagnosis_code'
                         then 1
                         else split_part(icd_diagnosis_code_rank, '_', 4)::int
                    end as icd_diagnosis_code_rank ,
                    serv_from_date as service_date_from,
                    serv_to_date as service_date_to,
                    client_id,
                    max(ingest_timestamp_utc) over (partition by claim_id) as ingest_timestamp_utc

    from unpivot_medical_claims
    where icd_diagnosis_code is not null

)

SELECT *
FROM claim_diagnoses
