with unpivot_claim_header as (

    {{ dbt_utils.unpivot(
      relation=ref('base_ssm__claim_header'),
      exclude=['identifier_claim_header',
               'patient',
               'billable_period_start',
               'client_id',
               'ingest_date'],
      remove=['subscriber_id',
              'dependent_number',
              'place_of_service',
              'sub_type',
              'service_date_time',
              'billable_period_end',
              'admission_type',
              'admission_source',
              'discharge_status',
              'rev_code',
              'drg',
              'first_name',
              'last_name',
              'gender',
              'date_of_birth',
              'diagnosis_code1',
              'diagnosis_code2',
              'diagnosis_code3',
              'diagnosis_code4',
              'diagnosis_code5',
              'diagnosis_code6',
              'diagnosis_code7',
              'diagnosis_code8',
              'diagnosis_code9',
              'diagnosis_code10',
              'diagnosis_code11',
              'diagnosis_code12',
              'diagnosis_code13',
              'diagnosis_code14',
              'diagnosis_code15',
              'diagnosis_code16',
              'diagnosis_code17',
              'diagnosis_code18',
              'diagnosis_code19',
              'diagnosis_code20',
              'icd_version_indicator',
              'allowed_amount',
              'charge_amount',
              'payment_date_time',
              'total',
              'member_payment_amount',
              'claim_adjustment_type_code',
              'claim_adjustment_sequence_number',
              'claim_adjustment_date_time',
              'provider',
              'rendering_provider_npi',
              'referring_provider_npi',
              'pcp_provider_npi',
              'facility',
              'lob',
              'insurer'],
      field_name='code_system',
      value_name='code'
      ) }}

),

hcpcs_codes as (

    select distinct {{ dbt_utils.surrogate_key(['product_or_service',
                                                'identifier_claim_header',
                                                'identifier_claim_line',
                                                'patient']) }} as identifier_procedure,
                    patient as identifier_external_source,
                    identifier_claim_header as encounter,
                    'claim' as category_code,
                    product_or_service as code,
                    'HCPCS' as code_system,
                    serviced_period_start as performed_datetime,
                    client_id,
                    ingest_date

    from {{ ref('base_ssm__claim_detail') }}
    where product_or_service is not null

),

icd_codes as (

    select distinct {{ dbt_utils.surrogate_key(['code',
                                                'identifier_claim_header',
                                                'patient']) }} as identifier_procedure,
                    patient as identifier_external_source,
                    identifier_claim_header as encounter,
                    'claim' as category_code,
                    code,
                    'ICD' as code_system,
                    billable_period_start as performed_datetime,
                    client_id,
                    ingest_date

    from unpivot_claim_header
    where code is not null

),

icd_and_hcpcs_combined as (

    select identifier_procedure,
           identifier_external_source,
           encounter,
           category_code,
           code,
           code_system,
           performed_datetime,
           client_id,
           ingest_date

    from hcpcs_codes

    union all

    select identifier_procedure,
           identifier_external_source,
           encounter,
           category_code,
           code,
           code_system,
           performed_datetime,
           client_id,
           ingest_date

    from icd_codes

)

select *
from icd_and_hcpcs_combined
