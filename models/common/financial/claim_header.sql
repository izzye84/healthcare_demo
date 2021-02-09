with combine_all_clients as (

    /* using dbt's union_relations function to consolidate staging tables
       into a single output */
    {{ dbt_utils.union_relations(
        relations=[
            ref('stg_humana_common__claim_header'),
            ]
        )
    }}

),

-- dbt_utils.union_relations adds an additional column that we drop from the final output
claim_header as (

  select sh_claim_id,
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
         paid_amount,
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
         authorization_id
         client_id,
         ingest_timestamp_utc

    from combine_all_clients
)

select *
from claim_header
