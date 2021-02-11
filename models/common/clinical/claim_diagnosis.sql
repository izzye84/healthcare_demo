with combine_all_clients as (

    /* using dbt's union_relations function to consolidate staging tables
       into a single output */
    {{ dbt_utils.union_relations(
        relations=[
            ref('stg_humana_common__claim_diagnosis'),
            ]
        )
    }}

),

-- dbt_utils.union_relations adds an additional column that we drop from the final output
claim_diagnosis as (

  select sh_diagnosis_id,
         claim_id,
         member_id,
         patient_id,
         icd_diagnosis_code,
         icd_indicator,
         icd_diagnosis_code_rank ,
         service_date_from,
         service_date_to,
         client_id,
         ingest_timestamp_utc

    from combine_all_clients
)

select *
from claim_diagnosis
