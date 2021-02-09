with combine_all_clients as (

    /* using dbt's union_relations function to consolidate staging tables
       into a single output */
    {{ dbt_utils.union_relations(
        relations=[
            ref('stg_humana_common__claim_line'),
            ]
        )
    }}

),

-- dbt_utils.union_relations adds an additional column that we drop from the final output
claim_line as (

  select sh_line_number,
         claim_id,
         line_number,
         member_id,
         patient_id,
         billing_provider_npi,
         servicing_provider_npi,
         revenue_code,
         procedure_code,
         procedure_modifier_1,
         procedure_modifier_2,
         procedure_modifier_3,
         procedure_modifier_4,
         ndc_code,
         service_date_from,
         service_date_to,
         service_units,
         charge_amount,
         total_paid,
         allowed_amount,
         plan_payment,
         member_liability,
         standard_cost,
         claim_type,
         transaction_type,
         client_id,
         ingest_timestamp_utc

    from combine_all_clients
)

select *
from claim_line
