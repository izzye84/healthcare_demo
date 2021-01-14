/* using dbt's union_relations function to consolidate staging tables
   into a single output */
{{ dbt_utils.union_relations(
    relations=[
        ref('stg_conviva__facility_claim_line'),
        ref('stg_humana__claim_line'),
        ref('stg_ssm__claim_line'),
        ]
    )
}}
