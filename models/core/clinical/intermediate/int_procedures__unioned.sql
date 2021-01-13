{{ config(dist = 'identifier_external_source') }}

{{
    dbt_utils.union_relations (
        relations=[
            ref('stg_ssm__clinical_procedure'),
            ref('stg_ssm__claim_procedures'),
            ref('stg_humana__claim_procedures'),
            ref('stg_conviva__procedure')
        ]
    )
}}