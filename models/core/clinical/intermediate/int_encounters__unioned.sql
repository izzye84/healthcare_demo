{{ config(dist = 'subject') }}

{{
    dbt_utils.union_relations(
        relations=[
            ref('stg_ssm__clinical_encounters'),
            ref('stg_ssm__claim_encounters'),
            ref('stg_humana__claim_encounters'),
            ref('stg_conviva__clinical_encounter')
        ]
    )
}}
