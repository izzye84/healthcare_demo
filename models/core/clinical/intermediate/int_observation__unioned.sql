{{
  config(
    dist = 'identifier_external_source'
    )
}}

{{
    dbt_utils.union_relations(
        relations=[
            ref('stg_ssm__lab_observation'),
            ref('stg_ssm__vital_observation'),
            ref('stg_humana__lab_observation'),
            ref('stg_conviva__lab_observation'),
            ref('stg_conviva__vital_observation')
        ]
    )
}}