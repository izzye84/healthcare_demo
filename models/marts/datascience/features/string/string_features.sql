{{
    config(
        dist = 'feature_link'
    )
}}

{{ 
    dbt_utils.union_relations(
        relations=[
            ref('eng_admission_discharge_status')
        ]
    )
}}