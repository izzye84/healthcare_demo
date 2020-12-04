{{
    config(
        dist = 'feature_link'
    )
}}

{{ 
    dbt_utils.union_relations(
        relations=[
            ref('eng_admit_length'), 
            ref('eng_admit_rate'),
            ref('eng_age_at_claim_date'),
            ref('eng_readmission_status_30day')
        ]
    )
}}