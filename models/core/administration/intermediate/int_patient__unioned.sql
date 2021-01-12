{{
  config(
        dist = 'identifier_external_source '
    )
}}

{{
    dbt_utils.union_relations (
        relations=[
            ref('member_patient__joined'),
            ref('referral_demographics__joined'),
            ref('stg_conviva__patient')
        ]
    )
}}