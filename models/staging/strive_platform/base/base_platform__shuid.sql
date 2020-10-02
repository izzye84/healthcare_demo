{{
    config(
        materialized = 'ephemeral'
    )
}}

with 

eligibility_ssm_shuid as (
    select * from {{ source('platform_data_pro','eligibility') }} where client_id = 'ssm'
),

final as (
    select distinct
      {{ dbt_utils.surrogate_key(['member_id','insurance_type','payer']) }} as identifier_strive_id
      ,sh_uid as identifier_sh_uid
      ,client_id
      ,max(ingest_id) as ingest_date
    from eligibility_ssm_shuid
    group by 1,2,3
)

select * from final