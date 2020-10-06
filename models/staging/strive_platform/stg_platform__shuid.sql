with 

eligibility_ssm_shuid as (
    
    select row_number() over(partition by member_id,insurance_type,payer,sh_uid order by ingest_id desc) as row_num
        ,trim(member_id) as member_id
        ,trim(insurance_type) as insurance_type
        ,trim(payer) as payer
        ,sh_uid
        ,client_id
        ,ingest_id
    from {{ source('platform_data_pro','eligibility') }} 
    where client_id = 'ssm'
),

final as (
    select distinct
      {{ dbt_utils.surrogate_key(['member_id','insurance_type','payer']) }} as identifier_strive_id
      ,sh_uid as identifier_sh_uid
      ,client_id
      ,date(substring(ingest_id,2,8))::timestamp as ingest_date
    from eligibility_ssm_shuid
    where row_num = 1
)

select * from final
