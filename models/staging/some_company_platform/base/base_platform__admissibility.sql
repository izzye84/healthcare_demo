{{
    config(
    materialized = 'ephemeral'
    )
}}

with

source as (
    select * from {{ source('platform_data_pro', 'patient_admissibility') }}
),

add_rownum as (
    select
        sh_uid
        ,admissible
        ,client_id
        ,ingest_id
        ,row_number() over(partition by sh_uid,sh_ingest_utc order by ingest_id desc) as row_num
    from source
),

renamed as (
    select 
        sh_uid as identifier_sh_uid
        ,admissible as active
        ,client_id
        ,(substring(ingest_id,2,8) || ' ' || substring(ingest_id,12,6))::timestamp as ingest_date
    from add_rownum
    where row_num = 1
)

select * from renamed