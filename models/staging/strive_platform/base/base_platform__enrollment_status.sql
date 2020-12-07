{{
  config(
    materialized = 'ephemeral'
    )
}}

with

source as (
    select * from {{ source('platform_data_pro', 'enrollment_status') }}
),

renamed as (
    select
        sh_uid as identifier_sh_uid
        ,status
        ,reason as status_reason
        ,disenrolled_date
        ,update_date
        ,client_id
        ,(substring(run_id,2,8) || ' ' || substring(run_id,12,6))::timestamp as ingest_date
    from source
)

select * from renamed