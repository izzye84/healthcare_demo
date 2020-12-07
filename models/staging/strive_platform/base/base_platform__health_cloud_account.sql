{{
  config(
    materialized = 'ephemeral'
    )
}}

with

source as (
    select
        sh_uid
        ,patient_status__c
        ,inadmissible_reason__c
        ,disenrolled_reason__c
        ,ingest_id
        ,client_id
    from {{ source('platform_data_pro', 'health_cloud_account') }}
),

renamed as (
    select
        sh_uid as identifier_sh_uid
        ,patient_status__c as status
        ,inadmissible_reason__c as inadmissible_reason
        ,disenrolled_reason__c as disenrolled_reason
        ,(substring(ingest_id,2,8) || ' ' || substring(ingest_id,12,6))::timestamp as ingest_date
        ,client_id
    from source
)

select * from renamed