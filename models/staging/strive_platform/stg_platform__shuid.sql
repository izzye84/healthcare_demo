with

base_shuid as (
    
    select *
    from {{ ref('base_platform__shuid') }}

)

select identifier_strive_id
    ,identifier_sh_uid
    ,client_id
    ,date(substring(ingest_date,2,8))::timestamp as ingest_date
from base_shuid
