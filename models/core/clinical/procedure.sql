{{
    config(
        dist = 'identifier_external_source'
    )
}}

with

procedures as (
    select * from {{ ref('int_procedures__unioned') }}
)

select
    identifier_procedure
    ,identifier_external_source
    ,encounter
    ,category_code
    ,code
    ,code_system
    ,code_display
    ,performed_datetime
    ,client_id
    ,ingest_date
    
from procedures
