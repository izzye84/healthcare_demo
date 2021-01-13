{{
    config(
        dist = 'identifier_external_source'
    )
}}

with

ssm_clinical_procedures as (
    select * from {{ ref('int_procedures__unioned') }}
)

select 
    identifier
    ,identifier_external_source
    ,encounter
    ,category_code
    ,code
    ,code_system
    ,code_display
    ,performed_datetime
    ,client_id
    ,ingest_date
from ssm_clinical_procedures
