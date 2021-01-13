{{
    config(
        dist = 'identifier_external_source'
    )
}}

with

clinical_diagnoses as (
    select * from {{ ref('int_conditions__unioned') }}
)

select 
    identifier_condition
    ,identifier_external_source
    ,encounter
    ,category_code
    ,code
    ,code_system
    ,code_display
    ,recorded_date
    ,onset_datetime
    ,abatement_datetime
    ,client_id
    ,ingest_date
from clinical_diagnoses
