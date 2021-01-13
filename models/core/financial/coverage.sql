{{
    config(
        dist = 'identifier_external_source'
    )
}}

with

unioned_coverage as (
    select *
    from {{ ref('int_coverage__unioned') }}
)

select identifier
    ,identifier_external_source
    ,status
    ,period_coverage_start
    ,period_coverage_end
    ,subscriber_id
    ,relationship
    ,network
    ,payor
    ,client_id
    ,ingest_date
from unioned_coverage