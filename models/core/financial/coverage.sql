{{
    config(
        dist = 'identifier_external_source'
    )
}}

with

ssm_coverage as (
    select * from {{ ref('stg_ssm__eligibility') }}
),

humana_coverage as (
    select * from {{ ref('stg_humana__eligibility') }}
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
from ssm_coverage

union all

select identifier
    ,identifier_external_source
    ,status
    ,period_coverage_start
    ,period_coverage_end
    ,subscriber_id
    ,null as relationship
    ,network
    ,payor
    ,client_id
    ,ingest_date
from humana_coverage