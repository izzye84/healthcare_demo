{{
    config(
        dist = 'identifier_strive_id'
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
    ,identifier_strive_id
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

union

select identifier
    ,identifier_strive_id
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