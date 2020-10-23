with

eligibility as (
    select * from {{ ref('base_humana__eligibility') }}
)

select identifier
    ,identifier_external_source
    
    ,case
        when period_coverage_end >= date(getdate()) then 'active'
        else 'cancelled'
    end as status

    ,period_coverage_start
    ,period_coverage_end
    ,subscriber_id
    ,network
    ,payor
    ,client_id
    ,ingest_date
from eligibility