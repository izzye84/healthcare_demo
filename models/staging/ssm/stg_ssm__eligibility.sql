with

eligibility as (
    select * from {{ ref('base_ssm__member_eligibility') }}
)

select identifier
    ,identifier_external_source
    
    ,case
        when period_coverage_end >= date(getdate()) then 'active'
        else 'cancelled'
    end as status

    ,period_coverage_start
    ,period_coverage_end
    ,identifier_external_subscriber_id as subscriber_id
    ,relationship
    ,network
    ,payor
    ,client_id
    ,ingest_date
from eligibility