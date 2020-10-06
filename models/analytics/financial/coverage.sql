with

ssm_coverage as (
    select * from {{ ref('stg_ssm__eligibility') }}
)

select *
from ssm_coverage