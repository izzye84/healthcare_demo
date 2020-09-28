with

coverage as (
    select row_number() over(partition by identifier_strive_id order by ingest_date desc) as row_num
        ,*
    from {{ ref('stg_ssm__eligibility') }}
)

select *
from coverage
where row_num = 1