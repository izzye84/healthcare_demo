with

member as (
    select row_number() over(partition by identifier_strive_id order by ingest_date desc) as row_num
        ,*
    from {{ ref('stg_ssm__member') }}
)

select *
from member
where row_num = 1