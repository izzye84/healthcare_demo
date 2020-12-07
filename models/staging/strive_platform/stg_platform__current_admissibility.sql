with

source as (
    select * from {{ ref('base_platform__admissibility') }}
),

add_rownum as (
    select 
        identifier_sh_uid
        ,active
        ,row_number() over(partition by identifier_sh_uid order by ingest_date desc) as rownum
    from source
),

rename as (
    select
        identifier_sh_uid
        ,active
    from add_rownum
    where rownum = 1
)

select * from rename