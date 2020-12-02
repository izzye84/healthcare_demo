with

source as (
    select * from {{ source('platform_data_pro', 'patient_admissibility') }}
),

add_rownum as (
    select 
        sh_uid
        ,admissible
        ,row_number() over(partition by sh_uid order by ingest_id desc) as rownum
    from source
),

rename as (
    select
        sh_uid as identifier_sh_uid
        ,admissible as active
    from add_rownum
    where rownum = 1
)

select * from rename