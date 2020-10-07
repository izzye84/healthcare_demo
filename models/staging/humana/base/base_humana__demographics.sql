{{
    config(
        materialized = 'ephemeral'
    )
}}

with 

source_referral as (
    select row_number() over(partition by pers_gen_key order by ingest_date desc) as row_num
        ,* 
    from {{ source('humana_src','demographics') }}
)

select {{ dbt_utils.surrogate_key(['pers_gen_key']) }} as identifier_strive_id
    ,date(decsd_date) as deceased_date
    ,{{ empty_string_to_null('client_id') }} as client_id
    ,ingest_date
from source_referral
where row_num = 1