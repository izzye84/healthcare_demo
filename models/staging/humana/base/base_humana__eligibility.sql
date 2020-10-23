{{
    config(
        materialized = 'ephemeral'
    )
}}

with 

demographics_eligibility as (
    select row_number() over(partition by pers_gen_key order by ingest_date desc,date(substring(cov_eff_date,1,9)) desc) as row_num
        ,* 
    from {{ source('humana_src','demographics') }}
)

select {{ dbt_utils.surrogate_key(['pers_gen_key','cov_eff_date']) }} as identifier
    ,{{ dbt_utils.surrogate_key(['pers_gen_key']) }} as identifier_external_source
    ,date(substring(cov_eff_date,1,9)) as period_coverage_start
    ,date(substring(cov_end_date,1,9)) as period_coverage_end
    ,{{ empty_string_to_null('pers_gen_key') }} as subscriber_id
    ,{{ empty_string_to_null('lob') }} as network
    ,'Humana' as payor
    ,{{ empty_string_to_null('client_id') }} as client_id
    ,ingest_date
from demographics_eligibility
where row_num = 1