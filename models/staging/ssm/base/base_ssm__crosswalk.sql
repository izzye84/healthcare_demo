{{
    config(
        materialized = 'ephemeral'
    )
}}

with

member_crosswalk_source as (
    select * from {{ source('ssm_claims','member_crosswalk') }}
),

member_crosswalk_add_rownum as (
    select row_number() over(partition by person_id, lob, insurance_name order by ingest_date desc) as row_num
        ,*
    from member_crosswalk_source
),

member_crosswalk_dedup as (
    select *
    from member_crosswalk_add_rownum
    where row_num = 1
),

member_crosswalk_trim_id as (
    select trim(enterprise_mrn) as enterprise_mrn
        ,trim(person_id) as person_id
        ,trim(lob) as lob
        ,trim(insurance_name) as insurance_name
        ,ingest_date
    from member_crosswalk_dedup
)

select {{ empty_string_to_null('enterprise_mrn') }} as enterprise_mrn
    ,{{ empty_string_to_null('person_id') }} as person_id
    ,{{ empty_string_to_null('lob') }} as lob
    ,{{ empty_string_to_null('insurance_name') }} as insurance_name
    ,ingest_date
from member_crosswalk_trim_id