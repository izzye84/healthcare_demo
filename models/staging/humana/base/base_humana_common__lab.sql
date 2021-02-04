{{
    config(
        materialized = 'table'
    )
}}

with

lab_source as (
    select * from {{ source('humana_src','lab_claims') }} {{ limit_dev_data() }}
),

lab_add_rownum as (
    select row_number() over(partition by pers_gen_key, lab_result_key order by ingest_date desc) as row_num
        ,*
    from lab_source
),

renamed as (
    select {{ empty_string_to_null('pers_gen_key') }} as patient_id
        , {{ empty_string_to_null('lab_result_key') }} as lab_id
        , {{ empty_string_to_null('loinc_cd') }} as lab_code
        , {{ empty_string_to_null('vendor_loinc_cd') }} as service_code
        , TO_DATE(service_date, 'MM/DD/YY') as result_date
        , {{ empty_string_to_null('lab_results_value') }} as value_quantity
        , {{ empty_string_to_null('vendor_results_units_desc') }} as unit
        , ingest_date as ingest_timestamp_utc
    from lab_add_rownum
    where row_num = 1
)
select * from renamed