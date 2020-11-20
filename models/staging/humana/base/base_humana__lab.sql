{{
    config(
        materialized = 'ephemeral'
    )
}}

with

lab_source as (
    select * from {{ source('humana_src','lab_claims') }}
),

lab_add_rownum as (
    select row_number() over(partition by pers_gen_key, lab_result_key order by ingest_date desc) as row_num
        ,*
    from lab_source
),

lab_dedup as (
    select *
    from lab_add_rownum
    where row_num = 1
),

renamed as (
    select {{ dbt_utils.surrogate_key(['lab_result_key']) }} as identifier_observation
        ,{{ dbt_utils.surrogate_key(['pers_gen_key']) }} as identifier_external_source
        ,{{ empty_string_to_null('src_mbr_id') }} as src_mbr_id
        ,{{ empty_string_to_null('src_platform_cd') }} as src_platform_cd
        ,{{ empty_string_to_null('loinc_cd') }} as code_source
        ,{{ empty_string_to_null('vendor_loinc_cd') }} as vendor_loinc_cd
        ,{{ empty_string_to_null('lab_results_value') }} as value_quantity
        ,{{ empty_string_to_null('vendor_results_units_desc') }} as value_quantity_unit
        ,date(service_date) as effective_date
        ,{{ empty_string_to_null('cov_month') }} as cov_month
        ,{{ empty_string_to_null('client_id') }} as client_id
        ,ingest_date as ingest_date
    from lab_dedup
)

select * from renamed