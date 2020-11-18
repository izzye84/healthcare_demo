{{
    config(
        materialized = 'ephemeral'
    )
}}

with

source as ( 
    select * from {{ source('reference_data','npi_taxonomy') }} 
),

renamed as (
        select
            {{ empty_string_to_null('medicare_specialty_code') }} as medicare_specialty_code
            ,{{ empty_string_to_null('medicare_provider_supplier_type_description') }} as specialty_code_display
            ,{{ empty_string_to_null('provider_taxonomy_code') }} as specialty_code
            ,{{ empty_string_to_null('provider_taxonomy_description') }} as specialty_code_text
        from source
)

select * from renamed