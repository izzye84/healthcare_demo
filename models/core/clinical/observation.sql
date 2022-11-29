with observation as (
    select * from {{ ref('int_observation__unioned') }}
)

select 
    identifier_observation
    ,identifier_order
    ,identifier_external_source
    ,identifier_encounter
    ,status_code
    ,status_display
    ,status_system
    ,category_code
    ,category_display
    ,category_system
    ,code_some_company
    ,code_some_company_display
    ,code_some_company_system
    ,value_quantity
    ,value_quantity_unit
    ,value_string
    ,value_boolean
    ,interpretation_code
    ,interpretation_display
    ,interpretation_system
    ,reference_range_low
    ,reference_range_high
    ,reference_range_text
    ,effective_date
    ,issued_date
    ,client_id
    ,ingest_date
from observation