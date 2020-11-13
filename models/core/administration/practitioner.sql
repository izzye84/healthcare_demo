{{
    config(
        dist = 'identifier_npi'
    )
}}

with

source as (
    select * from {{ ref('base_reference_data__npi') }}
)

select
    identifier_npi
    ,name_given_first
    ,name_given_middle
    ,name_family
    ,name_prefix
    ,name_suffix
    ,address_line1
    ,address_line2
    ,address_city
    ,address_state
    ,address_postal_code
    ,telecom_phone_number_work
    ,telecom_fax_number_work
    ,gender
from source
where entity_type_code = 1