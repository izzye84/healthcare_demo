{{
    config(
        dist = 'practitioner',
        sort = 'practitioner'
    )
}}

with

source as (
    select * from {{ ref('stg_reference_data__practitioner_role') }}
)

select * from source