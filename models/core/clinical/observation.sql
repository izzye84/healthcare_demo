{{
    config(
        dist = 'identifier_external_source'
    )
}}

with observation as (
    select * from {{ ref('observation__unioned') }}
)

select * from observation