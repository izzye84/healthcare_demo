{{
    config(
        dist = 'identifier_external_source'
    )
}}

with

clinical_diagnoses as (
    select * from {{ ref('int_condition__unioned') }}
)

select * from clinical_diagnoses
