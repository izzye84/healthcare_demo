{{ config(dist = 'subject') }}

with all_client_encounters as (

    select *
    from {{ ref('int_encounters__unioned') }}

)

select 
    identifier_encounter
    ,identifier_external_encounter
    ,hospitalization_admit_source
    ,hospitalization_discharge_disposition_code
    ,status
    ,type
    ,service_type_code
    ,subject
    ,period
    ,reason_code_text
    ,participant_individual
    ,client_id
    ,ingest_date
from all_client_encounters
