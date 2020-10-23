with

patient_demographics as (
    select * from {{ ref('base_humana__demographics') }}
)

select identifier_external_source
    ,deceased_date
    ,client_id
    ,ingest_date
from patient_demographics