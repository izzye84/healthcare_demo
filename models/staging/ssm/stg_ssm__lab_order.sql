with

lab_base as (
    select * from {{ ref('base_ssm__lab') }}
),

lab_order_add_rownum as (
    select row_number() over(partition by patient_account_number, identifier_encounter, lab_id, reason_code, reason_display order by ingest_date) as row_num
        ,*
    from lab_base
),

lab_order_dedup as (
    select *
    from lab_order_add_rownum
    where row_num = 1
),

crosswalk_base as (
    select * from {{ ref('base_ssm__crosswalk') }}
),

joined as (
    select distinct crosswalk_base.person_id
        ,crosswalk_base.lob
        ,crosswalk_base.insurance_name
        ,lab_order_dedup.identifier_encounter
        ,lab_order_dedup.lab_id
        ,lab_order_dedup.reason_code
        ,lab_order_dedup.reason_display
        ,lab_order_dedup.reason_system
        ,lab_order_dedup.detail
        ,lab_order_dedup.order_date
        ,lab_order_dedup.ingest_date
        ,lab_order_dedup.client_id
    from lab_order_dedup inner join crosswalk_base
        on lab_order_dedup.patient_account_number = crosswalk_base.enterprise_mrn
)

select {{ dbt_utils.surrogate_key(['person_id','lob','insurance_name','identifier_encounter','lab_id','reason_code','reason_display']) }} as identifier_order
    ,{{ dbt_utils.surrogate_key(['person_id','lob','insurance_name'])}} as identifier_external_source
    ,reason_code
    ,reason_display
    ,reason_system
    ,detail
    ,order_date
    ,ingest_date
    ,client_id
from joined