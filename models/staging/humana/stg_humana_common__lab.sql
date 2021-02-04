with

lab as (
    select * from {{ ref('base_humana_common__lab') }}
)

select {{ dbt_utils.surrogate_key(['patient_id','lab_id']) }} as sh_lab_item_id
    , patient_id 
    , NULL as patient_account_number
    , NULL as clinical_encounter_id
    , lab_id
    , 'LOINC' AS lab_code_set
    , lab_code
    , NULL as lab_order_date
    , NULL as lab_order_description
    , service_code
    , NULL as service_code_modifier
    , NULL as service_code_type
    , NULL as service_code_desc
    , NULL as result_description
    , NULL as collection_date
    , result_date 
    , case when regexp_count(value_quantity, '^\\-?[0-9]\\d*(\\.\\d+)?$') = 0 then trim(value_quantity::varchar(255))
    else null
    end as result_text
    , case when regexp_count(value_quantity, '^\\-?[0-9]\\d*(\\.\\d+)?$') > 0 then value_quantity::decimal(38,2)
        else null
      end as result_numeric
    , NULL as result_pos_neg
    , unit
    , NULL as reference_range
    , NULL as abnormal_flag
    , NULL as result_status
    , ingest_timestamp_utc
    , NULL as created_timestamp
    , NULL as modified_timestamp
from lab