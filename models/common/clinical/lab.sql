with lab as {{
    dbt_utils.union_relations(
        relations=[
            ref('stg_humana_common__lab')
        ]
    )
}}


select sh_lab_item_id
, patient_id
, patient_account_number
, clinical_encounter_id
, lab_id
, lab_code_set
, lab_code
, lab_order_date
, lab_order_description
, service_code
, service_code_modifier
, service_code_type
, service_code_desc
, result_description
, collection_date
, result_date
, result_text
, result_numeric
, result_pos_neg
, unit
, reference_range
, abnormal_flag
, result_status
, ingest_timestamp_utc
, created_timestamp
, modified_timestamp
from lab