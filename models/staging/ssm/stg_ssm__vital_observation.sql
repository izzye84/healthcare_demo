with

vital_base as (
    select * from {{ ref('base_ssm__vital') }}
),

crosswalk_base as (
    select * from {{ ref('base_ssm__crosswalk') }}
),

loinc_base as (
    select *
    from {{ ref('stg_reference_data__loinc') }}
),

loinc_vital as (
    select loinc_num
        ,long_common_name
        ,case loinc_num
            when '39156-5' then 'BMI'
            when '8287-5' then 'HC'
            when '8302-2' then 'Ht'
            when '8867-4' then 'Pulse'
            when '9279-1' then 'RR'
            when '2708-6' then 'SaO2'
            when '8310-5' then 'Temp'
            when '29463-7' then 'Wt'
            when '8480-6' then 'BP Systolic'
            when '8462-4' then 'BP Diastolic'
            end as join_key
    from loinc_base
    where loinc_num in (
        '39156-5',
        '8480-6',
        '8462-4',
        '8287-5',
        '8302-2',
        '8867-4',
        '9279-1',
        '2708-6',
        '8310-5',
        '29463-7'
        )
),

joined as (
    select crosswalk_base.person_id
        ,crosswalk_base.lob
        ,crosswalk_base.insurance_name
        ,vital_base.identifier_encounter
        ,vital_base.code_source
        ,vital_base.value_quantity
        ,vital_base.value_quantity_unit
        ,vital_base.created_timestamp
        ,vital_base.ingest_date
        ,vital_base.client_id
    from vital_base inner join crosswalk_base
        on vital_base.patient_account_number = crosswalk_base.enterprise_mrn
),

/***********************************************************
 Creates base table for mapping logic
 Adds missing fields
************************************************************/
map_base as (
    select person_id
        ,lob
        ,insurance_name
        ,null as identifier_order
        ,identifier_encounter
        ,null as status_code
        ,null as status_display
        ,null as status_system
        ,'vital-signs' as category_code
        ,'Vital Signs' as category_display
        ,'http://terminology.hl7.org/CodeSystem/observation-category' as category_system
        ,code_source
        ,null as code_source_display
        ,null as code_source_system
        ,value_quantity
        ,value_quantity_unit
        ,null as value_string
        ,null as value_boolean
        ,null as interpretation_code
        ,null as interpretation_display
        ,null as interpretation_system
        ,null as reference_range_low
        ,null as reference_range_high
        ,null as reference_range_text
        ,date(created_timestamp) as effective_date
        ,date(created_timestamp) as issued_date
        ,ingest_date
        ,client_id
    from joined
),

/***********************************************************
 Splits systolic blood pressure values from source value
 and maps to LOINC
 Creates surrogate keys
************************************************************/
vital_bp_sys as (
    select person_id
        ,lob
        ,insurance_name
        ,identifier_order
        ,identifier_encounter
        ,status_code
        ,status_display
        ,status_system
        ,category_code
        ,category_display
        ,category_system
        ,code_source
        ,code_source_display
        ,code_source_system
        ,loinc_vital.loinc_num as code_some_company
        ,loinc_vital.long_common_name as code_some_company_display
        ,'http://loinc.org' as code_some_company_system
        ,split_part(value_quantity,'/',1) as value_quantity
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
        ,ingest_date
        ,client_id
    from map_base left join loinc_vital
        on 'BP Systolic' = loinc_vital.join_key
    where code_source = 'BP'
        and value_quantity is not null
),

/***********************************************************
 Splits diastolic blood pressure values from source value
 and maps to LOINC
 Creates surrogate keys
************************************************************/
vital_bp_dia as (
    select person_id
        ,lob
        ,insurance_name
        ,identifier_order
        ,identifier_encounter
        ,status_code
        ,status_display
        ,status_system
        ,category_code
        ,category_display
        ,category_system
        ,code_source
        ,code_source_display
        ,code_source_system
        ,loinc_vital.loinc_num as code_some_company
        ,loinc_vital.long_common_name as code_some_company_display
        ,'http://loinc.org' as code_some_company_system
        ,split_part(value_quantity,'/',2) as value_quantity
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
        ,ingest_date
        ,client_id
    from map_base left join loinc_vital
        on 'BP Diastolic' = loinc_vital.join_key
    where code_source = 'BP'
        and value_quantity is not null
),

/***********************************************************
 Maps all other vitals to LOINC
 Creates surrogate keys
************************************************************/
vital_other as (
    select person_id
        ,lob
        ,insurance_name
        ,identifier_order
        ,identifier_encounter
        ,status_code
        ,status_display
        ,status_system
        ,category_code
        ,category_display
        ,category_system
        ,code_source
        ,code_source_display
        ,code_source_system
        ,loinc_vital.loinc_num as code_some_company
        ,loinc_vital.long_common_name as code_some_company_display
        ,'http://loinc.org' as code_some_company_system
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
        ,ingest_date
        ,client_id
    from map_base left join loinc_vital
        on map_base.code_source = loinc_vital.join_key
    where code_source <> 'BP'
        and value_quantity is not null
)

/***********************************************************
 Unions mapped vitals
 Creates surrogate keys
************************************************************/
select {{ dbt_utils.surrogate_key(['person_id','lob','insurance_name','identifier_encounter','code_some_company','value_quantity']) }} as identifier_observation
        ,identifier_order
        ,{{ dbt_utils.surrogate_key(['person_id','lob','insurance_name'])}} as identifier_external_source
        ,identifier_encounter
        ,status_code
        ,status_display
        ,status_system
        ,category_code
        ,category_display
        ,category_system
        ,code_source
        ,code_source_display
        ,code_source_system
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
        ,ingest_date
        ,client_id
from (select * from vital_bp_sys

    union all

    select * from vital_bp_dia

    union all

    select * from vital_other
) as unioned