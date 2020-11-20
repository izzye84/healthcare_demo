with

lab_base as (
    select * from {{ ref('base_humana__lab') }}
),

loinc_base as (
    select * from {{ ref('stg_reference_data__loinc') }}
),

loinc_concept_10000 as (
    select loinc_num
    from loinc_base
    where component like '%Glomerular filtration%'
),

loinc_concept_10001 as (
    select loinc_num
    from loinc_base
    where (component like 'Albumin/Creatinine%'
    or component like 'Microalbumin/Creatinine%')
),

loinc_concept_10002 as (
    select loinc_num
    from loinc_base
    where component = 'Albumin'
    and body_system in ('Ser', 'Ser/Plas', 'Ser/Plas/Bld', 'Ser/Bld', 'Ser+Plas', 'Ser+Bld', 'Bld')
),

loinc_concept_10003 as (
    select loinc_num
    from loinc_base
    where component = 'Phosphate'
    and body_system in ('Ser', 'Ser/Plas', 'Ser/Plas/Bld', 'Ser/Bld', 'Ser+Plas', 'Ser+Bld', 'Bld')
),

loinc_concept_10004 as (
    select loinc_num
    from loinc_base
    where component = 'Calcium'
    and body_system in ('Ser', 'Ser/Plas', 'Ser/Plas/Bld', 'Ser/Bld', 'Ser+Plas', 'Ser+Bld', 'Bld')
),

loinc_concept_10005 as (
    select loinc_num
    from loinc_base
    where component like 'Hemoglobin A1c%'
    and body_system in ('Ser', 'Ser/Plas', 'Ser/Plas/Bld', 'Ser/Bld', 'Ser+Plas', 'Ser+Bld', 'Bld')
),

loinc_concept_10006 as (
    select loinc_num
    from loinc_base
    where component = 'Carbon dioxide'
    and body_system in ('Ser', 'Ser/Plas', 'Ser/Plas/Bld', 'Ser/Bld', 'Ser+Plas', 'Ser+Bld', 'Bld')
),

loinc_concept_10007 as (
  select loinc_num
  from loinc_base
  where component = 'Cholesterol.in LDL'
  and body_system in ('Ser', 'Ser/Plas', 'Ser/Plas/Bld', 'Ser/Bld', 'Ser+Plas', 'Ser+Bld', 'Bld')

),

loinc_concept_10008 as (
    select loinc_num
    from loinc_base
    where component like 'Triglyceride'
    and body_system in ('Ser', 'Ser/Plas', 'Ser/Plas/Bld', 'Ser/Bld', 'Ser+Plas', 'Ser+Bld', 'Bld')
),

/***********************************************************
 Creates surrogate keys
 Splits result value into string and numeric, the order of
 the case statements is important!
************************************************************/
mapped as (
    select {{ dbt_utils.surrogate_key(['identifier_external_source','identifier_observation']) }} as identifier_observation
        ,null as identifier_order
        ,identifier_external_source
        ,null as identifier_encounter
        ,null as status_code
        ,null as status_display
        ,null as status_system
        ,'laboratory' as category_code
        ,'Laboratory' as category_display
        ,'http://terminology.hl7.org/CodeSystem/observation-category' as category_system
        ,code_source
        ,null as code_source_display
        ,null as code_source_system

        ,case when regexp_count(value_quantity, '^\\-?[0-9]\\d*(\\.\\d+)?$') > 0 then value_quantity
            else null
            end as value_quantity

        ,value_quantity_unit

        ,case when regexp_count(value_quantity, '^\\-?[0-9]\\d*(\\.\\d+)?$') = 0 then value_quantity
            else null
            end as value_string

        ,null as value_boolean
        ,null as interpretation_code
        ,null as interpretation_display
        ,null as interpretation_system
        ,null as reference_range_low
        ,null as reference_range_high
        ,null as reference_range_text
        ,effective_date
        ,effective_date as issued_date
        ,ingest_date
        ,client_id
    from lab_base
),

/***************************************************
 eGFR coded concept
****************************************************/
lab_concept_10000 as (
    select identifier_observation
        ,identifier_order
        ,identifier_external_source
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
        ,'10000' as code_strive
        ,'eGFR' as code_strive_display
        ,'https://strivehealth.atlassian.net/l/c/08n1scXH' as code_strive_system
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
    from mapped
    where code_source in (
        select loinc_num
        from loinc_concept_10000
        )
    and value_quantity is not null
),

/***************************************************
 ACR coded concept
****************************************************/
lab_concept_10001 as (
    select identifier_observation
        ,identifier_order
        ,identifier_external_source
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
        ,'10001' as code_strive
        ,'ACR' as code_strive_display
        ,'https://strivehealth.atlassian.net/l/c/08n1scXH' as code_strive_system
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
    from mapped
    where code_source in (
        select loinc_num
        from loinc_concept_10001
        )
    and value_quantity is not null
),

/***************************************************
 Albumin coded concept
****************************************************/
lab_concept_10002 as (
    select identifier_observation
        ,identifier_order
        ,identifier_external_source
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
        ,'10002' as code_strive
        ,'Albumin' as code_strive_display
        ,'https://strivehealth.atlassian.net/l/c/08n1scXH' as code_strive_system
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
    from mapped
    where code_source in (
        select loinc_num
        from loinc_concept_10002
        )
    and value_quantity is not null
),

/***************************************************
 Phosphorus coded concept
****************************************************/
lab_concept_10003 as (
    select identifier_observation
        ,identifier_order
        ,identifier_external_source
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
        ,'10003' as code_strive
        ,'Phosphorus' as code_strive_display
        ,'https://strivehealth.atlassian.net/l/c/08n1scXH' as code_strive_system
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
    from mapped
    where code_source in (
        select loinc_num
        from loinc_concept_10003
        )
    and value_quantity is not null
),

/***************************************************
 Calcium coded concept
****************************************************/
lab_concept_10004 as (
    select identifier_observation
        ,identifier_order
        ,identifier_external_source
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
        ,'10004' as code_strive
        ,'Calcium' as code_strive_display
        ,'https://strivehealth.atlassian.net/l/c/08n1scXH' as code_strive_system
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
    from mapped
    where code_source in (
        select loinc_num
        from loinc_concept_10004
        )
    and value_quantity is not null
),

/***************************************************
 A1C coded concept
****************************************************/
lab_concept_10005 as (
    select identifier_observation
        ,identifier_order
        ,identifier_external_source
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
        ,'10005' as code_strive
        ,'A1C' as code_strive_display
        ,'https://strivehealth.atlassian.net/l/c/08n1scXH' as code_strive_system
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
    from mapped
    where code_source in (
        select loinc_num
        from loinc_concept_10005
        )
    and value_quantity is not null
),

/***************************************************
 BiCarbonate coded concept
****************************************************/
lab_concept_10006 as (
    select identifier_observation
        ,identifier_order
        ,identifier_external_source
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
        ,'10006' as code_strive
        ,'BiCarbonate' as code_strive_display
        ,'https://strivehealth.atlassian.net/l/c/08n1scXH' as code_strive_system
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
    from mapped
    where code_source in (
        select loinc_num
        from loinc_concept_10006
        )
    and value_quantity is not null
),

/***************************************************
 LDL Cholesterol coded concept
****************************************************/
lab_concept_10007 as (
    select identifier_observation
        ,identifier_order
        ,identifier_external_source
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
        ,'10007' as code_strive
        ,'LDL Cholesterol' as code_strive_display
        ,'https://strivehealth.atlassian.net/l/c/08n1scXH' as code_strive_system
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
    from mapped
    where code_source in (
        select loinc_num
        from loinc_concept_10007
        )
    and value_quantity is not null
),

/***************************************************
 Triglyceride coded concept
****************************************************/
lab_concept_10008 as (
    select identifier_observation
        ,identifier_order
        ,identifier_external_source
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
        ,'10008' as code_strive
        ,'Triglyceride' as code_strive_display
        ,'https://strivehealth.atlassian.net/l/c/08n1scXH' as code_strive_system
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
    from mapped
    where code_source in (
        select loinc_num
        from loinc_concept_10008
        )
    and value_quantity is not null
)

select * from lab_concept_10000

union all

select * from lab_concept_10001

union all

select * from lab_concept_10002

union all

select * from lab_concept_10003

union all

select * from lab_concept_10004

union all

select * from lab_concept_10005

union all

select * from lab_concept_10006

union all

select * from lab_concept_10007

union all

select * from lab_concept_10008