with

lab_base as (
    select * from {{ ref('base_ssm__lab') }}
),

crosswalk_base as (
    select * from {{ ref('base_ssm__crosswalk') }}

),

joined as (
    select crosswalk_base.person_id
        ,crosswalk_base.lob
        ,crosswalk_base.insurance_name
        ,lab_base.identifier_encounter
        ,lab_base.lab_id
        ,lab_base.reason_code
        ,lab_base.reason_display
        ,lab_base.code_source
        ,lab_base.code_source_display
        ,lab_base.code_source_system
        ,lab_base.value_quantity
        ,lab_base.value_quantity_unit
        ,lab_base.value_string
        ,lab_base.value_boolean
        ,lab_base.reference_range_text
        ,lab_base.interpretation_code
        ,lab_base.status_code
        ,lab_base.effective_date
        ,lab_base.ingest_date
        ,lab_base.client_id
    from lab_base inner join crosswalk_base
        on lab_base.patient_account_number = crosswalk_base.enterprise_mrn
),

/***********************************************************
 Creates surrogate keys
 Maps codes for status, category, and interpretation
 Casts values for value_quantity abd value_boolean
 Parses reference_range_text to low and high with exceptions
************************************************************/
mapped as (
    select {{ dbt_utils.surrogate_key(['person_id','lob','insurance_name','identifier_encounter','lab_id','code_source','code_source_display','value_quantity','value_string']) }} as identifier_observation
        ,{{ dbt_utils.surrogate_key(['person_id','lob','insurance_name','identifier_encounter','lab_id','reason_code','reason_display']) }} as identifier_order
        ,{{ dbt_utils.surrogate_key(['person_id','lob','insurance_name'])}} as identifier_external_source
        ,identifier_encounter

        ,case status_code
            when 'P' then 'preliminary'
            when 'F' then 'final'
            when 'V' then 'final'
            when 'A' then 'amended'
            when 'C' then 'corrected'
            when 'W' then 'entered-in-error'
            else 'unknown'
            end as status_code

        ,case status_code
            when 'P' then 'Preliminary'
            when 'F' then 'Final'
            when 'V' then 'Final'
            when 'A' then 'Amended'
            when 'C' then 'Corrected'
            when 'W' then 'Entered in Error'
            else 'Unknown'
            end as status_display

        ,'http://hl7.org/fhir/observation-status' as status_system
        ,'laboratory' as category_code
        ,'Laboratory' as category_display
        ,'http://terminology.hl7.org/CodeSystem/observation-category' as category_system
        ,code_source
        ,code_source_display
        ,code_source_system
        ,value_quantity
        ,value_quantity_unit
        ,value_string
        ,value_boolean
        ,interpretation_code

        ,case interpretation_code
            when 'L' then 'Low'
            when 'H' then 'High'
            when 'LL' then 'Critical low'
            when 'HH' then 'Critical high'
            when '<' then 'Off scale low'
            when '>' then 'Off scale high'
            when 'N' then 'Normal'
            when 'A' then 'Abnormal'
            when 'AA' then 'Critical abnormal'
            when 'U' then 'Significant change up'
            when 'D' then 'Significant change down'
            when 'B' then 'Better'
            when 'W' then 'Worse'
            when 'S' then 'Susceptible'
            when 'MS' then 'Susceptible'
            when 'VS' then 'Susceptible'
            when 'R' then 'Resistant'
            when 'I' then 'Intermediate'
            when 'POS' then 'Positive'
            when 'NEG' then 'Negative'
            when 'IND' then 'Indeterminate'
            when 'DET' then 'Detected'
            when 'ND' then 'Not detected'
            when 'AC' then 'Anti-complementary substances present'
            when 'TOX' then 'Cytotoxic substance present'
            when 'QCF' then 'Quality Control Failure'
            when 'RR' then 'Reactive'
            when 'WR' then 'Weakly reactive'
            when 'NR' then 'Non-reactive'
            when 'HM' then 'Hold for Medical Review'
            else null
            end as interpretation_display

        ,'http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation' as interpretation_system

        ,case
            when split_part(reference_range_text,'-',3) is null then split_part(reference_range_text,'-',1)
            else null
            end as reference_range_low

        ,case
            when split_part(reference_range_text,'-',3) is null then split_part(reference_range_text,'-',2)
            else null
            end as reference_range_high

        ,reference_range_text
        ,effective_date
        ,effective_date as issued_date
        ,ingest_date
        ,client_id
    from joined
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
    where code_source_display like '%GFR%'
        and value_quantity not in ('9999999', '0')
        and value_quantity is not null
        and value_quantity not like '%.%'
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
    where code_source_display like 'MICROALB%CREAT%'
        and value_quantity not in ('9999999')
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
    where code_source_display like 'ALBUMIN'
        and value_quantity not in ('9999999')
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
    where code_source_display like 'PHOSPHORUS'
        and value_quantity not in ('9999999')
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
    where code_source_display like 'CALCIUM'
        and value_quantity not in ('9999999')
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
    where code_source_display like '%A1C%'
        and value_quantity not in ('9999999')
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
    where code_source_display like 'CO2'
        and value_quantity not in ('9999999')
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
    where code_source_display in ('LDL', 'LDL EXTERNAL', 'LDL CALCULATED EXTERNAL', 'LDL CALCULATED', 'LDL DIRECT', 'LDL DIRECT EXTERNAL')
        and value_quantity not in ('9999999')
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
    where code_source_display in ('TRIGLYCERIDES', 'TRIGLYCERIDES EXTERNAL', 'TRIGLYCERIDES FLD')
        and value_quantity not in ('9999999')
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