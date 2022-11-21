with

vital_base as (
    select * from {{ ref('base_conviva__vital') }}
),

loinc_base as (
    select * from {{ ref('stg_reference_data__loinc') }}
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

map_base as (
    select 
        identifier_external_source
        ,identifier_encounter
        ,'vital_signs' as category_code
        ,'Vital Signs' as category_display
        ,'http://terminology.hl7.org/CodeSystem/observation-category' as category_system
        
        ,case
            when code_source like 'BMI%' then 'BMI'
            when code_source like '%BP%' then 'BP'
            when code_source like 'HC%' then 'HC'
            when code_source = 'HR' or code_source like '%Pulse%' then 'Pulse'
            when code_source like 'Ht%' then 'Ht'
            when code_source in ('Oxygen stat %','SaO2') then 'SaO2'
            when code_source like 'Wt%' then 'Wt'
            else code_source
        end as code_source
        
        ,case
            when code_source like '%BP%' then split_part(regexp_replace(split_part(value_quantity,',',1),'[A-Za-z:-]',''),' ',1)
            else value_quantity
        end as value_quantity

        ,case
            when value_quantity_unit = 'Ht-cm' then 'cm'
            when value_quantity_unit = 'Ht %' then '%'
            when value_quantity_unit = 'Wt-kg' then 'kg'
            when value_quantity_unit = 'Wt %' then '%'
            else value_quantity_unit
        end as value_quantity_unit

        ,created_date as effective_date
        ,created_date as issued_date
        ,client_id
        ,ingest_date
    from vital_base
),

/***********************************************************
 Splits systolic blood pressure values from source value
 and maps to LOINC
************************************************************/
vital_bp_sys as (
        select
            identifier_external_source
            ,identifier_encounter
            ,category_code
            ,category_display
            ,category_system
            ,code_source
            ,loinc_vital.loinc_num as code_some_company
            ,loinc_vital.long_common_name as code_some_company_display
            ,'http://loinc.org' as code_some_company_system
            ,split_part(value_quantity,'/',1) as value_quantity
            ,value_quantity_unit
            ,effective_date
            ,issued_date
            ,client_id
            ,ingest_date
        from map_base
            left join loinc_vital on 'BP Systolic' = loinc_vital.join_key
        where code_source = 'BP'
            and value_quantity is not null
),

/***********************************************************
 Splits diastolic blood pressure values from source value
 and maps to LOINC
************************************************************/
vital_bp_dia as (
        select
            identifier_external_source
            ,identifier_encounter
            ,category_code
            ,category_display
            ,category_system
            ,code_source
            ,loinc_vital.loinc_num as code_some_company
            ,loinc_vital.long_common_name as code_some_company_display
            ,'http://loinc.org' as code_some_company_system
            ,split_part(value_quantity,'/',2) as value_quantity
            ,value_quantity_unit
            ,effective_date
            ,issued_date
            ,client_id
            ,ingest_date
        from map_base
            left join loinc_vital on 'BP Diastolic' = loinc_vital.join_key
        where code_source = 'BP'
            and value_quantity is not null
),

vital_other as (
        select
            identifier_external_source
            ,identifier_encounter
            ,category_code
            ,category_display
            ,category_system
            ,code_source
            ,loinc_vital.loinc_num as code_some_company
            ,loinc_vital.long_common_name as code_some_company_display
            ,'http://loinc.org' as code_some_company_system
            ,value_quantity
            ,value_quantity_unit
            ,effective_date
            ,issued_date
            ,client_id
            ,ingest_date
        from map_base
            left join loinc_vital on map_base.code_source = loinc_vital.join_key
        where code_source <> 'BP'
            and value_quantity is not null
),

/***********************************************************
 Unions mapped vitals
 Creates surrogate keys
************************************************************/
unioned as (
    select * from vital_bp_sys

    union all

    select * from vital_bp_dia

    union all

    select * from vital_other
)

select distinct
    {{ dbt_utils.surrogate_key(['identifier_external_source','identifier_encounter','code_some_company','code_source','value_quantity','effective_date']) }} as identifier_observation
    ,identifier_external_source
    ,identifier_encounter
    ,category_code
    ,category_display
    ,category_system
    ,code_source
    ,code_some_company
    ,code_some_company_display
    ,code_some_company_system
    ,value_quantity
    ,value_quantity_unit
    ,effective_date
    ,issued_date
    ,client_id
    ,ingest_date
from unioned