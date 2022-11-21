with

lab_base as (
    select * from {{ ref('base_conviva__lab') }}
),

mapped as (
    select {{ dbt_utils.surrogate_key(['identifier_external_source','identifier_encounter','lab_id','reason_code','code_source','code_source_display','value_quantity','value_string']) }} as identifier_observation
        ,identifier_external_source
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
            when split_part(reference_range_text,'-',2) is null then null
            when split_part(reference_range_text,'-',3) is null then split_part(reference_range_text,'-',1)
            else null
            end as reference_range_low

        ,case
            when split_part(reference_range_text,'-',2) is null then null
            when split_part(reference_range_text,'-',3) is null then split_part(reference_range_text,'-',2)
            else null
            end as reference_range_high

        ,reference_range_text
        ,effective_date
        ,effective_date as issued_date
        ,client_id
        ,ingest_date
    from lab_base
),

lab_concepts as (
  select
    identifier_observation
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

    ,case
      when (code_source_display like 'Creatinine%U%') then '10010-01'
      when (code_source_display like 'Albumin%U%') then '10002-01'
      when (code_source_display like 'Protein%Urine%') then '10011-01'
      when (code_source_display like '%GFR%' and value_quantity <> '0') then '10000-00'
      when (code_source_display in ('Alb/Creat Ratio', 'Albumin/Creatinine Ratio', 'Microalb/Creat Ratio', 'ALBUMIN/CREATININE RATIO, RANDOM URINE', 'MICROALBUMIN/CREATININE RATIO, RANDOM URINE', 'MICROALBUMIN/CREATININE$RATIO, RANDOM URINE')) then '10001-00'
      when (code_source_display in ('Protein/Creat Ratio', 'PROTEIN/CREATININE RATIO')) then '10012-00'
      when (code_source_display in ('ALBUMIN', 'Albumin', 'Albumin, Serum') and value_quantity_unit in ('gm/dL','g/dL')) then '10002-00'
      when (code_source_display in ('Protein', 'PROTEIN', 'Protein, Body Fluid', 'Protein, Total, Serum')) then '10011-00'
      when (code_source_display in ('Phosphorus', 'PHOSPHATE (AS PHOSPHORUS)')) then '10003-00'
      when (code_source_display in ('CALCIUM', 'Calcium', 'Calcium, Serum') and value_quantity_unit = 'mg/dL') then '10004-00'
      when (code_source_display in ('HEMOGLOBIN A1c', 'Hemoglobin A1c', 'Hb A1c Diabetic Assessment')) then '10005-00'
      when (code_source_display in ('Carbon Dioxide (CO2)', 'Carbon Dioxide, Total', 'CARBON DIOXIDE')) then '10009-00'
      when (code_source_display in ('LDL-CHOLESTEROL', 'LDL Chol Calc (NIH)', 'LDL Chol. (Direct)', 'LDL-C', 'LDL CHOLESTEROL', 'LDL Cholesterol Calc')) then '10007-00'
      when (code_source_display in ('Triglycerides', 'TRIGLYCERIDES')) then '10008-00'
      when (code_source_display in ('Fructosamine', 'FRUCTOSAMINE')) then '10013-00'
    end as code_some_company

    ,case
      when (code_source_display like 'Creatinine%U%') then 'Creatinine Urine'
      when (code_source_display like 'Albumin%U%') then 'Albumin Urine'
      when (code_source_display like 'Protein%Urine%') then 'Protein Urine'
      when (code_source_display like '%GFR%' and value_quantity <> '0') then 'eGFR'
      when (code_source_display in ('Alb/Creat Ratio', 'Albumin/Creatinine Ratio', 'Microalb/Creat Ratio', 'ALBUMIN/CREATININE RATIO, RANDOM URINE', 'MICROALBUMIN/CREATININE RATIO, RANDOM URINE', 'MICROALBUMIN/CREATININE$RATIO, RANDOM URINE')) then 'ACR'
      when (code_source_display in ('Protein/Creat Ratio', 'PROTEIN/CREATININE RATIO')) then 'PCR'
      when (code_source_display in ('ALBUMIN', 'Albumin', 'Albumin, Serum') and value_quantity_unit in ('gm/dL','g/dL')) then 'Albumin'
      when (code_source_display in ('Protein', 'PROTEIN', 'Protein, Body Fluid', 'Protein, Total, Serum')) then 'Protein'
      when (code_source_display in ('Phosphorus', 'PHOSPHATE (AS PHOSPHORUS)')) then 'Phosphorus'
      when (code_source_display in ('CALCIUM', 'Calcium', 'Calcium, Serum') and value_quantity_unit = 'mg/dL') then 'Calcium'
      when (code_source_display in ('HEMOGLOBIN A1c', 'Hemoglobin A1c', 'Hb A1c Diabetic Assessment')) then 'A1C'
      when (code_source_display in ('Carbon Dioxide (CO2)', 'Carbon Dioxide, Total', 'CARBON DIOXIDE')) then 'Carbon Dioxide'
      when (code_source_display in ('LDL-CHOLESTEROL', 'LDL Chol Calc (NIH)', 'LDL Chol. (Direct)', 'LDL-C', 'LDL CHOLESTEROL', 'LDL Cholesterol Calc')) then 'LDL Cholesterol'
      when (code_source_display in ('Triglycerides', 'TRIGLYCERIDES')) then 'Triglyceride'
      when (code_source_display in ('Fructosamine', 'FRUCTOSAMINE')) then 'Fructosamine'
    end as code_some_company_display

    ,'https://some_companyhealth.atlassian.net/l/c/08n1scXH' as code_some_company_system
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
    ,effective_date as issued_date
    ,client_id
    ,ingest_date
  from mapped
  where value_quantity is not null
  and effective_date is not null
  and (
    (code_source_display like 'Creatinine%U%') or
    (code_source_display like 'Albumin%U%') or
    (code_source_display like 'Protein%Urine%') or
    (code_source_display like '%GFR%' and value_quantity <> '0') or
    (code_source_display in ('Alb/Creat Ratio', 'Albumin/Creatinine Ratio', 'Microalb/Creat Ratio', 'ALBUMIN/CREATININE RATIO, RANDOM URINE', 'MICROALBUMIN/CREATININE RATIO, RANDOM URINE', 'MICROALBUMIN/CREATININE$RATIO, RANDOM URINE')) or
    (code_source_display in ('Protein/Creat Ratio', 'PROTEIN/CREATININE RATIO')) or
    (code_source_display in ('ALBUMIN', 'Albumin', 'Albumin, Serum') and value_quantity_unit in ('gm/dL','g/dL')) or
    (code_source_display in ('Protein', 'PROTEIN', 'Protein, Body Fluid', 'Protein, Total, Serum')) or
    (code_source_display in ('Phosphorus', 'PHOSPHATE (AS PHOSPHORUS)')) or
    (code_source_display in ('CALCIUM', 'Calcium', 'Calcium, Serum') and value_quantity_unit = 'mg/dL') or
    (code_source_display in ('HEMOGLOBIN A1c', 'Hemoglobin A1c', 'Hb A1c Diabetic Assessment')) or
    (code_source_display in ('Carbon Dioxide (CO2)', 'Carbon Dioxide, Total', 'CARBON DIOXIDE')) or
    (code_source_display in ('LDL-CHOLESTEROL', 'LDL Chol Calc (NIH)', 'LDL Chol. (Direct)', 'LDL-C', 'LDL CHOLESTEROL', 'LDL Cholesterol Calc')) or
    (code_source_display in ('Triglycerides', 'TRIGLYCERIDES')) or
    (code_source_display in ('Fructosamine', 'FRUCTOSAMINE'))
  )
)

select * from lab_concepts