{{ config(dist = 'feature_link') }}

with humana_disease_stage as (

    select identifier_external_source as feature_link,
           '100006' as feature_code,
           'Disease Stage' as feature_display,
           disease_stage as feature_value,
           ingest_date as feature_date,
           client_id

  	from {{ ref('stg_humana__disease_stage') }}

),

-- Determining how many dialysis procedures a patient has had
ssm_dialysis_procedure_cnt as (

    select identifier_external_source,
           count(code) as dialysis_cnt

    from {{ ref('stg_ssm__claim_procedures') }}
    where performed_datetime between date_add('mon', -100, getdate()) and
                                     getdate() and
          code in (select cpt_code :: varchar as cpt_code
                   from {{ ref('seed_dialysis_cpt_codes') }})

    group by identifier_external_source

),

-- calculating latest disease stage by patient and by diagnosis code
ssm_latest_diagnoses as (

    select distinct identifier_external_source,
                    code,
                    max(recorded_date) over (partition by identifier_external_source,
                                                          code) as latest_diagnosis_date,
                    client_id

    from {{ ref('stg_ssm__claim_diagnoses') }}
    where recorded_date between date_add('mon', -36, getdate()) and
                                getdate()

),

-- adding dialysis counts to each patient to help determine ESRD statuses
adding_dialysis_cnt as (

    select ssm_latest_diagnoses.identifier_external_source,
           ssm_latest_diagnoses.code,
           ssm_latest_diagnoses.latest_diagnosis_date,
           ssm_latest_diagnoses.client_id,
  	       coalesce(ssm_dialysis_procedure_cnt.dialysis_cnt, 0) as dialysis_cnt

    from ssm_latest_diagnoses
    left join ssm_dialysis_procedure_cnt
  		   on ssm_latest_diagnoses.identifier_external_source = ssm_dialysis_procedure_cnt.identifier_external_source

),

calculate_ssm_disease_stage as (

    select identifier_external_source,
           code,
           case when code = 'N186' AND
                     dialysis_cnt > 0
                then 'ESRD'

                when code = 'N186'
                then 'ESRD No Dialysis'

                when code = 'N185'
                then 'CKD 5'

                when code = 'N184'
  						  then 'CKD 4'

                when code = 'N183'
  						  then 'CKD 3'

                when code = 'N182'
  						  then 'CKD 2'

                when code = 'N181'
  						  then 'CKD 1'

  						  when code = 'N189'
                then 'CKD UNSPECIFIED'

  						  else 'None'
  				 end as disease_stage,
           case when code = 'N186' and
                     dialysis_cnt > 0
                then 7

                when code = 'N186'
                then 6

                when code = 'N185'
                then 5

                when code = 'N184'
  						  then 4

                when code = 'N183'
  						  then 3

                when code = 'N182'
  						  then 2

                when code = 'N181'
  						  then 1

  						  when code = 'N189'
                then 0

  				      else -1
  		     end as disease_stage_rank,
           latest_diagnosis_date,
           client_id,
  		     dialysis_cnt

    from adding_dialysis_cnt

),

-- using row_number() to determine each patient's max disease stage
max_disease_stage as (

    select identifier_external_source,
           disease_stage,
           latest_diagnosis_date,
           row_number() over (partition by identifier_external_source
                              order by disease_stage_rank desc,
                                       latest_diagnosis_date desc) as max_disease_stage,
           client_id

    from calculate_ssm_disease_stage

),

ssm_disease_stage as (

    select identifier_external_source as feature_link,
           '100006' as feature_code,
           'Disease Stage' as feature_display,
           disease_stage as feature_value,
           latest_diagnosis_date as feature_date,
           client_id

    from max_disease_stage
    where max_disease_stage = 1

),

humana_and_ssm_combined as (

    select *
    from humana_disease_stage

    union all

    select *
    from ssm_disease_stage

)

select *
from humana_and_ssm_combined
