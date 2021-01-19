with source as (

    select *
    from {{ source('conviva_claims','facility_claim_diagnosis') }}
         {{ limit_dev_data() }}
         
),

add_row_num as (

    select row_number() over(partition by facility_header_claim_id, patient_id, icd_diagnosis_code
                             order by ingest_date desc) as row_num,
           *
    from source

),

source_renamed as (

    select facility_header_claim_id as identifier_claim_header,
            {{ empty_string_to_null('member_id') }} as member_id,
            {{ empty_string_to_null('patient_id') }} as patient_id,
            {{ empty_string_to_null('icd_diagnosis_code') }} as code,
            {{ empty_string_to_null('icd_indicator') }} as code_system,
            client_id,
            payer_lob,
            ingest_date
    from add_row_num
    where row_num = 1

)

select *
from source_renamed
