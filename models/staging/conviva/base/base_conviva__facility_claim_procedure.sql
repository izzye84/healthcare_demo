with source as (

    select *
    from {{ source('conviva_claims','facility_claim_procedure') }}
         {{ limit_dev_data() }}

),

add_row_num as (
    select row_number() over(partition by facility_header_claim_id,
                                          procedure_code
                             order by ingest_date desc) as row_num,
            *
    from source
),

source_renamed as (

    select facility_header_claim_id as identifier_claim_header,
           member_id,
           patient_id,
           procedure_code as code,
           procedure_modifier,
           coding_system as code_system,
           procedure_code_rank,
           client_id,
           ingest_date

    from add_row_num
    where row_num = 1

)

select *
from source_renamed
