with source as (

    select *
    from {{ source('ssm_claims', 'pharmacy_claim') }} 
    {{ limit_dev_data() }}

),

add_row_num as (

    select row_number() over(partition by claim_number
                             order by ingest_date desc) as row_num,
           *

    from source

),

source_renamed as (

    select claim_number AS identifier_claim_header,
           allowed_amount,
           date(fill_date_time) as billable_period,
           charge_amount,
           {{ empty_string_to_null('claim_adjustment_type_code') }} as claim_adjustment_type_code,
           {{ empty_string_to_null('claim_adjustment_sequence_number') }} as claim_adjustment_sequence_number,
           claim_adjustment_date_time,
           {{ empty_string_to_null('claim_sequence_number') }} as claim_sequence_number,
           date_of_birth,
           {{ empty_string_to_null('daw') }} as daw,
           days_supply,
           {{ empty_string_to_null('dependent_number') }} as dependent_number,
           {{ empty_string_to_null('dosage') }} as dosage,
           {{ empty_string_to_null('prescribing_npi') }} as facility,
           {{ empty_string_to_null('first_name') }} as first_name,
           {{ empty_string_to_null('formulary') }} as formulary,
           {{ empty_string_to_null('gender') }} as gender,
           {{ empty_string_to_null('generic') }} as generic,
           {{ empty_string_to_null('last_name') }} as last_name,
           member_payment_amount,
           {{ empty_string_to_null('metric_qty') }} as metric_qty,
           {{ dbt_utils.surrogate_key(['patient_external_id','lob','insurance_name']) }} as patient,
           patient_external_id,
           date(payment_date_time) AS payment_date_time,
           plan_payment_amount AS total,
           {{ empty_string_to_null('pharmacy_npi') }} AS provider,
           {{ empty_string_to_null('retail') }} as retail,
           {{ empty_string_to_null('rx_number') }} as rx_number,
           {{ empty_string_to_null('qty_units') }} as qty_units,
           {{ empty_string_to_null('subscriber_id') }} as subscriber_id,
           {{ empty_string_to_null('ndc_code') }} as sub_type,
           lob,
           insurance_name as insurer,
           client_id,
           ingest_date

    from add_row_num
    where row_num = 1

)

select *
from source_renamed
