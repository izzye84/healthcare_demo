with source as (

    select * from {{ source('ssm_claims','claim_detail') }} {{ limit_dev_data() }}

),

add_row_num as (
    select row_number() over(partition by claim_number,
                                          claim_line_number
                             order by ingest_date desc) as row_num,
            *
    from source
),

source_renamed as (

    select {{ empty_string_to_null('claim_number') }} as identifier_claim_header,
           {{ empty_string_to_null('claim_line_number') }} as identifier_claim_line,
           {{ empty_string_to_null('claim_sequence_number') }} as claim_sequence_number,
           {{ dbt_utils.surrogate_key(['patient_external_id',
                                       'lob',
                                       'insurance_name']) }} as patient,
           {{ empty_string_to_null('patient_external_id') }} as patient_external_id,
           {{ empty_string_to_null('subsciber_id') }} as subsciber_id,
           {{ empty_string_to_null('dependent_number') }} as dependent_number,
           {{ empty_string_to_null('place_of_service') }} as location,
           {{ empty_string_to_null('type_of_bill') }} as type_of_bill,
           service_date_time,
           date(claim_from_date_time) as serviced_period_start,
           date(claim_thru_date_time) as serviced_period_end,
           {{ empty_string_to_null('rev_code') }} as revenue,
           {{ empty_string_to_null('drg') }} as drg,
           {{ empty_string_to_null('first_name') }} as first_name,
           {{ empty_string_to_null('last_name') }} as last_name,
           {{ empty_string_to_null('gender') }} as gender,
           date_of_birth,
           {{ empty_string_to_null('cpt_hcpcs') }} as product_or_service,
           {{ empty_string_to_null('loinc') }} as loinc,
           {{ empty_string_to_null('snomed') }} as snomed,
           {{ empty_string_to_null('units') }}::numeric(18,2) as quantity,
           {{ empty_string_to_null('claim_adjustment_type_code') }} as claim_adjustment_type_code,
           {{ empty_string_to_null('claim_adjustment_sequence_number') }} as claim_adjustment_sequence_number,
           claim_adjustment_date_time,
           {{ empty_string_to_null('performing_npi') }} as performing_npi,
           allowed_amount,
           charge_amount,
           payment_date_time,
           plan_payment_amount as net,
           member_payment_amount,
           lob,
           insurance_name,
           client_id,
           ingest_date

        from add_row_num
        where row_num = 1

    )

    select *
    from source_renamed
