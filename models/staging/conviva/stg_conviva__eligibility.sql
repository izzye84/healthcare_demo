with

source as (
    select *
    from {{ source('conviva_claims', 'eligibility') }}
),

add_row_num as (
    select
        row_number() over(partition by patient_id order by ingest_date desc, coverage_begin_date desc) as row_num
        ,patient_id
        ,coverage_begin_date
        ,coverage_end_date
        ,contract_number_type_indicator
        ,contract_number
        ,group_id
        ,hospice_begin_date
        ,hospice_end_date
        ,insurance_type
        ,payer
        ,plan_code
        ,original_entitlement_reason
        ,dual_status_indicator
        ,patient_account_number
        ,client_id
        ,payer_lob
        ,ingest_date
    from source
),

renamed as (
    select
        {{ dbt_utils.surrogate_key(['patient_id','coverage_begin_date']) }} as identifier
        ,{{ dbt_utils.surrogate_key(['patient_id']) }} as identifier_external_source
        ,date(coverage_begin_date) as period_coverage_start
        ,date(coverage_end_date) as period_coverage_end
        ,{{ empty_string_to_null('patient_id') }} as subscriber_id
        ,{{ empty_string_to_null('insurance_type') }} as network
        ,{{ empty_string_to_null('payer') }} as payor
        ,{{ empty_string_to_null('client_id') }} as client_id
        ,ingest_date
    from add_row_num
    where row_num = 1
),

add_status as (
    select
        identifier
        ,identifier_external_source
        
        ,case
            when period_coverage_end >= date(getdate()) then 'active'
            else 'cancelled'
        end as status
        
        ,period_coverage_start
        ,period_coverage_end
        ,subscriber_id
        ,network
        ,payor
        ,client_id
        ,ingest_date
    from renamed
)

select * from add_status