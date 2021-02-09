with medical_claims as (

    select *
    from {{ ref('base_humana_common__medical_claims') }}

),

claim_line as (

    select {{ dbt_utils.surrogate_key(['line_number',
                                       'client_id']) }} as sh_line_number,
           claim_id,
           row_number() over (partition by claim_id
                              order by line_number) as line_number,
           member_id,
           null as patient_id,
           billing_provider_npi,
           null as servicing_provider_npi,
           revenue_code,
           procedure_code,
           procedure_modifier_1,
           procedure_modifier_2,
           null as procedure_modifier_3,
           null as procedure_modifier_4,
           ndc_code,
           serv_from_date as service_date_from,
           serv_to_date as service_date_to,
           service_units,
           total_charges as charge_amount,
           member_liability + plan_payment as total_paid,
           allowed_amount,
           plan_payment,
           member_liability,
           null as standard_cost,
           'Facility' as claim_type,
           transaction_type,
           client_id,
           ingest_timestamp_utc

    from medical_claims

)

select *
from claim_line
