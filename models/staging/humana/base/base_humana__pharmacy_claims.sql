{{ config(materialized = 'ephemeral') }}

with source as (

    select *
    from {{ source('humana_src', 'pharmacy_claims') }} {{ limit_dev_data() }}

),

add_row_num as (

  select row_number() over(partition by document_key
                           order by ingest_date desc) as row_num,
         *
  from source

),

source_renamed as (

    select {{ empty_string_to_null('aso_ind') }} as aso_ind,
           awp_tot_amt,
           service_date as billable_period,
           {{ empty_string_to_null('brand_name') }} as brand_name,
           {{ empty_string_to_null('claim_generic_ind') }} as claim_generic_ind,
           cob_coins_amt,
           cov_month,
           {{ empty_string_to_null('dea_id') }} as dea_id,
           ded_paid_amt,
           dispense_fee,
           {{ empty_string_to_null('drug_cov_status_cd') }} as drug_cov_status_cd,
           {{ empty_string_to_null('phys_npi_id') }} as facility,
           {{ empty_string_to_null('generic_name') }} as generic_name,
           {{ empty_string_to_null('icd9_diag_cd') }} as icd9_diag_cd,
           document_key as identifier_claim_header,
           {{ empty_string_to_null('mail_order_ind') }} as mail_order_ind,
           mbr_respons_amt,
           {{ empty_string_to_null('mco_contract_nbr') }} as mco_contract_nbr,
           {{ empty_string_to_null('multi_source_ind') }} as multi_source_ind,
           net_paid_amt,
           {{ empty_string_to_null('part_b_d_cd') }} as part_b_d_cd,
           {{ dbt_utils.surrogate_key(['pers_gen_key']) }} as patient,
           {{ empty_string_to_null('pay_day_supply_cnt') }} as pay_day_supply_cnt,
           {{ empty_string_to_null('payable_qty') }} as payable_qty,
           {{ empty_string_to_null('pbp_segment_id') }} as pbp_segment_id,
           {{ empty_string_to_null('pcs_group_id') }} as pcs_group_id,
           {{ empty_string_to_null('plan_ben_pkg_id') }} as plan_ben_pkg_id,
           process_date,
           {{ empty_string_to_null('phar_npi_id') }} as provider,
           {{ empty_string_to_null('refill_nbr') }} as refill_nbr,
           {{ empty_string_to_null('reversal_ind') }} as reversal_ind,
           rx_cost,
           rx_count,
           rx_ingrd_cost,
           rx_sales_tax_amt,
           {{ empty_string_to_null('specific_ther_class_cd') }} as specific_ther_class_cd,
           {{ empty_string_to_null('src_mbr_id') }} as src_mbr_id,
           {{ empty_string_to_null('src_platform_cd') }} as src_platform_cd,
           {{ empty_string_to_null('stc_ther_class_cd') }} as stc_ther_class_cd,
           {{ empty_string_to_null('ndc_id') }} as sub_type,
           bill_amt AS total,
           withhold_amt,
           client_id,
           ingest_date
    from add_row_num
    where row_num = 1
)

select *
from source_renamed
