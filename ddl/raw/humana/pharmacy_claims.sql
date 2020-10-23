create external table raw_humana.pharmacy_claims (
    aso_ind varchar(255),
    awp_tot_amt numeric(18,2),
    bill_amt numeric(18,2),
    brand_name varchar(255),
    claim_generic_ind varchar(255),
    cob_coins_amt numeric(18,2),
    dea_id varchar(255),
    ded_paid_amt numeric(18,2),
    dispense_fee numeric(18,2),
    document_key varchar(255),
    drug_cov_status_cd varchar(255),
    generic_name varchar(255),
    icd9_diag_cd varchar(255),
    cov_month varchar(255),  
    mail_order_ind varchar(255),
    mbr_respons_amt numeric(18,2),
    mco_contract_nbr varchar(255),
    multi_source_ind varchar(255),
    ndc_id varchar(255),
    net_paid_amt numeric(18,2),
    part_b_d_cd varchar(255),
    pay_day_supply_cnt varchar(255),
    payable_qty varchar(255),
    pbp_segment_id varchar(255),
    pcs_group_id varchar(255),
    pers_gen_key varchar(255),
    phar_npi_id varchar(255),
    phys_npi_id varchar(255),
    plan_ben_pkg_id varchar(255),
    process_date varchar(255),
    refill_nbr varchar(255),
    reversal_ind varchar(255),
    rx_cost numeric(18,2),
    rx_count numeric(18,0),
    rx_ingrd_cost numeric(18,2),
    rx_sales_tax_amt numeric(18,2),
    service_date varchar(255),
    specific_ther_class_cd varchar(255),
    src_mbr_id varchar(255),
    src_platform_cd varchar(255),
    stc_ther_class_cd varchar(255),
    withhold_amt numeric(18,2)
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
stored as textfile
location 's3://strive-analytics-warehouse-pro/clients/client_id=humana/data_frequency=batch/pharmacy_claims/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_humana.pharmacy_claims add
partition(client_id='humana', ingest_date='2020-06-15') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=humana/data_frequency=batch/pharmacy_claims/ingest_date=2020-06-15/';