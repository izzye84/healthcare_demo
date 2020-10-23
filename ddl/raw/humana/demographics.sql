create external table raw_humana.demographics (
    age varchar(255), 
    birth_date varchar(255), 
    cov_month varchar(255),  
    ctrct_cat_cd varchar(255),
    esrd_ind varchar(255),
    grpr_comp_name varchar(255),
    grpr_id varchar(255),
    hospice_ind varchar(255),
    lob varchar(255),
    mco_contract_nbr varchar(255),
    medicaid_dual_status_cd varchar(255),
    orig_reas_entitle_cd varchar(255),
    pbp_segment_id varchar(255),
    pcp_assignment varchar(255),
    pers_gen_key varchar(255),
    plan_benefit_package_id varchar(255),
    product_type varchar(255),
    recon_ma_risk_score_nbr varchar(255),
    sex_cd varchar(255),
    snp_plans varchar(255),
    src_mbr_id varchar(255),
    src_platform_cd varchar(255),
    src_subs_mbr_id varchar(255),
    state_of_residence varchar(255),
    std_rptg_st varchar(255),
    decsd_date varchar(255), 
    cov_eff_date varchar(255), 
    cov_end_date varchar(255), 
    home_phone_nbr varchar(255),
    pers_first_name varchar(255),
    pers_last_name varchar(255)
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
stored as textfile
location 's3://strive-analytics-warehouse-pro/clients/client_id=humana/data_frequency=batch/demographics/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_humana.demographics add
partition(client_id='humana', ingest_date='2020-06-15') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=humana/data_frequency=batch/demographics/ingest_date=2020-06-15/';