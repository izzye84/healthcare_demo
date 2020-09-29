create external table raw_humana.medical_claims (
    admit_diag_cd varchar(255), 
    admit_src_cd varchar(255),  
    allow_icob_admit_cnt varchar(255),
    allow_icob_amt varchar(255),
    allow_icob_days_cnt varchar(255),
    allow_icob_visits_cnt varchar(255),
    bill_type_cd varchar(255),
    chrg_amt varchar(255), 
    chrg_days_cnt varchar(255),
    clm_aso_ind varchar(255),
    cob_paid_amt varchar(255),
    copay_amt varchar(255),
    cpt_mod_cd varchar(255),
    cpt_mod_cd2 varchar(255),
    ctrct_cat_cd varchar(255),
    deduct_amt varchar(255),
    primary_diag_cd varchar(255),
    diag_cd2 varchar(255),
    diag_cd3 varchar(255),
    diag_cd4 varchar(255),
    diag_cd5 varchar(255),
    diag_cd6 varchar(255),
    diag_cd7 varchar(255),
    diag_cd8 varchar(255),
    diag_cd9 varchar(255),
    diag_cd10 varchar(255),
    diag_cd11 varchar(255),
    diag_cd12 varchar(255),
    diag_cd13 varchar(255),
    diag_cd14 varchar(255),
    diag_cd15 varchar(255),
    diag_cd16 varchar(255),
    diag_cd17 varchar(255),
    diag_cd18 varchar(255),
    diag_cd19 varchar(255),
    diag_cd20 varchar(255),
    diag_cd21 varchar(255),
    diag_cd22 varchar(255),
    diag_cd23 varchar(255),
    diag_cd24 varchar(255),
    diag_cd25 varchar(255),
    dischg_stat_cd varchar(255) ,
    drg_lclm_cd varchar(255),
    fin_prod_cd varchar(255),
    hcpcs_cpt4_base_cd1 varchar(255),
    icd_proc_01_cd varchar(255),
    icd_proc_02_cd varchar(255),
    icd_proc_03_cd varchar(255),
    icd_proc_04_cd varchar(255),
    icd_proc_05_cd varchar(255),
    in_plan_ntwk_ind varchar(255),
    cov_month date,
    mbr_cost_shr_amt varchar(255), 
    mbr_pers_gen_key varchar(255),
    mco_contract_nbr varchar(255),
    medclm_key varchar(255),
    medclm_lclm_from_date date,
    medclm_lclm_key varchar(255),
    medclm_lclm_to_date date,
    ndc_id varchar(255),
    net_paid_amt varchar(255), 
    net_paid_days_cnt varchar(255),
    npi_id varchar(255),
    paid_date date, 
    pbp_segment_id varchar(255),
    plan_benefit_package_id varchar(255),
    pot_cd varchar(255),
    prov_tax_id varchar(255),
    pymt_cat_cd varchar(255),
    raw_clm_billg_prov_key varchar(255),
    raw_clm_refr1_prov_key varchar(255),
    raw_clm_rendr_prov_key varchar(255),
    revenue_cd varchar(255),
    serv_from_date date, 
    serv_to_date date,  
    serv_unit_cnt varchar(255), 
    src_admit_date date,  
    src_dischrg_date date, 
    src_mbr_id varchar(255),
    src_platform_cd varchar(255),
    src_prov_specialty_cd varchar(255)
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format delimited
fields terminated by '|'
stored as textfile
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/medical_claims/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_humana.medical_claims add
partition(client_id='humana', ingest_date='2020-03-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/medical_claims/ingest_date=2020-03-01/';

alter table raw_humana.medical_claims add
partition(client_id='humana', ingest_date='2020-03-16') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/medical_claims/ingest_date=2020-03-16/';

alter table raw_humana.medical_claims add
partition(client_id='humana', ingest_date='2020-04-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/medical_claims/ingest_date=2020-04-01/';

alter table raw_humana.medical_claims add
partition(client_id='humana', ingest_date='2020-05-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/medical_claims/ingest_date=2020-05-01/';

alter table raw_humana.medical_claims add
partition(client_id='humana', ingest_date='2020-06-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/medical_claims/ingest_date=2020-06-01/';

alter table raw_humana.medical_claims add
partition(client_id='humana', ingest_date='2020-07-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/medical_claims/ingest_date=2020-07-01/';

alter table raw_humana.medical_claims add
partition(client_id='humana', ingest_date='2020-08-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/medical_claims/ingest_date=2020-08-01/';



