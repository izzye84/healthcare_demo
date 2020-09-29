create external table raw_humana.referral (
    mbr_pers_gen_key varchar(255),
    idcard_mbr_id varchar(255),
    pers_last_name varchar(255),
    pers_first_name varchar(255),
    birth_date date,
    gender varchar(255),
    home_phone_nbr varchar(255),
    work_phone_nbr varchar(255),
    work_phone_ext varchar(255),
    primary_email_addr varchar(255),
    ckd_stage varchar(255),
    latest_gfr varchar(255),
    latest_acr_score_over300 varchar(255),
    esrd_2clms_last4mo varchar(255),
    dialysis_predictive_model_score varchar(255),
    state_of_residence varchar(255),
    county_of_residence varchar(255),
    current_zip_code varchar(255),
    addr_line1 varchar(255),
    addr_line2 varchar(255),
    city_name varchar(255),
    zip_cd varchar(255),
    zip_plus_cd varchar(255),
    latest_month_neph_visit date,
    neph_provider_name varchar(255),
    neph_npi varchar(255),
    neph_practice_name varchar(255),
    neph_first_name varchar(255),
    neph_last_name varchar(255),
    rendering_nephrologist_npi varchar(255),
    last_pcp_attributed_month date,
    pcp_provider_name varchar(255),
    pcp_npi varchar(255),
    pcp_practice_name varchar(255),
    pcp_first_name varchar(255),
    pcp_last_name varchar(255),
    pcp_tax_id varchar(255),
    latest_month_cardiologist_visit date,
    cardiologist_practice_name varchar(255),
    cardiologist_first_name varchar(255),
    cardiologist_last_name varchar(255),
    latest_mo_endocrinologist_visit date,
    endocrinologist_practice_name varchar(255),
    endocrinologist_first_name varchar(255),
    endocrinologist_last_name varchar(255),
    vendor varchar(255),
    lob varchar(255),
    referral_month varchar(255),
    live varchar(255),
    notes varchar(255)
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format delimited
fields terminated by '|'
stored as textfile
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/referral/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_humana.referral add
partition(client_id='humana', ingest_date='2020-03-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/referral/ingest_date=2020-03-01/';

alter table raw_humana.referral add
partition(client_id='humana', ingest_date='2020-03-16') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/referral/ingest_date=2020-03-16/';

alter table raw_humana.referral add
partition(client_id='humana', ingest_date='2020-04-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/referral/ingest_date=2020-04-01/';

alter table raw_humana.referral add
partition(client_id='humana', ingest_date='2020-05-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/referral/ingest_date=2020-05-01/';

alter table raw_humana.referral add
partition(client_id='humana', ingest_date='2020-06-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/referral/ingest_date=2020-06-01/';

alter table raw_humana.referral add
partition(client_id='humana', ingest_date='2020-07-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/referral/ingest_date=2020-07-01/';

alter table raw_humana.referral add
partition(client_id='humana', ingest_date='2020-08-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/referral/ingest_date=2020-08-01/';
