create external table raw_ssm.payer (
    patient_id varchar(255),
    patient_account_number varchar(255),
    payer_id varchar(255),
    payer_name varchar(255),
    payer_code varchar(255),
    contract_number varchar(255),
    medicaid_id varchar(255),
    medicare_id varchar(255),
    insurance_type_flag varchar(255),
    insurance_product_type varchar(255),
    coverage_begin_date timestamp,
    coverage_end_date timestamp,
    insurance_address_line_1 varchar(255),
    insurance_address_line_2 varchar(255),
    insurance_city varchar(255),
    insurance_state varchar(255),
    insurance_zip varchar(255),
    insurance_group_number varchar(255),
    insurance_subscriber_first_name varchar(255),
    insurance_subscriber_last_name varchar(255),
    insurance_subscriber_relation varchar(255),
    created_timestamp timestamp,
    modified_timestamp timestamp
)
partitioned by (client_id varchar(50), ingest_date char(10))
row format delimited
fields terminated by '|'
stored as textfile
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/payer/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_ssm.payer add
partition(client_id='ssm', ingest_date='2020-03-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/payer/ingest_date=2020-03-01/';

alter table raw_ssm.payer add
partition(client_id='ssm', ingest_date='2020-03-16') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/payer/ingest_date=2020-03-16/';

alter table raw_ssm.payer add
partition(client_id='ssm', ingest_date='2020-04-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/payer/ingest_date=2020-04-01/';

alter table raw_ssm.payer add
partition(client_id='ssm', ingest_date='2020-05-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/payer/ingest_date=2020-05-01/';

alter table raw_ssm.payer add
partition(client_id='ssm', ingest_date='2020-06-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/payer/ingest_date=2020-06-01/';

alter table raw_ssm.payer add
partition(client_id='ssm', ingest_date='2020-07-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/payer/ingest_date=2020-07-01/';

alter table raw_ssm.payer add
partition(client_id='ssm', ingest_date='2020-08-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/payer/ingest_date=2020-08-01/';