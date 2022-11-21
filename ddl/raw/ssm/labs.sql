create external table raw_ssm.labs(
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_id varchar(255),
    lab_id varchar(255),
    lab_code_set varchar(255),
    lab_code varchar(255),
    lab_order_date date,
    lab_order_description varchar(255),
    service_code varchar(255),
    service_code_modifier varchar(255),
    service_code_type varchar(255),
    service_code_desc varchar(255),
    result_description varchar(255),
    collection_date date,
    result_date date,
    result_text varchar(255),
    result_numeric numeric(18,4),
    result_pos_neg varchar(255),
    unit varchar(255),
    reference_range varchar(255),
    abnormal_flag varchar(255),
    result_status varchar(255),
    created_timestamp timestamp,
    modified_timestamp timestamp
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format delimited
fields terminated by '|'
stored as textfile
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/labs/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_ssm.labs add
partition(client_id='ssm', ingest_date='2020-03-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/labs/ingest_date=2020-03-01/';

alter table raw_ssm.labs add
partition(client_id='ssm', ingest_date='2020-03-16') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/labs/ingest_date=2020-03-16/';

alter table raw_ssm.labs add
partition(client_id='ssm', ingest_date='2020-04-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/labs/ingest_date=2020-04-01/';

alter table raw_ssm.labs add
partition(client_id='ssm', ingest_date='2020-05-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/labs/ingest_date=2020-05-01/';

alter table raw_ssm.labs add
partition(client_id='ssm', ingest_date='2020-06-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/labs/ingest_date=2020-06-01/';

alter table raw_ssm.labs add
partition(client_id='ssm', ingest_date='2020-07-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/labs/ingest_date=2020-07-01/';

alter table raw_ssm.labs add
partition(client_id='ssm', ingest_date='2020-08-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/labs/ingest_date=2020-08-01/';