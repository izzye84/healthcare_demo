create external table raw_ssm.vitals (
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_id varchar(255),
    vitals_captured_by varchar(255),
    vitals_name varchar(255),
    vitals_value numeric(18,2),
    vitals_unit varchar(255),
    created_timestamp timestamp,
    modified_timestamp timestamp
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format delimited
fields terminated by '|'
stored as textfile
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/vitals/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_ssm.vitals add
partition(client_id='ssm', ingest_date='2020-03-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/vitals/ingest_date=2020-03-01/';

alter table raw_ssm.vitals add
partition(client_id='ssm', ingest_date='2020-03-16') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/vitals/ingest_date=2020-03-16/';

alter table raw_ssm.vitals add
partition(client_id='ssm', ingest_date='2020-04-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/vitals/ingest_date=2020-04-01/';

alter table raw_ssm.vitals add
partition(client_id='ssm', ingest_date='2020-05-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/vitals/ingest_date=2020-05-01/';

alter table raw_ssm.vitals add
partition(client_id='ssm', ingest_date='2020-06-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/vitals/ingest_date=2020-06-01/';

alter table raw_ssm.vitals add
partition(client_id='ssm', ingest_date='2020-07-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/vitals/ingest_date=2020-07-01/';

alter table raw_ssm.vitals add
partition(client_id='ssm', ingest_date='2020-08-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/vitals/ingest_date=2020-08-01/';