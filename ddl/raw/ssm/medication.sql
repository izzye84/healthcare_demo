create external table raw_ssm.medication (
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_id varchar(255),
    medication_origin_id varchar(255),
    medication_code_set varchar(255),
    medication_code varchar(255),
    medication_description varchar(255),
    medication_begin_date date,
    medication_end_date date,
    days_supply varchar(255),
    quantity varchar(255),
    refills int,
    strength varchar(255),
    strength_unit varchar(255),
    formulation varchar(255),
    route varchar(255),
    frequency varchar(255),
    dosage varchar(255),
    med_status varchar(255),
    sample varchar(255),
    created_timestamp timestamp,
    modified_timestamp timestamp
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format delimited
fields terminated by '|'
stored as textfile
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/medication/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_ssm.medication add
partition(client_id='ssm', ingest_date='2020-03-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/medication/ingest_date=2020-03-01/';

alter table raw_ssm.medication add
partition(client_id='ssm', ingest_date='2020-03-16') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/medication/ingest_date=2020-03-16/';

alter table raw_ssm.medication add
partition(client_id='ssm', ingest_date='2020-04-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/medication/ingest_date=2020-04-01/';

alter table raw_ssm.medication add
partition(client_id='ssm', ingest_date='2020-05-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/medication/ingest_date=2020-05-01/';

alter table raw_ssm.medication add
partition(client_id='ssm', ingest_date='2020-06-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/medication/ingest_date=2020-06-01/';

alter table raw_ssm.medication add
partition(client_id='ssm', ingest_date='2020-07-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/medication/ingest_date=2020-07-01/';

alter table raw_ssm.medication add
partition(client_id='ssm', ingest_date='2020-08-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/medication/ingest_date=2020-08-01/';