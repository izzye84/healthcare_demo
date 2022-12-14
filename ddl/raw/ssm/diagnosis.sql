create external table raw_ssm.diagnosis (
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_id varchar(255),
    diagnosis_date date,
    diagnosis_code varchar(255),
    onset_date date,
    resolved_date date,
    code_set varchar(255),
    diagnosis_name varchar(255),
    service_date date,
    created_timestamp timestamp,
    modified_timestamp timestamp
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format delimited
fields terminated by '|'
stored as textfile
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/diagnosis/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_ssm.diagnosis add
partition(client_id='ssm', ingest_date='2020-05-06') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/diagnosis/ingest_date=2020-05-06/';