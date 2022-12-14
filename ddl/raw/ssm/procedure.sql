create external table raw_ssm.procedure (
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_id varchar(255),
    procedure_code varchar(255),
    procedure_description varchar(255),
    procedure_code_type varchar(255),
    modifier1 varchar(255),
    modifier2 varchar(255),
    modifier3 varchar(255),
    modifier4 varchar(255),
    created_timestamp timestamp,
    modified_timestamp timestamp
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format delimited
fields terminated by '|'
stored as textfile
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/procedure/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_ssm.procedure add
partition(client_id='ssm', ingest_date='2020-05-06') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/procedure/ingest_date=2020-05-06/';