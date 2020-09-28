create external table raw_ssm.immunization (
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_id varchar(255),
    given_date timestamp,
    immunization_description varchar(255),
    lot_number varchar(255),
    vaccine_manufacturer varchar(255),
    immunization_dose numeric(18,2),
    site varchar(255),
    route varchar(255),
    expiration_date timestamp,
    cvx_code varchar(255),
    cpt_code varchar(255),
    immunization_given_by_id varchar(255),
    immunization_date timestamp,
    vfc_status varchar(255),
    created_timestamp timestamp,
    modified_timestamp timestamp
)
partitioned by (client_id varchar(50), ingest_date char(10))
row format delimited
fields terminated by '|'
stored as textfile
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/immunization/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_ssm.immunization add
partition(client_id='ssm', ingest_id='2020-03-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/immunization/ingest_date=2020-03-01/';

alter table raw_ssm.immunization add
partition(client_id='ssm', ingest_id='2020-03-16') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/immunization/ingest_date=2020-03-16/';

alter table raw_ssm.immunization add
partition(client_id='ssm', ingest_id='2020-04-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/immunization/ingest_date=2020-04-01/';

alter table raw_ssm.immunization add
partition(client_id='ssm', ingest_id='2020-05-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/immunization/ingest_date=2020-05-01/';

alter table raw_ssm.immunization add
partition(client_id='ssm', ingest_id='2020-06-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/immunization/ingest_date=2020-06-01/';

alter table raw_ssm.immunization add
partition(client_id='ssm', ingest_id='2020-07-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/immunization/ingest_date=2020-07-01/';

alter table raw_ssm.immunization add
partition(client_id='ssm', ingest_id='2020-08-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/immunization/ingest_date=2020-08-01/';