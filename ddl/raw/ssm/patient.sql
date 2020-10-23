create external table raw_ssm.patient (
    patient_id varchar(255),
    patient_account_number varchar(255),
    social_security_number varchar(255),
    first_name varchar(255),
    last_name varchar(255),
    preferred_name varchar(255),
    salutation varchar(255),
    date_of_birth date,
    gender varchar(255),
    race varchar(255),
    language_spoken varchar(255),
    secondary_language_spoken varchar(255),
    death_date date,
    country_of_birth varchar(255),
    address_line_1 varchar(255),
    address_line_2 varchar(255),
    city varchar(255),
    state varchar(255),
    zip_code varchar(255),
    zip_code_last_4 varchar(255),
    mailing_country varchar(255),
    address_type varchar(255),
    phone_number varchar(255),
    contact_type varchar(255),
    email_address varchar(255),
    pcp_provider_id varchar(255),
    pcp_npi varchar(255),
    created_timestamp timestamp,
    modified_timestamp timestamp
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format delimited
fields terminated by '|'
stored as textfile
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/patient/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_ssm.patient add
partition(client_id='ssm', ingest_date='2020-03-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/patient/ingest_date=2020-03-01/';

alter table raw_ssm.patient add
partition(client_id='ssm', ingest_date='2020-03-16') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/patient/ingest_date=2020-03-16/';

alter table raw_ssm.patient add
partition(client_id='ssm', ingest_date='2020-04-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/patient/ingest_date=2020-04-01/';

alter table raw_ssm.patient add
partition(client_id='ssm', ingest_date='2020-05-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/patient/ingest_date=2020-05-01/';

alter table raw_ssm.patient add
partition(client_id='ssm', ingest_date='2020-06-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/patient/ingest_date=2020-06-01/';

alter table raw_ssm.patient add
partition(client_id='ssm', ingest_date='2020-07-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/patient/ingest_date=2020-07-01/';

alter table raw_ssm.patient add
partition(client_id='ssm', ingest_date='2020-08-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/patient/ingest_date=2020-08-01/';