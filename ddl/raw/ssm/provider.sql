create external table raw_ssm.provider (
    provider_id varchar(255),
    npi varchar(255),
    first_name varchar(255),
    last_name varchar(255),
    suffix varchar(255),
    provider_type varchar(255),
    title varchar(255),
    date_of_birth timestamp,
    gender varchar(255),
    address_line_1 varchar(255),
    address_line_2 varchar(255),
    city varchar(255),
    state varchar(255),
    zip_code varchar(255),
    zip_code_last_4 varchar(255),
    address_type varchar(255),
    phone_number varchar(255),
    contact_type varchar(255),
    email_address varchar(255),
    fax_number varchar(255),
    practice_name varchar(255),
    practice_tin varchar(255),
    practice_oid varchar(255),
    organization_id varchar(255),
    organization_type varchar(255),
    organization_group_npi varchar(255),
    organization_group_name varchar(255),
    organization_group_tin varchar(255),
    organization_group_oid varchar(255),
    organization_group_address_line_1 varchar(255),
    organization_group_address_line_2 varchar(255),
    organization_group_city varchar(255),
    organization_group_state varchar(255),
    organization_group_zip_code varchar(255),
    organization_group_zip_code_4 varchar(255),
    primary_specialty_code varchar(255),
    primary_specialty_description varchar(255),
    secondary_specialty_code varchar(255),
    secondary_specialty_description varchar(255),
    created_timestamp timestamp,
    modified_timestamp timestamp
)
partitioned by (client_id varchar(50), ingest_date char(10))
row format delimited
fields terminated by '|'
stored as textfile
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/provider/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_ssm.provider add
partition(client_id='ssm', ingest_date='2020-03-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/provider/ingest_date=2020-03-01/';

alter table raw_ssm.provider add
partition(client_id='ssm', ingest_date='2020-03-16') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/provider/ingest_date=2020-03-16/';

alter table raw_ssm.provider add
partition(client_id='ssm', ingest_date='2020-04-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/provider/ingest_date=2020-04-01/';

alter table raw_ssm.provider add
partition(client_id='ssm', ingest_date='2020-05-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/provider/ingest_date=2020-05-01/';

alter table raw_ssm.provider add
partition(client_id='ssm', ingest_date='2020-06-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/provider/ingest_date=2020-06-01/';

alter table raw_ssm.provider add
partition(client_id='ssm', ingest_date='2020-07-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/provider/ingest_date=2020-07-01/';

alter table raw_ssm.provider add
partition(client_id='ssm', ingest_date='2020-08-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/provider/ingest_date=2020-08-01/';