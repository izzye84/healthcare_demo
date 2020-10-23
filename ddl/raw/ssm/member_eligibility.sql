create external table raw_ssm.member_eligibility (
    first_name varchar(255),
    middle_name varchar(255),
    last_name varchar(255),
    social_security varchar(15),
    dob date,
    patient_id varchar(255),
    relation varchar(255),
    gender varchar(255),
    race varchar(255),
    ethnic_group varchar(255),
    primary_phone varchar(50),
    primary_email varchar(255),
    address_line1 varchar(255),
    address_line2 varchar(255),
    city varchar(255),
    state varchar(255),
    zip varchar(255),
    eff_date date,
    term_date date,
    lob varchar(255),
    contract varchar(255),
    insurance_name varchar(255),
    primary_care_prov_id varchar(255)
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format delimited
fields terminated by '|'
stored as textfile
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/member_eligibility/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_ssm.member_eligibility add
partition(client_id='ssm', ingest_date='2020-03-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/member_eligibility/ingest_date=2020-03-01/';

alter table raw_ssm.member_eligibility add
partition(client_id='ssm', ingest_date='2020-03-16') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/member_eligibility/ingest_date=2020-03-16/';

alter table raw_ssm.member_eligibility add
partition(client_id='ssm', ingest_date='2020-04-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/member_eligibility/ingest_date=2020-04-01/';

alter table raw_ssm.member_eligibility add
partition(client_id='ssm', ingest_date='2020-05-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/member_eligibility/ingest_date=2020-05-01/';

alter table raw_ssm.member_eligibility add
partition(client_id='ssm', ingest_date='2020-06-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/member_eligibility/ingest_date=2020-06-01/';

alter table raw_ssm.member_eligibility add
partition(client_id='ssm', ingest_date='2020-07-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/member_eligibility/ingest_date=2020-07-01/';

alter table raw_ssm.member_eligibility add
partition(client_id='ssm', ingest_date='2020-08-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/member_eligibility/ingest_date=2020-08-01/';