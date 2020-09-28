create external table raw_ssm.referral (
    patient varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_id varchar(255),
    referral_id varchar(255),
    referral_date timestamp,
    sender_network varchar(255),
    sending_practice varchar(255),
    sending_practitioner_npi varchar(255),
    receiver_network varchar(255),
    receiving_practice varchar(255),
    receiving_practitioner_npi varchar(255),
    receiving_organization varchar(255),
    receiving_organization_id varchar(255),
    referral_icd_code varchar(255),
    referral_cpt_code varchar(255),
    referral_description varchar(255),
    out_of_network_status varchar(255),
    out_of_network_reason varchar(255),
    created_timestamp timestamp,
    modified_timestamp timestamp
)
partitioned by (client_id varchar(50), ingest_date char(10))
row format delimited
fields terminated by '|'
stored as textfile
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/referral/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_ssm.referral add
partition(client_id='ssm', ingest_id='2020-03-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/referral/ingest_date=2020-03-01/';

alter table raw_ssm.referral add
partition(client_id='ssm', ingest_id='2020-03-16') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/referral/ingest_date=2020-03-16/';

alter table raw_ssm.referral add
partition(client_id='ssm', ingest_id='2020-04-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/referral/ingest_date=2020-04-01/';

alter table raw_ssm.referral add
partition(client_id='ssm', ingest_id='2020-05-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/referral/ingest_date=2020-05-01/';

alter table raw_ssm.referral add
partition(client_id='ssm', ingest_id='2020-06-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/referral/ingest_date=2020-06-01/';

alter table raw_ssm.referral add
partition(client_id='ssm', ingest_id='2020-07-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/referral/ingest_date=2020-07-01/';

alter table raw_ssm.referral add
partition(client_id='ssm', ingest_id='2020-08-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/referral/ingest_date=2020-08-01/';