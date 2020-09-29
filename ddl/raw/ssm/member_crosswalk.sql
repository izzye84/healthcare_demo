create external table raw_ssm.member_crosswalk(
    insurance_name varchar(255),
    lob varchar(255),
    payer_contract varchar(255),
    epic_pat_id varchar(255),
    enterprise_mrn varchar(255),
    person_id varchar(255)
)
partitioned by (client_id varchar(50), ingest_date char(10))
row format delimited
fields terminated by '|'
stored as textfile
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/crosswalk/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_ssm.member_crosswalk add
partition(client_id='ssm', ingest_date='2020-03-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/crosswalk/ingest_date=2020-03-01/';

alter table raw_ssm.member_crosswalk add
partition(client_id='ssm', ingest_date='2020-03-16') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/crosswalk/ingest_date=2020-03-16/';

alter table raw_ssm.member_crosswalk add
partition(client_id='ssm', ingest_date='2020-04-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/crosswalk/ingest_date=2020-04-01/';

alter table raw_ssm.member_crosswalk add
partition(client_id='ssm', ingest_date='2020-05-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/crosswalk/ingest_date=2020-05-01/';

alter table raw_ssm.member_crosswalk add
partition(client_id='ssm', ingest_date='2020-06-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/crosswalk/ingest_date=2020-06-01/';

alter table raw_ssm.member_crosswalk add
partition(client_id='ssm', ingest_date='2020-07-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/crosswalk/ingest_date=2020-07-01/';

alter table raw_ssm.member_crosswalk add
partition(client_id='ssm', ingest_date='2020-08-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/crosswalk/ingest_date=2020-08-01/';