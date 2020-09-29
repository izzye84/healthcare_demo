create external table raw_humana.lab_claims (
    cov_month date,
    lab_result_key varchar(255),
    lab_results_value varchar(255), 
    loinc_cd varchar(255),
    pers_gen_key varchar(255),
    service_date date, 
    src_mbr_id varchar(255),
    src_platform_cd varchar(255),
    vendor_loinc_cd varchar(255),
    vendor_results_units_desc varchar(255)
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format delimited
fields terminated by '|'
stored as textfile
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/lab_claims/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_humana.lab_claims add
partition(client_id='humana', ingest_date='2020-03-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/lab_claims/ingest_date=2020-03-01/';

alter table raw_humana.lab_claims add
partition(client_id='humana', ingest_date='2020-03-16') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/lab_claims/ingest_date=2020-03-16/';

alter table raw_humana.lab_claims add
partition(client_id='humana', ingest_date='2020-04-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/lab_claims/ingest_date=2020-04-01/';

alter table raw_humana.lab_claims add
partition(client_id='humana', ingest_date='2020-05-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/lab_claims/ingest_date=2020-05-01/';

alter table raw_humana.lab_claims add
partition(client_id='humana', ingest_date='2020-06-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/lab_claims/ingest_date=2020-06-01/';

alter table raw_humana.lab_claims add
partition(client_id='humana', ingest_date='2020-07-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/lab_claims/ingest_date=2020-07-01/';

alter table raw_humana.lab_claims add
partition(client_id='humana', ingest_date='2020-08-01') 
location 's3://strive-analytics-warehouse/clients/client_id=humana/data_frequency=batch/lab_claims/ingest_date=2020-08-01/';


