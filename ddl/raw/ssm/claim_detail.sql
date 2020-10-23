create external table raw_ssm.claim_detail(
    claim_number varchar(255),
    claim_line_number varchar(255),
    claim_sequence_number varchar(255),
    patient_external_id varchar(255),
    subsciber_id varchar(255),
    dependent_number varchar(255),
    place_of_service varchar(255),
    type_of_bill varchar(255),
    service_date_time timestamp,
    claim_from_date_time timestamp,
    claim_thru_date_time timestamp,
    rev_code varchar(255),
    drg varchar(255),
    first_name varchar(255),
    last_name varchar(255),
    gender varchar(255),
    date_of_birth date,
    cpt_hcpcs varchar(255),
    loinc varchar(255),
    snomed varchar(255),
    units varchar(255),
    claim_adjustment_type_code varchar(255),
    claim_adjustment_sequence_number varchar(255),
    claim_adjustment_date_time timestamp,
    performing_npi varchar(255),
    allowed_amount numeric(18,2),
    charge_amount numeric(18,2),
    payment_date_time timestamp,
    plan_payment_amount numeric(18,2),
    member_payment_amount numeric(18,2)
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format delimited
fields terminated by '|'
stored as textfile
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_ssm.claim_detail add
partition(client_id='ssm', ingest_date='2020-03-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/ingest_date=2020-03-01/';

alter table raw_ssm.claim_detail add
partition(client_id='ssm', ingest_date='2020-03-16') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/ingest_date=2020-03-16/';

alter table raw_ssm.claim_detail add
partition(client_id='ssm', ingest_date='2020-04-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/ingest_date=2020-04-01/';

alter table raw_ssm.claim_detail add
partition(client_id='ssm', ingest_date='2020-05-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/ingest_date=2020-05-01/';

alter table raw_ssm.claim_detail add
partition(client_id='ssm', ingest_date='2020-06-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/ingest_date=2020-06-01/';

alter table raw_ssm.claim_detail add
partition(client_id='ssm', ingest_date='2020-07-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/ingest_date=2020-07-01/';

alter table raw_ssm.claim_detail add
partition(client_id='ssm', ingest_date='2020-08-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/ingest_date=2020-08-01/';