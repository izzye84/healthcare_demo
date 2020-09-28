create external table raw_ssm.pharmacy_claim (
    claim_number varchar(255),
    claim_sequence_number varchar(255),
    patient_external_id varchar(255),
    subscriber_id varchar(255),
    dependent_number varchar(255),
    first_name varchar(255),
    last_name varchar(255),
    gender varchar(255),
    date_of_birth timestamp,
    ndc_code varchar(255),
    days_supply int,
    fill_date_time timestamp,
    claim_adjustment_type_code varchar(255),
    claim_adjustment_sequence_number varchar(255),
    claim_adjustment_date_time timestamp,
    allowed_amount numeric(18,2),
    charge_amount numeric(18,2),
    payment_date_time timestamp,
    plan_payment_amount numeric(18,2),
    member_payment_amount numeric(18,2),
    daw varchar(255),
    rx_number varchar(255),
    dosage varchar(255),
    metric_qty varchar(255),
    qty_units varchar(255),
    prescribing_npi varchar(255),
    pharmacy_npi varchar(255),
    formulary varchar(255),
    generic varchar(255),
    retail varchar(255)
)
partitioned by (client_id varchar(50), ingest_date char(10))
row format delimited
fields terminated by '|'
stored as textfile
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/pharmacy_claim/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_ssm.pharmacy_claim add
partition(client_id='ssm', ingest_id='2020-03-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/pharmacy_claim/ingest_date=2020-03-01/';

alter table raw_ssm.pharmacy_claim add
partition(client_id='ssm', ingest_id='2020-03-16') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/pharmacy_claim/ingest_date=2020-03-16/';

alter table raw_ssm.pharmacy_claim add
partition(client_id='ssm', ingest_id='2020-04-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/pharmacy_claim/ingest_date=2020-04-01/';

alter table raw_ssm.pharmacy_claim add
partition(client_id='ssm', ingest_id='2020-05-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/pharmacy_claim/ingest_date=2020-05-01/';

alter table raw_ssm.pharmacy_claim add
partition(client_id='ssm', ingest_id='2020-06-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/pharmacy_claim/ingest_date=2020-06-01/';

alter table raw_ssm.pharmacy_claim add
partition(client_id='ssm', ingest_id='2020-07-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/pharmacy_claim/ingest_date=2020-07-01/';

alter table raw_ssm.pharmacy_claim add
partition(client_id='ssm', ingest_id='2020-08-01') 
location 's3://strive-analytics-warehouse/clients/client_id=ssm/data_frequency=batch/pharmacy_claim/ingest_date=2020-08-01/';