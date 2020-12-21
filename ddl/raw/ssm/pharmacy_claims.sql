create external table raw_ssm.pharmacy_claims (
    claim_number varchar(255),
    claim_sequence_number varchar(255),
    patient_external_id varchar(255),
    subscriber_id varchar(255),
    dependent_number varchar(255),
    first_name varchar(255),
    last_name varchar(255),
    gender varchar(255),
    date_of_birth date,
    ndc_code varchar(255),
    days_supply int,
    fill_date_time varchar(255),
    claim_adjustment_type_code varchar(255),
    claim_adjustment_sequence_number varchar(255),
    claim_adjustment_date_time varchar(255),
    allowed_amount numeric(18,2),
    charge_amount numeric(18,2),
    payment_date_time varchar(255),
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
partitioned by (client_id varchar(50), lob varchar(50), insurance_name varchar(50), ingest_date timestamp)
row format delimited
fields terminated by '|'
stored as textfile
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/pharmacy_claims/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_ssm.pharmacy_claim add
partition(client_id='ssm', lob='Medicare FFS', insurance_name='MSSP Missouri', ingest_date='2020-03-06') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/pharmacy_claims/lob=Medicare_FFS/insurance_name=MSSP_Missouri/ingest_date=2020-03-06/';