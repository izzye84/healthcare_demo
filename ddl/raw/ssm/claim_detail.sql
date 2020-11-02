create external table raw_ssm.claim_detail(
    claim_number varchar(255),
    claim_line_number varchar(255),
    claim_sequence_number varchar(255),
    patient_external_id varchar(255),
    subsciber_id varchar(255),
    dependent_number varchar(255),
    place_of_service varchar(255),
    type_of_bill varchar(255),
    service_date_time varchar(255),
    claim_from_date_time varchar(255),
    claim_thru_date_time varchar(255),
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
    claim_adjustment_date_time varchar(255),
    performing_npi varchar(255),
    allowed_amount numeric(18,2),
    charge_amount numeric(18,2),
    payment_date_time varchar(255),
    plan_payment_amount numeric(18,2),
    member_payment_amount numeric(18,2)
)
partitioned by (client_id varchar(50), lob varchar(50), insurance_name varchar(50), ingest_date timestamp)
row format delimited
fields terminated by '|'
stored as textfile
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Commercial', insurance_name='Anthem', ingest_date='2020-06-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Commercial/insurance_name=Anthem/ingest_date=2020-06-01/';
alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Commercial', insurance_name='Anthem', ingest_date='2020-07-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Commercial/insurance_name=Anthem/ingest_date=2020-07-01/';
alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Commercial', insurance_name='Anthem', ingest_date='2020-08-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Commercial/insurance_name=Anthem/ingest_date=2020-08-01/';
alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Commercial', insurance_name='Anthem', ingest_date='2020-09-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Commercial/insurance_name=Anthem/ingest_date=2020-09-01/';

alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Commercial', insurance_name='Cigna', ingest_date='2020-06-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Commercial/insurance_name=Cigna/ingest_date=2020-06-01/';
alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Commercial', insurance_name='Cigna', ingest_date='2020-07-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Commercial/insurance_name=Cigna/ingest_date=2020-07-01/';
alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Commercial', insurance_name='Cigna', ingest_date='2020-08-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Commercial/insurance_name=Cigna/ingest_date=2020-08-01/';
alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Commercial', insurance_name='Cigna', ingest_date='2020-09-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Commercial/insurance_name=Cigna/ingest_date=2020-09-01/';

alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Commercial', insurance_name='United Healthcare', ingest_date='2020-06-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Commercial/insurance_name=United_Healthcare/ingest_date=2020-06-01/';

alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='Anthem', ingest_date='2020-06-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Medicare_Advantage/insurance_name=Anthem/ingest_date=2020-06-01/';
alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='Anthem', ingest_date='2020-07-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Medicare_Advantage/insurance_name=Anthem/ingest_date=2020-07-01/';
alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='Anthem', ingest_date='2020-08-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Medicare_Advantage/insurance_name=Anthem/ingest_date=2020-08-01/';
alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='Anthem', ingest_date='2020-09-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Medicare_Advantage/insurance_name=Anthem/ingest_date=2020-09-01/';

alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='Coventry', ingest_date='2020-06-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Medicare_Advantage/insurance_name=Coventry/ingest_date=2020-06-01/';
alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='Coventry', ingest_date='2020-07-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Medicare_Advantage/insurance_name=Coventry/ingest_date=2020-07-01/';
alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='Coventry', ingest_date='2020-08-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Medicare_Advantage/insurance_name=Coventry/ingest_date=2020-08-01/';
alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='Coventry', ingest_date='2020-09-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Medicare_Advantage/insurance_name=Coventry/ingest_date=2020-09-01/';

alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='United Healthcare', ingest_date='2020-06-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Medicare_Advantage/insurance_name=United_Healthcare/ingest_date=2020-06-01/';
alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='United Healthcare', ingest_date='2020-07-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Medicare_Advantage/insurance_name=United_Healthcare/ingest_date=2020-07-01/';
alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='United Healthcare', ingest_date='2020-08-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Medicare_Advantage/insurance_name=United_Healthcare/ingest_date=2020-08-01/';

alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Medicare FFS', insurance_name='MSSP Missouri', ingest_date='2020-06-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Medicare_FFS/insurance_name=MSSP_Missouri/ingest_date=2020-06-01/';
alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Medicare FFS', insurance_name='MSSP Missouri', ingest_date='2020-07-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Medicare_FFS/insurance_name=MSSP_Missouri/ingest_date=2020-07-01/';
alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Medicare FFS', insurance_name='MSSP Missouri', ingest_date='2020-08-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Medicare_FFS/insurance_name=MSSP_Missouri/ingest_date=2020-08-01/';
alter table raw_ssm.claim_detail add
partition(client_id='ssm', lob='Medicare FFS', insurance_name='MSSP Missouri', ingest_date='2020-09-01') 
location 's3://strive-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_detail/lob=Medicare_FFS/insurance_name=MSSP_Missouri/ingest_date=2020-09-01/';