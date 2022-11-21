create external table raw_ssm.claim_header (
    claim_number varchar(255),
    claim_sequence_number varchar(255),
    patient_external_id varchar(255),
    subscriber_id varchar(255),
    dependent_number varchar(255),
    place_of_service varchar(255),
    type_of_bill varchar(255),
    service_date_time varchar(255),
    claim_from_date_time varchar(255),
    claim_thru_date_time varchar(255),
    admission_type varchar(255),
    admission_source varchar(255),
    discharge_status varchar(255),
    rev_code varchar(255),
    drg varchar(255),
    first_name varchar(255),
    last_name varchar(255),
    gender varchar(255),
    date_of_birth date,
    procedure_code1 varchar(255),
    procedure_code2 varchar(255),
    procedure_code3 varchar(255),
    procedure_code4 varchar(255),
    procedure_code5 varchar(255),
    procedure_code6 varchar(255),
    procedure_code7 varchar(255),
    procedure_code8 varchar(255),
    procedure_code9 varchar(255),
    procedure_code10 varchar(255),
    procedure_code11 varchar(255),
    procedure_code12 varchar(255),
    procedure_code13 varchar(255),
    procedure_code14 varchar(255),
    procedure_code15 varchar(255),
    procedure_code16 varchar(255),
    procedure_code17 varchar(255),
    procedure_code18 varchar(255),
    procedure_code19 varchar(255),
    procedure_code20 varchar(255),
    procedure_code1_modifier varchar(255),
    procedure_code2_modifier varchar(255),
    procedure_code3_modifier varchar(255),
    procedure_code4_modifier varchar(255),
    procedure_code5_modifier varchar(255),
    procedure_code6_modifier varchar(255),
    procedure_code7_modifier varchar(255),
    procedure_code8_modifier varchar(255),
    procedure_code9_modifier varchar(255),
    procedure_code10_modifier varchar(255),
    procedure_code11_modifier varchar(255),
    procedure_code12_modifier varchar(255),
    procedure_code13_modifier varchar(255),
    procedure_code14_modifier varchar(255),
    procedure_code15_modifier varchar(255),
    procedure_code16_modifier varchar(255),
    procedure_code17_modifier varchar(255),
    procedure_code18_modifier varchar(255),
    procedure_code19_modifier varchar(255),
    procedure_code20_modifier varchar(255),
    diagnosis_code1 varchar(255),
    diagnosis_code2 varchar(255),
    diagnosis_code3 varchar(255),
    diagnosis_code4 varchar(255),
    diagnosis_code5 varchar(255),
    diagnosis_code6 varchar(255),
    diagnosis_code7 varchar(255),
    diagnosis_code8 varchar(255),
    diagnosis_code9 varchar(255),
    diagnosis_code10 varchar(255),
    diagnosis_code11 varchar(255),
    diagnosis_code12 varchar(255),
    diagnosis_code13 varchar(255),
    diagnosis_code14 varchar(255),
    diagnosis_code15 varchar(255),
    diagnosis_code16 varchar(255),
    diagnosis_code17 varchar(255),
    diagnosis_code18 varchar(255),
    diagnosis_code19 varchar(255),
    diagnosis_code20 varchar(255),
    icd_version_indicator varchar(255),
    allowed_amount numeric(18,2),
    charge_amount numeric(18,2),
    payment_date_time timestamp,
    plan_payment_amount numeric(18,2),
    member_payment_amount numeric(18,2),
    claim_adjustment_type_code varchar(255),
    claim_adjustment_sequence_number varchar(255),
    claim_adjustment_date_time varchar(255),
    submitting_provider_npi varchar(255),
    rendering_provider_npi varchar(255),
    referring_provider_npi varchar(255),
    pcp_provider_npi varchar(255),
    facility_npi varchar(255)
)
partitioned by (client_id varchar(50), lob varchar(50), insurance_name varchar(50), ingest_date timestamp)
row format delimited
fields terminated by '|'
stored as textfile
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/'
table properties ('skip.header.line.count' = '1');

alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Commercial', insurance_name='Anthem', ingest_date='2020-06-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Commercial/insurance_name=Anthem/ingest_date=2020-06-01/';
alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Commercial', insurance_name='Anthem', ingest_date='2020-07-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Commercial/insurance_name=Anthem/ingest_date=2020-07-01/';
alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Commercial', insurance_name='Anthem', ingest_date='2020-08-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Commercial/insurance_name=Anthem/ingest_date=2020-08-01/';
alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Commercial', insurance_name='Anthem', ingest_date='2020-09-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Commercial/insurance_name=Anthem/ingest_date=2020-09-01/';

alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Commercial', insurance_name='Cigna', ingest_date='2020-06-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Commercial/insurance_name=Cigna/ingest_date=2020-06-01/';
alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Commercial', insurance_name='Cigna', ingest_date='2020-07-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Commercial/insurance_name=Cigna/ingest_date=2020-07-01/';
alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Commercial', insurance_name='Cigna', ingest_date='2020-08-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Commercial/insurance_name=Cigna/ingest_date=2020-08-01/';
alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Commercial', insurance_name='Cigna', ingest_date='2020-09-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Commercial/insurance_name=Cigna/ingest_date=2020-09-01/';

alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Commercial', insurance_name='United Healthcare', ingest_date='2020-06-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Commercial/insurance_name=United_Healthcare/ingest_date=2020-06-01/';

alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='Anthem', ingest_date='2020-06-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Medicare_Advantage/insurance_name=Anthem/ingest_date=2020-06-01/';
alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='Anthem', ingest_date='2020-07-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Medicare_Advantage/insurance_name=Anthem/ingest_date=2020-07-01/';
alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='Anthem', ingest_date='2020-08-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Medicare_Advantage/insurance_name=Anthem/ingest_date=2020-08-01/';
alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='Anthem', ingest_date='2020-09-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Medicare_Advantage/insurance_name=Anthem/ingest_date=2020-09-01/';

alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='Coventry', ingest_date='2020-06-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Medicare_Advantage/insurance_name=Coventry/ingest_date=2020-06-01/';
alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='Coventry', ingest_date='2020-07-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Medicare_Advantage/insurance_name=Coventry/ingest_date=2020-07-01/';
alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='Coventry', ingest_date='2020-08-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Medicare_Advantage/insurance_name=Coventry/ingest_date=2020-08-01/';
alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='Coventry', ingest_date='2020-09-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Medicare_Advantage/insurance_name=Coventry/ingest_date=2020-09-01/';

alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='United Healthcare', ingest_date='2020-06-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Medicare_Advantage/insurance_name=United_Healthcare/ingest_date=2020-06-01/';
alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='United Healthcare', ingest_date='2020-07-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Medicare_Advantage/insurance_name=United_Healthcare/ingest_date=2020-07-01/';
alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Medicare Advantage', insurance_name='United Healthcare', ingest_date='2020-08-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Medicare_Advantage/insurance_name=United_Healthcare/ingest_date=2020-08-01/';

alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Medicare FFS', insurance_name='MSSP Missouri', ingest_date='2020-06-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Medicare_FFS/insurance_name=MSSP_Missouri/ingest_date=2020-06-01/';
alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Medicare FFS', insurance_name='MSSP Missouri', ingest_date='2020-07-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Medicare_FFS/insurance_name=MSSP_Missouri/ingest_date=2020-07-01/';
alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Medicare FFS', insurance_name='MSSP Missouri', ingest_date='2020-08-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Medicare_FFS/insurance_name=MSSP_Missouri/ingest_date=2020-08-01/';
alter table raw_ssm.claim_header add
partition(client_id='ssm', lob='Medicare FFS', insurance_name='MSSP Missouri', ingest_date='2020-09-01') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/claim_header/lob=Medicare_FFS/insurance_name=MSSP_Missouri/ingest_date=2020-09-01/';