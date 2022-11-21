create external table raw_conviva.procedure (
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_id varchar(255),
    procedure_code varchar(255),
    procedure_description varchar(255),
    procedure_code_type varchar(255),
    modifier1 varchar(255),
    modifier2 varchar(255),
    modifier3 varchar(255),
    modifier4 varchar(255),
    created_timestamp varchar(255),
    modified_timestamp varchar(255)
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
  'separatorChar' = '|',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
stored as textfile
location 's3://some_company-analytics-warehouse-pro/clients/client_id=conviva/data_frequency=batch/procedure/'
table properties ('skip.header.line.count' = '1');