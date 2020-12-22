create external table raw_conviva.diagnosis (
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_id varchar(255),
    diagnosis_date varchar(255),
    diagnosis_code varchar(255),
    onset_date varchar(255),
    resolved_date varchar(255),
    code_set varchar(255),
    diagnosis_name varchar(255),
    service_date varchar(255),
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
location 's3://strive-analytics-warehouse-pro/clients/client_id=conviva/data_frequency=batch/diagnosis/'
table properties ('skip.header.line.count' = '1');