create external table raw_conviva.vitals (
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_id varchar(255),
    vitals_captured_by varchar(255),
    vitals_name varchar(255),
    vitals_value varchar(255),
    vitals_unit varchar(255),
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
location 's3://some_company-analytics-warehouse-pro/clients/client_id=conviva/data_frequency=batch/vitals/'
table properties ('skip.header.line.count' = '1');