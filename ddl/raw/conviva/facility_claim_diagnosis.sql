create external table raw_conviva.facility_claim_diagnosis (
    facility_header_claim_id varchar(255),
    member_id varchar(255),
    patient_id varchar(255),
    icd_diagnosis_code varchar(255),
    icd_indicator varchar(255),
    icd_diagnosis_code_rank varchar(255)
)
partitioned by (client_id varchar(50), payer_lob varchar(50), ingest_date timestamp)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
  'separatorChar' = '|',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
stored as textfile
location 's3://strive-analytics-warehouse-pro/clients/client_id=conviva/data_frequency=batch/facility_claim_diagnosis/'
table properties ('skip.header.line.count' = '1');