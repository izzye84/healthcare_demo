create external table raw_conviva.eligibility (
    member_id varchar(255),
    patient_id varchar(255),
    id varchar(255),
    coverage_begin_date varchar(255),
    coverage_end_date varchar(255),
    contract_number_type_indicator varchar(255),
    contract_number varchar(255),
    group_id varchar(255),
    hospice_begin_date varchar(255),
    hospice_end_date varchar(255),
    insurance_type varchar(255),
    payer varchar(255),
    plan_code varchar(255),
    original_entitlement_reason varchar(255),
    dual_status_indicator varchar(255),
    patient_account_number varchar(255)
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
  'separatorChar' = '|',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
stored as textfile
location 's3://strive-analytics-warehouse-pro/clients/client_id=conviva/data_frequency=batch/eligibility/'
table properties ('skip.header.line.count' = '1');