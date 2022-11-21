create external table raw_conviva.lab (
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_id varchar(255),
    lab_id varchar(255),
    lab_code_set varchar(255),
    lab_code varchar(255),
    lab_order_date varchar(255),
    lab_order_description varchar(255),
    service_code varchar(255),
    service_code_modifier varchar(255),
    service_code_type varchar(255),
    service_code_desc varchar(255),
    result_description varchar(255),
    collection_date varchar(255),
    result_date varchar(255),
    result_text varchar(255),
    result_numeric varchar(255),
    result_pos_neg varchar(255),
    unit varchar(255),
    reference_range varchar(255),
    abnormal_flag varchar(255),
    result_status varchar(255),
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
location 's3://some_company-analytics-warehouse-pro/clients/client_id=conviva/data_frequency=batch/lab/'
table properties ('skip.header.line.count' = '1');