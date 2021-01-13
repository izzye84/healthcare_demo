create external table raw_conviva.clinical_encounter (
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_date varchar(255),
    clinical_encounter_facility_name varchar(255),
    clinical_encounter_id varchar(255),
    clinical_encounter_reason varchar(255),
    clinical_encounter_begin_datetime varchar(255),
    clinical_encounter_status varchar(255),
    clinical_encounter_visit_type varchar(255),
    clinical_encounter_end_datetime varchar(255),
    provider_id varchar(255),
    provider_npi varchar(255),
    user_id varchar(255),
    place_of_service_code varchar(255),
    place_of_service_description varchar(255),
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
location 's3://strive-analytics-warehouse-pro/clients/client_id=conviva/data_frequency=batch/clinical_encounter/'
table properties ('skip.header.line.count' = '1');