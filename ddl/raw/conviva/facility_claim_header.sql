create external table raw_conviva.facility_claim_header (
    facility_header_claim_id varchar(255),
    member_id varchar(255),
    patient_id varchar(255),
    place_of_service_code varchar(255),
    place_of_service_description varchar(255),
    claim_processed_date varchar(255),
    transaction_type varchar(255),
    insurance_product_type_desc varchar(255),
    line_of_business_desc varchar(255),
    service_date_from varchar(255),
    service_date_to varchar(255),
    admit_diagnosis_code varchar(255),
    admit_datetime varchar(255),
    admit_type varchar(255),
    source_of_admission varchar(255),
    discharge_datetime varchar(255),
    discharge_status varchar(255),
    accident_date varchar(255),
    drg_code varchar(255),
    network_status varchar(255),
    paid_amount varchar(255),
    allowed_amount varchar(255),
    plan_payment varchar(255),
    member_liability varchar(255),
    total_charges varchar(255),
    total_standard_cost varchar(255),
    admitting_provider_npi varchar(255),
    bill_type varchar(255),
    billing_provider_npi varchar(255),
    referring_provider_npi varchar(255),
    facility_npi varchar(255),
    provider_tin varchar(255),
    apc varchar(255),
    apc_version varchar(255),
    capitated_services_indicator varchar(255),
    professional_claim_id varchar(255),
    principal_diagnosis_code varchar(255),
    principal_diagnosis_icd_indicator varchar(255),
    icd_primary_procedure_code varchar(255),
    icd_primary_procedure_indicator varchar(255),
    servicing_provider_npi varchar(255)
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
  'separatorChar' = '|',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
stored as textfile
location 's3://strive-analytics-warehouse-pro/clients/client_id=conviva/data_frequency=batch/facility_claim_header/'
table properties ('skip.header.line.count' = '1');