create external table raw_conviva.facility_claim_line (
    facility_header_claim_id varchar(255),
    line_number varchar(255),
    member_id varchar(255),
    patient_id varchar(255),
    billing_provider_npi varchar(255),
    servicing_provider_npi varchar(255),
    revenue_code varchar(255),
    procedure_code varchar(255),
    procedure_modifier_1 varchar(255),
    procedure_modifier_2 varchar(255),
    procedure_modifier_3 varchar(255),
    procedure_modifier_4 varchar(255),
    ndc_code varchar(255),
    service_date_from varchar(255),
    service_date_to varchar(255),
    service_units varchar(255),
    charge_amount varchar(255),
    total_paid varchar(255),
    allowed_amount varchar(255),
    plan_payment varchar(255),
    member_liability varchar(255),
    standard_cost varchar(255)
)
partitioned by (client_id varchar(50), payer_lob varchar(50), ingest_date timestamp)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
  'separatorChar' = '|',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
stored as textfile
location 's3://some_company-analytics-warehouse-pro/clients/client_id=conviva/data_frequency=batch/facility_claim_line/'
table properties ('skip.header.line.count' = '1');