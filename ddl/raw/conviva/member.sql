create external table raw_conviva.member (
    member_id varchar(255),
    patient_id varchar(255),
    eligibility_id varchar(255),
    social_security_number varchar(255),
    first_name varchar(255),
    last_name varchar(255),
    date_of_birth varchar(255),
    gender varchar(255),
    race varchar(255),
    relationship varchar(255),
    address_line_1 varchar(255),
    address_line_2 varchar(255),
    city varchar(255),
    state varchar(255),
    zip_code varchar(255),
    zip_code_last_4 varchar(255),
    mailing_country varchar(255),
    phone_number varchar(255),
    email_address varchar(255),
    primary_care_provider_npi varchar(255),
    market varchar(255),
    primary_care_location varchar(255)
)
partitioned by (client_id varchar(50), payer_lob varchar(50), ingest_date timestamp)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
  'separatorChar' = '|',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
stored as textfile
location 's3://some_company-analytics-warehouse-pro/clients/client_id=conviva/data_frequency=batch/member/'
table properties ('skip.header.line.count' = '1');