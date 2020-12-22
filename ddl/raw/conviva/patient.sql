create external table raw_conviva.patient (
	patient_id varchar(255),
	patient_account_number varchar(255),
	social_security_number varchar(255),
	first_name varchar(255),
	last_name varchar(255),
	preferred_name varchar(255),
	salutation varchar(255),
	date_of_birth varchar(255),
	gender varchar(255),
	race varchar(255),
	langauge_spoken varchar(255),
	secondary_language_spoken varchar(255),
	death_date varchar(255),
	country_of_birth varchar(255),
	address_line_1 varchar(255),
	address_line_2 varchar(255),
	city varchar(255),
	state varchar(255),
	zip_code varchar(255),
	zip_code_last_4 varchar(255),
	mailing_country varchar(255),
	address_type varchar(255),
	phone_number varchar(255),
	contact_type varchar(255),
	email_address varchar(255),
	pcp_provider_id varchar(255),
	pcp_npi varchar(255),
	created_timestamp timestamp,
	modified_timestamp timestamp
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
  'separatorChar' = '|',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
stored as textfile
location 's3://strive-analytics-warehouse-pro/clients/client_id=conviva/data_frequency=batch/patient/'
table properties ('skip.header.line.count' = '1');