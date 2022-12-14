create external table raw_humana.disenrollment (
    mbr_pers_gen_key varchar(255),
    idcard_mbr_id varchar(255),
    pers_first_name varchar(255),
    pers_last_name varchar(255),
    ineligible_date varchar(255),
    inelig_reason varchar(255),
    lob varchar(255)
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
stored as textfile
location 's3://some_company-analytics-warehouse-pro/clients/client_id=humana/data_frequency=batch/disenrollment/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_humana.disenrollment add
partition(client_id='humana', ingest_date='2020-07-17') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=humana/data_frequency=batch/disenrollment/ingest_date=2020-07-17/';