create external table raw_humana.premium (
    cms_premium numeric(18,2),
    cms_premium_seq_adj numeric(18,2),
    cov_month varchar(255),
    pers_gen_key varchar(255),
    recon_ma_risk_score_nbr numeric(18,3),
    src_mbr_id varchar(255),
    src_platform_cd varchar(255)
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
stored as textfile
location 's3://some_company-analytics-warehouse-pro/clients/client_id=humana/data_frequency=batch/premium/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_humana.premium add
partition(client_id='humana', ingest_date='2020-06-15') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=humana/data_frequency=batch/premium/ingest_date=2020-06-15/';