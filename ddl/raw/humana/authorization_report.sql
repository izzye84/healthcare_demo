create external table raw_humana.authorization_report (
    type varchar(255),
    src_auth_id varchar(255),
    initialnotificationdate varchar(255),
    additionalrodrequestdate varchar(255),
    auth_status varchar(255),
    auth_admission_date varchar(255),
    auth_discharge_date varchar(255),
    expected_discharge_dt varchar(255),
    authorizeddays numeric(18,0),
    rodstartdate varchar(255),
    rodenddate varchar(255),
    admission_type varchar(255),
    bed_type varchar(255),
    category varchar(255),
    primarydiagcode varchar(255),
    primarydiagdescription varchar(255),
    mbr_consolidatedsellingmarket varchar(255),
    mbr_coverageenddate varchar(255),
    mbr_dateofbirth varchar(255),
    mbr_subscriber_id varchar(255),
    mbr_memberiddependent_cd varchar(255),
    mbr_employergroupname varchar(255),
    mbr_employergroupnumber varchar(255),
    mbr_firstname varchar(255),
    mbr_lastname varchar(255),
    mbr_grouperid varchar(255),
    mbr_groupername varchar(255),
    mbrpersgenkey varchar(255),
    mbr_state varchar(255),
    fac_dbaname varchar(255),
    fac_taxid varchar(255),
    fac_npiid varchar(255),
    fac_s_state varchar(255),
    tre_dbaname varchar(255),
    tre_taxid varchar(255),
    tre_npiid varchar(255),
    req_dbaname varchar(255),
    req_taxid varchar(255),
    req_npiid varchar(255),
    discharge_disposition varchar(255)
)
partitioned by (client_id varchar(50), ingest_date timestamp)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
stored as textfile
location 's3://some_company-analytics-warehouse-pro/clients/client_id=humana/data_frequency=batch/authorization_report/'
table properties ('skip.header.line.count' = '1');

/* The following statement is an example of what needs to be run for each partition to ensure all data is loaded into the external table */

alter table raw_humana.authorization_report add
partition(client_id='humana', ingest_date='2020-07-31') 
location 's3://some_company-analytics-warehouse-pro/clients/client_id=humana/data_frequency=batch/authorization_report/ingest_date=2020-07-31/';