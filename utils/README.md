# Utils

## s3_file_copying

All customer files are copied from Platform client service buckets (ex. some_company-ssm-service-data-pro) to the Analytics Warehouse's (AW) bucket. Partitions are then added to the raw AW tables.

An example partition query:

```
alter table raw_ssm.vitals add if not exists partition(client_id='ssm', ingest_date='2020-10-09') location 's3://some_company-analytics-warehouse-pro/clients/client_id=ssm/data_frequency=batch/vitals/ingest_date=2020-10-09/';
```

### Usage

1. Log into an AWS SSO account that has permission to access AWS Secrets Manager.
2. `python3 copy_new_files.py`
3. `python3 add_partitions.py`

### analytics_warehouse.py

This is a Python class for connecting to the Analytics Warehouse Redshift cluster and running queries. It uses the Python package [psycopg2](https://www.psycopg.org/docs/).

### copy_new_files.py

This Python script copies files from the client services into the AW bucket if they have not been previously ingested.

#### Adding new customers and file types

This script currently requires quite a bit of manual work to add new customers.

The most important part of the script is the mapping from source to destination. This can be found in the section:

```
#####################################################
#   Mapping from Source Path to Destination Path    #
#####################################################
```

Otherwise, look through the script for examples of how SSM and Humana are handled. As we add more customers we can simplify this process, but it will semi-tedious as long as clients provide different file types with different schemas. 

### add_partitions.py

This Python script creates the partitions in the AW's raw tables. 
