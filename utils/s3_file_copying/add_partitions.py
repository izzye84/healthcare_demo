import time
import json
import logging
import re
from typing import List, Optional, Tuple

import boto3
import psycopg2

from analytics_warehouse import AnalyticsWarehouse

import os
if not os.path.exists('.file_copying_logs'):
    os.makedirs('.file_copying_logs')


FORMAT = '[%(asctime)-15s](%(name)s) %(message)s'
LOGGING_FILENAME = f'.file_copying_logs/add_partitions_{int(time.time())}.log'
logging.basicConfig(filename=LOGGING_FILENAME, level=logging.DEBUG, format=FORMAT)

aw = AnalyticsWarehouse(dbname='some_company-prod', secret_name='AnalyticsWarehouseDataLoadService')

print('Adding partitions to Redshift Spectrum external tables...')

# SSM
key_with_lob_pattern = re.compile(r'(clients/client_id=(\w+)/data_frequency=\w+/(\w+)/lob=(\w+)/insurance_name=(\w+)/ingest_date=(\d+-\d+-\d+)/)')
# Conviva
key_with_payer_lob_pattern = re.compile(r'(clients/client_id=(\w+)/data_frequency=\w+/(\w+)/payer_lob=(\w+)/ingest_date=(\d+-\d+-\d+)/)')
key_no_lob_pattern = re.compile(r'(clients/client_id=(\w+)/data_frequency=\w+/(\w+)/ingest_date=(\d+-\d+-\d+)/)')

aw_bucket_name = 'some_company-analytics-warehouse-pro'
s3 = boto3.resource('s3')
aw_bucket = s3.Bucket(aw_bucket_name)

objs = aw_bucket.objects.filter(Prefix='clients/')
keys = [k.key for k in list(objs)]

# A set of prefixes that have already been added
prefixes = set()

for k in keys:
    m_lob = re.search(key_with_lob_pattern, k)
    if m_lob:
        prefix = m_lob.group(1)
        if prefix not in prefixes:
            prefixes.add(prefix)
            client_id = m_lob.group(2)
            file_type = m_lob.group(3)
            lob = m_lob.group(4)
            lob = lob.replace('_', ' ')
            insurance_name = m_lob.group(5)
            insurance_name = insurance_name.replace('_', ' ')
            ingest_date = m_lob.group(6)
            table_name = f'raw_{client_id}.{file_type}'
            logging.debug(
                (
                    f'Adding partition for table_name={table_name}, '
                    f'prefix={prefix}, client_id={client_id}, '
                    f'ingest_date={ingest_date}, lob={lob}, insurance_name={insurance_name}'
                )
            )
            aw.add_partition(
                table_name,
                prefix,
                client_id,
                ingest_date,
                lob=lob,
                insurance_name=insurance_name
            )
        continue
    m_payer_lob = re.search(key_with_payer_lob_pattern, k)
    if m_payer_lob:
        prefix = m_payer_lob.group(1)
        if prefix not in prefixes:
            prefixes.add(prefix)
            client_id = m_payer_lob.group(2)
            file_type = m_payer_lob.group(3)
            payer_lob = m_payer_lob.group(4)
            ingest_date = m_payer_lob.group(5)
            table_name = f'raw_{client_id}.{file_type}'
            logging.debug(
                (
                    f'Adding partition for table_name={table_name}, '
                    f'prefix={prefix}, client_id={client_id}, '
                    f'ingest_date={ingest_date}, payer_lob={payer_lob}'
                )
            )
            aw.add_partition(
                table_name,
                prefix,
                client_id,
                ingest_date,
                payer_lob=payer_lob
            )
        continue
    m_no_lob = re.search(key_no_lob_pattern, k)
    if m_no_lob:
        prefix = m_no_lob.group(1)
        if prefix not in prefixes:
            prefixes.add(prefix)
            client_id = m_no_lob.group(2)
            file_type = m_no_lob.group(3)
            ingest_date = m_no_lob.group(4)
            table_name = f'raw_{client_id}.{file_type}'
            # Temp filter for pharmacy_claims file that breaks the schema !!!
            if 'pharmacy_claims/ingest_date=2020-06-15' in prefix:
                continue
            logging.debug(
                (
                    f'Adding partition for table_name={table_name}, '
                    f'prefix={prefix}, client_id={client_id}, '
                    f'ingest_date={ingest_date}'
                )
            )
            aw.add_partition(
                table_name,
                prefix,
                client_id,
                ingest_date
            )

print('Finished adding partitions')
print(f'Logs written to {LOGGING_FILENAME}')
