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
LOGGING_FILENAME = f'.file_copying_logs/copy_new_files_{int(time.time())}.log'
logging.basicConfig(filename=LOGGING_FILENAME, level=logging.DEBUG, format=FORMAT)


#####################################################
#           AW Schemas and Tables                   #
#####################################################

print('Connecting to the Analytics Warehouse...')
aw = AnalyticsWarehouse(dbname='strive-prod', secret_name='analytics_warehouse_inference_service_user')
print('Connected\n')

raw_schemas = aw.list_raw_schemas()
logging.debug(f'Raw schemas: {raw_schemas}')
print('Raw Schemas')
print('-----------------------------')
for rs in raw_schemas:
    print(rs)
print('-----------------------------\n')

tables = {s: [] for s in raw_schemas}
for rs in raw_schemas:
    tables[rs] = [t[1] for t in aw.list_tables(rs)]
logging.debug(f'Tables: {tables}')
print('Tables')
print('-----------------------------')
for rs in raw_schemas:
    print(rs)
    for t in tables[rs]:
        print(f'\t{t}')
print('-----------------------------\n')

#####################################################
#               Ingested Files                      #
#####################################################

print('Querying the Analytics Warehouse for files that have already been ingested...')
ingested_files = {s: [] for s in raw_schemas}
for k in tables.keys():
    for t in tables[k]:
        ingested_files[k].append(aw.list_ingested_files(f'{k}.{t}'))

ssm_ingested_files = {
    re.search(r's3://strive-analytics-warehouse-pro/(.+)', f[0]).group(1) for fs in ingested_files['raw_ssm'] for f in fs
}
humana_ingested_files = {
    re.search(r's3://strive-analytics-warehouse-pro/(.+)', f[0]).group(1) for fs in ingested_files['raw_humana'] for f in fs
}
print('Finished querying ingested files')

# Log results
log_str = 'SSM files that have already been ingested:\n'
for f in ssm_ingested_files:
    log_str += f'{f}\n'
logging.debug(log_str)
log_str = 'Humana files that have already been ingested:\n'
for f in humana_ingested_files:
    log_str += f'{f}\n'
logging.debug(log_str)

#####################################################
#   Mapping from Source Path to Destination Path    #
#####################################################

def ingest_date_transform(d: str) -> str:
    return d[:4] + '-' + d[4:6] + '-' + d[6:]

# Maps the source path "payer_lob" to "lob" and "insurance_name" for the output path
ssm_payer_lob_to_lob_insurance_name_mapping = {
    'anthem_commercial': ('Commercial', 'Anthem'),
    'anthem_ma': ('Medicare_Advantage', 'Anthem'),
    'cigna_commercial': ('Commercial', 'Cigna'),
    'coventry_ma': ('Medicare_Advantage', 'Coventry'),
    'mssp': ('Medicare_FFS', 'MSSP_Missouri'),
    'uhc_commercial': ('Commercial', 'United_Healthcare'),
    'cigna': ('Commercial', 'Cigna'),
    'exclusive_choice': ('EHP_STL', 'Exclusive_Choice'),
    # 'saints_care': (),
    # 'smgs': (),
    'uhc_ma': ('Medicare_Advantage', 'United_Healthcare')
}

mapping = {
    'ssm': {
        'allergies': {
            'src': re.compile(r'.+/ingest_id=D(\d+)\.T\d+/(ALG\.SSM\.D\d+\.T\d+\.txt)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'allergies/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'claim_detail': {
            'src': re.compile(r'claim_detail/schema_id=1/ingest_id=D(\d+)\.T\d+/payer_lob=(\w+)/(.+)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'claim_detail/'
                'lob={lob}/'
                'insurance_name={insurance_name}/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'claim_header': {
            'src': re.compile(r'claim_header/schema_id=1/ingest_id=D(\d+)\.T\d+/payer_lob=(\w+)/(.+)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'claim_header/'
                'lob={lob}/'
                'insurance_name={insurance_name}/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'diagnosis': {
            'src': re.compile(r'.+/ingest_id=D(\d+)\.T\d+/(DG\.SSM\.D\d+\.T\d+\.txt)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'diagnosis/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'encounter': {
            'src': re.compile(r'.+/ingest_id=D(\d+)\.T\d+/(CE\.SSM\.D\d+\.T\d+\.txt)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'encounter/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'form': {
            'src': re.compile(r'.+/ingest_id=D(\d+)\.T\d+/(FO\.SSM\.D\d+\.T\d+\.txt)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'form/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'immunization': {
            'src': re.compile(r'.+/ingest_id=D(\d+)\.T\d+/(IMM\.SSM\.D\d+\.T\d+\.txt)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'immunization/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'labs': {
            'src': re.compile(r'.+/ingest_id=D(\d+)\.T\d+/(LAB\.SSM\.D\d+\.T\d+\.txt)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'labs/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'medication': {
            'src': re.compile(r'.+/ingest_id=D(\d+)\.T\d+/(MED\.SSM\.D\d+\.T\d+\.txt)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'medication/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'member_crosswalk': {
            'src': re.compile(r'crosswalk/schema_id=1/ingest_id=D(\d+)\.T\d+/(.+)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'member_crosswalk/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'member_eligibility': {
            'src': re.compile(r'member_eligibility/schema_id=1/ingest_id=D(\d+)\.T\d+/payer_lob=\w+/(.+)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'member_eligibility/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'patient': {
            'src': re.compile(r'.+/ingest_id=D(\d+)\.T\d+/(PAT\.SSM\.D\d+\.T\d+\.txt)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'patient/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'payer': {
            'src': re.compile(r'.+/ingest_id=D(\d+)\.T\d+/(PAY\.SSM\.D\d+\.T\d+\.txt)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'payer/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'pharmacy_claim': {
            'src': re.compile(r'pharmacy_claim/schema_id=1/ingest_id=D(\d+)\.T\d+/payer_lob=(\w+)/(.+)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'pharmacy_claims/'
                'lob={lob}/'
                'insurance_name={insurance_name}/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'procedure': {
            'src': re.compile(r'.+/ingest_id=D(\d+)\.T\d+/(PROC\.SSM\.D\d+\.T\d+\.txt)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'procedure/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'provider': {
            'src': re.compile(r'.+/ingest_id=D(\d+)\.T\d+/(PROV\.SSM\.D\d+\.T\d+\.txt)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'provider/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'referral': {
            'src': re.compile(r'.+/ingest_id=D(\d+)\.T\d+/(REF\.SSM\.D\d+\.T\d+\.txt)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'referral/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'surgical_history': {
            'src': re.compile(r'.+/ingest_id=D(\d+)\.T\d+/(SH\.SSM\.D\d+\.T\d+\.txt)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'surgical_history/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'vitals': {
            'src': re.compile(r'.+/ingest_id=D(\d+)\.T\d+/(VIT\.SSM\.D\d+\.T\d+\.txt)'),
            'dst': (
                'clients/'
                'client_id=ssm/'
                'data_frequency=batch/'
                'vitals/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        }
    },
    'humana': {
        'authorization_report': {
            'src': re.compile(r'authorization_report/schema_id=1/ingest_id=D(\d+)\.T\d+/([\w\s]+\.csv)'),
            'dst': (
                'clients/'
                'client_id=humana/'
                'data_frequency=batch/'
                'authorization_report/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'demographics': {
            'src': re.compile(r'demographics/schema_id=1/ingest_id=D(\d+)\.T\d+/([\w\s]+\.csv)'),
            'dst': (
                'clients/'
                'client_id=humana/'
                'data_frequency=batch/'
                'demographics/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'disenrollment': {
            'src': re.compile(r'disenrollment/schema_id=1/ingest_id=D(\d+)\.T\d+/([\w\s]+\.csv)'),
            'dst': (
                'clients/'
                'client_id=humana/'
                'data_frequency=batch/'
                'disenrollment/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'lab_claims': {
            'src': re.compile(r'lab_claims/schema_id=1/ingest_id=D(\d+)\.T\d+/([\w\s]+\.csv)'),
            'dst': (
                'clients/'
                'client_id=humana/'
                'data_frequency=batch/'
                'lab_claims/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'medical_claims': {
            'src': re.compile(r'medical_claims/schema_id=1/ingest_id=D(\d+)\.T\d+/([\w\s]+\.csv)'),
            'dst': (
                'clients/'
                'client_id=humana/'
                'data_frequency=batch/'
                'medical_claims/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'pharmacy_claims': {
            'src': re.compile(r'pharmacy_claims/schema_id=1/ingest_id=D(\d+)\.T\d+/([\w\s]+\.csv)'),
            'dst': (
                'clients/'
                'client_id=humana/'
                'data_frequency=batch/'
                'pharmacy_claims/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'premium': {
            'src': re.compile(r'premium/schema_id=1/ingest_id=D(\d+)\.T\d+/([\w\s]+\.csv)'),
            'dst': (
                'clients/'
                'client_id=humana/'
                'data_frequency=batch/'
                'premium/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        },
        'referral': {
            'src': re.compile(r'referral/schema_id=1/ingest_id=D(\d+)\.T\d+/([\w\s]+\.csv)'),
            'dst': (
                'clients/'
                'client_id=humana/'
                'data_frequency=batch/'
                'referral/'
                'ingest_date={ingest_date}/'
                '{src_filename}'
            )
        }
    }
}

#####################################################
#          List and Filter Source Files             #
#####################################################

print('Getting a list of files from client service data buckets in S3...')

# SSM

s3 = boto3.resource('s3')

ssm_bucket = s3.Bucket('strive-ssm-service-data-pro')
ssm_objs = ssm_bucket.objects.all()
ssm_sources = [
    {
        'Bucket': 'strive-ssm-service-data-pro',
        'Key': obj.key
    }
    for obj in ssm_objs 
    if obj.key[-1] != '/' 
    and 'encrypted_files' not in obj.key 
    and 'decrypted_files' not in obj.key 
    and 'navvis_ssm_roster' not in obj.key
    and 'ssm_epic_roster' not in obj.key
    and 'patient_identifier' not in obj.key
]

# Log results
log_str = 'SSM source files:\n'
for ss in ssm_sources:
    log_str += f'{ss}\n'
logging.debug(log_str)

ssm_src_dst = []

for source in ssm_sources:
    for k, v in mapping['ssm'].items():
        m = re.search(v['src'], source['Key'])
        if m:
            if 'insurance_name' in v['dst']:
                try:
                    params = dict()
                    params['ingest_date'] = ingest_date_transform(m.group(1))
                    params['lob'], params['insurance_name'] = ssm_payer_lob_to_lob_insurance_name_mapping[m.group(2)]
                    params['src_filename'] = m.group(3)
                    dst = v['dst'].format(**params)
                    ssm_src_dst.append({'src': source, 'dst': dst})
                except KeyError as ke:
                    print('EXCEPTION raised while parsing a source key')
                    print(f'KeyError: {ke}')
            else:
                try:
                    params = dict()
                    params['ingest_date'] = ingest_date_transform(m.group(1))
                    params['src_filename'] = m.group(2)
                    dst = v['dst'].format(**params)
                    ssm_src_dst.append({'src': source, 'dst': dst})
                except Exception as e:
                    print('EXCEPTION raised while parsing a source key')
                    print(e)
                    print(m.group(0))
            break

ssm_new_files = []
for src_dst in ssm_src_dst:
    dst = src_dst['dst']
    if dst not in ssm_ingested_files:
        ssm_new_files.append(src_dst)

# Log results
log_str = 'SSM files to copy:\n'
for snf in ssm_new_files:
    log_str += f'{snf}\n'
logging.debug(log_str)

# HUMANA

humana_bucket = s3.Bucket('strive-humana-service-data-pro')
humana_objs = humana_bucket.objects.all()
humana_sources = [
    {
        'Bucket': 'strive-humana-service-data-pro',
        'Key': obj.key
    }
    for obj in humana_objs 
    if obj.key[-1] != '/'
]

# Log results
log_str = 'Humana source files:\n'
for hs in humana_sources:
    log_str += f'{hs}\n'
logging.debug(log_str)

humana_src_dst = []

for source in humana_sources:
    for k, v in mapping['humana'].items():
        m = re.search(v['src'], source['Key'])
        if m:
            try:
                params = dict()
                params['ingest_date'] = ingest_date_transform(m.group(1))
                params['src_filename'] = m.group(2)
                dst = v['dst'].format(**params)
                humana_src_dst.append({'src': source, 'dst': dst})
            except Exception as e:
                print('EXCEPTION raised while parsing a source key')
                print('----------')
                print(e)
                print(m.group(0))
                print('----------')
            break

humana_new_files = []
for src_dst in humana_src_dst:
    dst = src_dst['dst']
    if dst not in humana_ingested_files:
        humana_new_files.append(src_dst)

# Log results
log_str = 'Humana files to copy:\n'
for hnf in humana_new_files:
    log_str += f'{hnf}\n'
logging.debug(log_str)

print('Finished getting the list of files')

#####################################################
#          Copy New Files to AW Bucket              #
#####################################################

aw_bucket_name = 'strive-analytics-warehouse-pro'
print(f'Copying new files to {aw_bucket_name}...')
aw_bucket = s3.Bucket(aw_bucket_name)

new_files = ssm_new_files + humana_new_files

for nf in new_files:
    src = nf['src']
    dst = nf['dst']
    dst_obj = aw_bucket.Object(dst)
    logging.debug(f'Copying file from {src} to {dst}')
    dst_obj.copy(src)

print('Finished copying files\n')
print(f'Logs written to {LOGGING_FILENAME}')
