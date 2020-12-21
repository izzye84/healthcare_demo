import time
import json
import logging
import re
from typing import List, Optional, Tuple

import boto3
import psycopg2


class AnalyticsWarehouse:
    """ Connect to the Strive Redshift Analytics Warehouse and run queries """
    def __init__(self, dbname: str, secret_name: str):
        self.secret_name = secret_name
        creds = self.get_secret()
        creds['dbname'] = dbname
        if creds.get('username'):
            creds['user'] = creds['username']
            del creds['username']
        if creds.get('engine'):
            del creds['engine']
        if creds.get('dbClusterIdentifier'):
            del creds['dbClusterIdentifier']
        self.creds = creds
        self.conn = psycopg2.connect(**creds)
        self.conn.set_session(autocommit=True)
        
    def get_secret(self) -> dict:
        region_name = "us-east-1"
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        response = client.get_secret_value(
            SecretId=self.secret_name
        )
        return json.loads(response['SecretString'])
    
    def list_raw_schemas(self) -> List[Tuple]:
        query = '''
            select s.nspname as table_schema,
            s.oid as schema_id,  
            u.usename as owner
            from pg_catalog.pg_namespace s
            join pg_catalog.pg_user u on u.usesysid = s.nspowner
            order by table_schema;
        '''
        result = self._select_all_query(query)
        return [s[0] for s in result if 'raw' in s[0]]

    def list_tables(self, schema: str) -> List[Tuple]:
        query = (
            "select * "
            "from pg_catalog.svv_external_tables "
            f"where schemaname = '{schema}';"
        )
        return self._select_all_query(query)
    
    def list_ingested_files(self, table: str) -> List[Tuple]:
        return self._select_all_query(f'select distinct "$path" from {table};')
    
    def add_partition(
        self, 
        table_name: str,
        prefix: str,
        client_id: str, 
        ingest_date: str, 
        lob: Optional[str]= None, 
        insurance_name: Optional[str] = None
    ) -> None:
        query_with_lob = (
            "alter table {} add if not exists "
            "partition(client_id='{}', lob='{}', insurance_name='{}', ingest_date='{}') "
            "location 's3://strive-analytics-warehouse-pro/{}';"
        )
        query_no_lob = (
            "alter table {} add if not exists "
            "partition(client_id='{}', ingest_date='{}') "
            "location 's3://strive-analytics-warehouse-pro/{}';"
        )
        if lob:
            query = query_with_lob.format(table_name, client_id, lob, insurance_name, ingest_date, prefix)
        else:
            query = query_no_lob.format(table_name, client_id, ingest_date, prefix)
        self._no_return_query(query)
    
    def _select_all_query(self, query: str) -> List[Tuple]:
        logging.debug(f'Query: {query}')
        cur = self.conn.cursor()
        cur.execute(query)
        result = cur.fetchall()
        cur.close()
        return result
    
    def _no_return_query(self, query: str) -> None:
        logging.debug(f'Query: {query}')
        cur = self.conn.cursor()
        cur.execute(query)
        cur.close()

    def close_connection(self) -> None:
        self.conn.close()
