from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os
import pandas as pd
import numpy as np
from datetime import datetime
import gzip
from elasticsearch import Elasticsearch
import elasticsearch.helpers
import json
from collections import deque
from airflow.hooks.base_hook import BaseHook
from airflow import AirflowException


class StageLogsOperator(BaseOperator):

    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 connection_id='',
                 table_name='',
                 table_create_sql='',
                 csv_header_order='',
                 csv_file_path='',
                 *args, **kwargs):
        '''Constructor'''
        super(StageLogsOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.table_name = table_name
        self.table_create_sql = table_create_sql,
        self.csv_header_order = csv_header_order
        self.csv_file_path = csv_file_path
    
    def stage_log_data(self, postgres):
        '''Creates the logs table (if needed) and loads it from a csv file
           postgres : the object allowing interaction with the postgres DB'''
        postgres.run(self.table_create_sql)
        postgres.run(f"""COPY {self.table_name}({self.csv_header_order}) 
                        FROM '{self.csv_file_path}'
                        DELIMITER ',' CSV HEADER;""")
        sql = f'SELECT count(*) FROM {self.table_name}'
        email_logs_count = postgres.get_records(sql)[0][0]
        self.log.info(f'count of email_logs {email_logs_count}')
        
    def execute(self, context):
        '''Finds the data file for the day of run, connects to postgress, then COPYs the data into postgress.
           
           context : the name of the "Connection" with the host, passwd, etc details defined in Airflow'''
        self.log.info('StageLogsOperator starting')
        
        self.csv_file_path = os.path.join(self.csv_file_path, '{}.csv'.format(context['ds']))

        if os.path.exists(self.csv_file_path):
            self.log.info(f'found {self.csv_file_path}')            
            postgres = PostgresHook(postgres_conn_id=self.connection_id)
            self.log.info('Created connection to postgres')
            self.log.info('Copying data from CSV to postgres')
            self.stage_log_data(postgres)

        else:
            self.log.error(f'{self.csv_file_path} NOT found')
            raise AirflowException(f'{self.csv_file_path} NOT found')