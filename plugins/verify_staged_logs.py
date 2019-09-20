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


class VerifyStagedLogsOperator(BaseOperator):

    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 connection_id='',
                 table_name='',
                 csv_file_path='',
                 *args, **kwargs):

        super(VerifyStagedLogsOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.table_name = table_name
        self.csv_file_path = csv_file_path

    
    def execute(self, context):
        self.log.info('VerifyStagedLogsOperator starting')
        
        self.csv_file_path = os.path.join(self.csv_file_path, '{}.csv'.format(context['ds']))

        if os.path.exists(self.csv_file_path):
            self.log.info(f'found {self.csv_file_path}')        
            # get file count
            line_count = -1
            with open(self.csv_file_path) as f:
                for line in f:
                    line_count += 1
            # Postgress insert
            postgres = PostgresHook(postgres_conn_id=self.connection_id)
            self.log.info('Created connection to postgres')
            sql = "SELECT count(*) FROM {} WHERE datetime::date = date '{}'".format(self.table_name, context['ds'])
            email_logs_count = postgres.get_records(sql)[0][0]
            self.log.info(f'count of email_logs {email_logs_count} vs count of file lines {line_count}')
            if email_logs_count != line_count:
                self.log.error('Data mismatch in db vs file count')
                raise AirflowException('Data mismatch in db vs file count')

        else:
            self.log.error(f'{self.csv_file_path} NOT found')
            raise AirflowException(f'{self.csv_file_path} NOT found')