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


class BlendDataOperator(BaseOperator):

    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 connection_id='',
                 table_name='',
                 table_create_sql='',
                 table_insert_sql='',
                 *args, **kwargs):

        super(BlendDataOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.table_name = table_name
        self.table_create_sql = table_create_sql,
        self.table_insert_sql = table_insert_sql
            
    def execute(self, context):
        self.log.info('BlendDataOperator starting')
        
        try:
            postgres = PostgresHook(postgres_conn_id=self.connection_id)
            self.log.info('Created connection to postgres')
            postgres.run(self.table_create_sql)
            postgres.run("{} WHERE datetime::date = date '{}'".format(self.table_insert_sql,context['ds']) )
            self.log.info('Blended data')
        except Exception as e:
            self.log.error(e)
            raise AirflowException('Error blending data')