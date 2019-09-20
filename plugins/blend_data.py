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
                 table_create_sql='',
                 table_insert_sql='',
                 *args, **kwargs):
        ''''Constructor
            table_create_sql : The sql to create the blended_email_logs table if it doesn't exist
            table_insert_sql : The sql to perform the join between the employees and email_logs tables.'''

        super(BlendDataOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.table_create_sql = table_create_sql,
        self.table_insert_sql = table_insert_sql
            
    def execute(self, context):
        '''This operator establishes a connection with postgres, then runs a SQL statement to join 
           the data from the employee table and the email_logs table. This joined data goes into a 
           3rd table called blended_email_logs.'''
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