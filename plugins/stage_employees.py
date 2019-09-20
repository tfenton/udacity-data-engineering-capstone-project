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


class StageEmployeesOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 connection_id='',
                 table_name='employees',
                 min_table_size=0,
                 table_create_sql='',
                 table_drop_sql='',
                 csv_header_order='',
                 csv_file_path='',
                 *args, **kwargs):
        '''Constructor'''
        super(StageEmployeesOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.table_name = table_name
        self.min_table_size = min_table_size
        self.table_create_sql = table_create_sql,
        self.table_drop_sql = table_drop_sql
        self.csv_header_order = csv_header_order
        self.csv_file_path = csv_file_path

    def validate_employee_data(self, postgres):
        '''Queries the employees table and if it finds >self.min_table_size employees then we consider the data there good
           
           postgres : the connected postgres hook which allows a query to be run'''
        
        sql = f'SELECT count(*) FROM {self.table_name}'
        employees_count = postgres.get_records(sql)[0][0]
        self.log.info(f'count of employees {employees_count}')
        if employees_count < self.min_table_size:
            return False
        else:
            return True
    
    def stage_employee_data(self, postgres):
        '''Recreates the employees table and loads it from a csv file
           
           postgres : the connected postgres hook which allows a query to be run'''
        postgres.run(self.table_drop_sql)
        postgres.run(self.table_create_sql)
        posgres.run(f"""COPY {self.table_name}({self.csv_header_order}) 
                        FROM '{self.csv_file_path}'
                        DELIMITER ',' CSV HEADER;""")
        sql = f'SELECT count(*) FROM {self.table_name}'
        employees_count = postgres.get_records(sql)[0][0]
        self.log.info(f'count of email_logs {employees_count}')
        
    def execute(self, context):
        '''Finds the data file for the employees, connects to postgress, then COPYs the data into postgress.
           
           context : the name of the "Connection" with the host, passwd, etc details defined in Airflow'''
        self.log.info('StageEmployeesOperator starting')
        
        if os.path.exists(self.csv_file_path):
            self.log.info(f'found {self.csv_file_path}')
            # Postgress insert
            postgres = PostgresHook(postgres_conn_id=self.connection_id)
            self.log.info('Created connection to postgres')
            self.log.info('Copying data from CSV to postgres')
            if not self.validate_employee_data(postgres):
                self.stage_employee_data(postgres)

        else:
            self.log.error(f'{self.csv_file_path} NOT found')
            raise AirflowException(f'{self.csv_file_path} NOT found')