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


class ElasticsearchOperator(BaseOperator):

    ui_color = '#89DA59'
    
    def getAzureClient(self):
        return Elasticsearch(
                BaseHook.get_connection(self.connection_id).host,
                http_auth=(BaseHook.get_connection(self.connection_id).login,
                           BaseHook.get_connection(self.connection_id).password),
                use_ssl=True,
                ca_certs=self.cert_file_path,
                verify_certs=True)


    @apply_defaults
    def __init__(self,
                 connection_id='',
                 index_name='blended_email_logs',
                 cert_file_path='',
                 blended_data_select='',
                 blended_data_headers='',
                 *args, **kwargs):

        super(ElasticsearchOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.index_name = index_name
        self.cert_file_path = cert_file_path
        self.blended_data_select = blended_data_select
        self.blended_data_headers = blended_data_headers
        
    def execute(self, context):
        ''''''
        self.log.info('ElasticsearchOperator starting')
        try:
            client = getAzureClient()
            postgres = PostgresHook(postgres_conn_id=self.connection_id)
            self.log.info('Created connection to postgres')
            postgres.run( self.blended_data_select + 
                         "WHERE datetime::date = date '{}'".format(context['ds']) )
            for log in postgres.get_records(sql):
                doc = {}
                for i, header in enumerate(self.blended_data_headers):
                    doc[header] = log[i]
                client.index(self.index_name, doc)
        except Exception as e:
            self.log.error(e)
            raise AirflowException('Unable to insert data into ElasticSearch')