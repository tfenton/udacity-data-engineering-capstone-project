from datetime import datetime, timedelta
import os
from airflow import DAG
import airflow
from airflow.operators.dummy_operator import DummyOperator
from stage_employees import StageEmployeesOperator
from stage_logs import StageLogsOperator
from helpers.sql import Sql
from blend_data import BlendDataOperator
from verify_staged_logs import VerifyStagedLogsOperator
from elasticsearch_push import ElasticsearchOperator

default_args = {
    'owner': 'tim',
    'start_date': datetime(2019, 5, 1),
    'end_date' : datetime(2019, 9, 30),
}

dag = DAG('proof_point',
          default_args=default_args,
          description='Load and transform archive files into postgres and elasticsearch',
          schedule_interval='40 19 * * *',
          catchup=True,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_employees_operator = StageEmployeesOperator(
                                        task_id='stage_employee_data', 
                                        dag=dag,
                                        connection_id='postgres',
                                        table_name='employees',
                                        min_table_size=48000,
                                        table_create_sql=Sql.employees_table_create,
                                        table_drop_sql=Sql.employees_table_drop,
                                        csv_header_order=Sql.employees_csv_header_order,
                                        csv_file_path='/encrypted_vol/jupyter_data/airflow/data/employees.csv')

stage_logs_operator = StageLogsOperator(
                                        task_id='stage_log_data', 
                                        dag=dag,
                                        connection_id='postgres',
                                        table_name='email_logs',
                                        table_create_sql=Sql.email_log_table_create,
                                        csv_header_order=Sql.email_log_csv_header_order,
                                        csv_file_path='/encrypted_vol/jupyter_data/airflow/data/')

blend_data_operator = BlendDataOperator(
                                        task_id='blend_data', 
                                        dag=dag,
                                        connection_id='postgres',
                                        table_name='blended_email_logs',
                                        table_create_sql=Sql.blended_data_table_create,
                                        table_insert_sql=Sql.blended_data_insert)

verify_staged_logs_operator = VerifyStagedLogsOperator(
                                        task_id='verify_staged_logs', 
                                        dag=dag,
                                        connection_id='postgres',
                                        table_name='email_logs',
                                        csv_file_path='/encrypted_vol/jupyter_data/airflow/data/')

elasticsearch_operator = ElasticsearchOperator(
                                        task_id='push_to_elasticsearch', 
                                        dag=dag,
                                        connection_id='postgres',
                                        index_name='blended_email_logs',
                                        blended_data_select=Sql.blended_data_select,
                                        blended_data_headers=Sql.blended_data_headers,
                                        cert_file_path='/encrypted_vol/jupyter_data/ssl/combined.pem')

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# order the dependencies
start_operator >> stage_employees_operator
start_operator >> stage_logs_operator
stage_employees_operator >> blend_data_operator
stage_logs_operator >> verify_staged_logs_operator
verify_staged_logs_operator >> blend_data_operator
blend_data_operator >> elasticsearch_operator
elasticsearch_operator >> end_operator