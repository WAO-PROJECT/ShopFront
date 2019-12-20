import json
import airflow
import logging
from airflow.models import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator

class InsertCustomer:
    TABLE_NAME = "customers"

    INSERT_CUSTOMER_SQL = """
                 Insert into {}(customer_group_id, full_name, birthday, gender, phone1, is_actived, 
                 is_deleted, created_by, updated_by, created_at, updated_at, customer_code, cloud_pos_uuid) 
                 values('{}', '{}', {}, {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}' , {}, '{}')
                 """

    def insert_sql(self, **context):
        """
        Cau truy van them customer vao db piospa
        """
        json_pay_load = json.loads(json.dumps(context['dag_run'].conf['data']))

        return self.INSERT_CUSTOMER_SQL.format(self.TABLE_NAME,
                                               0,
                                               json_pay_load["last_name"],
                                               'NULL' if json_pay_load["birthday"] is None else "'{}'".format(
                                                   json_pay_load["birthday"]),
                                               'NULL' if json_pay_load["gender"] is None else "'{}'".format(
                                                   json_pay_load["gender"]),
                                               "0000000000",
                                               1,
                                               0,
                                               0,
                                               0,
                                               json_pay_load["created_at"],
                                               json_pay_load["updated_at"],
                                               'NULL' if json_pay_load["code"] is None else "'{}'".format(
                                                   json_pay_load["code"]),
                                               json_pay_load["uuid"])

    def insert_customer(self, **context):
        """
        Thuc thi task them customer
        """
        sql_query = self.insert_sql(**context)

        insert_mysql_task = MySqlHook(
            task_id='insert_pio_customer',
            mysql_conn_id='mysql_piospa_conn',
            dag=dag)
        insert_mysql_task.run(sql_query)

