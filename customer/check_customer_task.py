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


class CheckCustomer:
    TABLE_NAME = "customers"

    COUNT_CUSTOMER_SQL = "Select count(*) as customer_number from {} as cus where cus.cloud_pos_uuid='{}'"

    def select_customer_string(self, **context):
        """
        Cau truy van dem customer theo cloud_pos_uuid
        """
        json_pay_load = json.loads(json.dumps(context['dag_run'].conf['data']))

        return self.COUNT_CUSTOMER_SQL.format(self.TABLE_NAME, json_pay_load["uuid"])

    def get_customer(sefl, **context):
        """
        Thuc thi cau truy van dem customer theo cloud_pos_uuid
        """
        get_cus_task = MySqlHook(
            task_id='get_pio_customer',
            provide_context=True,
            mysql_conn_id='mysql_piospa_conn',
            dag=dag)

        return get_cus_task.get_records(sefl.select_customer_string(**context))

    def get_customer_records(**kwargs):
        """
        Kiem tra ket qua ket qua
        neu result > 0 : customer da ton tai, cap nhat lai thong tin cua customer
        neu result == 0: tao customer
        """
        ti = kwargs['ti']
        xcom = ti.xcom_pull(task_ids='get_customer_task')
        string_to_print = 'Value in xcom is: {}'.format(xcom)
        # Get data in your logs
        logging.info(string_to_print)
        logging.info(json.dumps(xcom))
        json_pay_load = json.loads(json.dumps(xcom))
        logging.info(json.dumps(json_pay_load))
        return 'customer_created' if json_pay_load[0][0] == 0 else 'end_follow'
