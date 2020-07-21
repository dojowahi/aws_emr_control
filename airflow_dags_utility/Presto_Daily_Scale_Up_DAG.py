from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from utility.presto_graceful_shutdown import PrestoGracefulShutdown


def scale_up(**kwargs):
    PrestoGracefulShutdown().scale_up()


default_args = {
  'owner': '722224',
  'depends_on_past': False,
  'email': ['arghya.saha@seagate.com', 'anushka.bishnoi@seagate.com'],
  'email_on_failure': True,
  'retries': 3
}

dag = DAG("Presto_Daily_Scale_Up", description="Presto Daily Scale Up", catchup=False,
          default_args=default_args, start_date=datetime(2019, 12, 30), schedule_interval='0 9 * * *')

task_dummy = DummyOperator(task_id="dummy", dag=dag)

daily_scale_up = PythonOperator(
    task_id='scale_up_nodes',
    dag=dag,
    python_callable=scale_up,
    retries=3
    )

task_dummy >> daily_scale_up
