from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from utility.presto_graceful_shutdown import PrestoGracefulShutdown


def scale_down(**kwargs):
    PrestoGracefulShutdown(timeout=720).scale_down()


default_args = {
  'owner': '722224',
  'depends_on_past': False,
  'email': ['arghya.saha@seagate.com', 'anushka.bishnoi@seagate.com'],
  'email_on_failure': True,
  'retries': 0
}

dag = DAG("Presto_Daily_Scale_Down_Gracefully", description="Presto Daily Scale Down Gracefully", catchup=False,
          default_args=default_args, start_date=datetime(2019, 12, 30), schedule_interval='0 23 * * *')

task_dummy = DummyOperator(task_id="dummy", dag=dag)

daily_scale_down = PythonOperator(
    task_id='scale_down_nodes',
    dag=dag,
    python_callable=scale_down,
    retries=0
    )

task_dummy >> daily_scale_down
