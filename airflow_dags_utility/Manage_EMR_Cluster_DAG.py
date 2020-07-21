from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime
import time
from utility.emr_management import ManageEMR


def setup_reserved_start(**kwargs):
    cluster_details = Variable.get('ehc_cluster_details', deserialize_json=True)
    cluster_id = cluster_details['cluster_id']
    emr = ManageEMR()

    print('Modifying auto scale rule for task node - r4.8x')
    task_r = cluster_details['task_r']
    if task_r['min_instances'] < task_r['max_instances']:
        emr.put_auto_scale_yarn_apps(cluster_id=cluster_id, instance_group_id=task_r['id'],
                                     min_capacity=task_r['max_instances'], max_capacity=task_r['max_instances'],
                                     add_nodes=2, remove_nodes=1, remove_periods=6, remove_apps_periods=6)
    print('Successfully set auto scale for task node - r4.8x')
    time.sleep(120)
    emr.modify_instance_count(cluster_id=cluster_id, instance_group_id=task_r['id'], requested_count=task_r['max_instances'])
    time.sleep(30)

    print('Modifying auto scale rule for task node - m5.12x')
    task_m = cluster_details['task_m']
    if task_m['min_instances'] < task_m['max_instances']:
        emr.put_auto_scale_yarn_apps(cluster_id=cluster_id, instance_group_id=task_m['id'],
                                     min_capacity=task_m['min_instances'], max_capacity=task_m['max_instances'],
                                     add_nodes=2, remove_nodes=1, remove_periods=6, remove_apps_periods=6)
    print('Successfully set auto scale for task node - m5.12x')
    time.sleep(30)


def setup_spot_start(**kwargs):
    cluster_details = Variable.get('ehc_cluster_details', deserialize_json=True)
    cluster_id = cluster_details['cluster_id']
    emr = ManageEMR()

    print('Modifying auto scale rule for task node - r4.16x')
    task_spot = cluster_details['task_spot']
    if task_spot['min_instances'] < task_spot['max_instances']:
        emr.put_auto_scale_yarn_apps(cluster_id=cluster_id, instance_group_id=task_spot['id'], min_capacity=task_spot['min_instances'],
                                    max_capacity=task_spot['max_instances'], add_nodes=15, add_periods=1, remove_nodes=1,
                                    remove_apps_nodes=50, remove_apps_periods=6)
    print('Successfully set auto scale for task node - r4.16x')
    time.sleep(30)


def setup_end(**kwargs):
    cluster_details = Variable.get('ehc_cluster_details', deserialize_json=True)
    cluster_id = cluster_details['cluster_id']
    emr = ManageEMR()

    print('Modifying auto scale rule for task node - r4.16x')
    task_spot = cluster_details['task_spot']
    if task_spot['min_instances'] < task_spot['max_instances']:
        emr.put_auto_scale_yarn_apps(cluster_id=cluster_id, instance_group_id=task_spot['id'], min_capacity=task_spot['min_instances'],
                                    max_capacity=task_spot['max_instances'], add_nodes=5, add_periods=90, remove_nodes=20,
                                    remove_periods=24, remove_apps_nodes=50, remove_apps_periods=1)
    print('Successfully set auto scale for task node - r4.16x')
    time.sleep(30)

    print('Modifying auto scale rule for task node - r4.8x')
    task_r = cluster_details['task_r']
    if task_r['min_instances'] < task_r['max_instances']:
        emr.put_auto_scale_yarn_apps(cluster_id=cluster_id, instance_group_id=task_r['id'], min_capacity=task_r['min_instances'],
                                    max_capacity=task_r['max_instances'], add_nodes=1, add_periods=2, remove_nodes=2,
                                    remove_periods=24, remove_apps_periods=1)
    print('Successfully set auto scale for task node - r4.8x')
    time.sleep(30)

    print('Modifying auto scale rule for task node - m5.12x')
    task_m = cluster_details['task_m']
    if task_m['min_instances'] < task_m['max_instances']:
        emr.put_auto_scale_yarn_apps(cluster_id=cluster_id, instance_group_id=task_m['id'], min_capacity=task_m['min_instances'],
                                    max_capacity=task_m['max_instances'], add_nodes=1, add_periods=1, remove_nodes=1,
                                    remove_periods=24, remove_apps_periods=1)
    print('Successfully set auto scale for task node - m5.12x')
    time.sleep(30)


def sleep(**kwargs):
    print('Sleep for {} seconds'.format(kwargs['seconds']))
    time.sleep(kwargs['seconds'])
    print('Sleep complete')


default_args = {
  'owner': '722224',
  'depends_on_past': False,
  'email': ['arghya.saha@seagate.com', 'anushka.bishnoi@seagate.com'],
  'email_on_failure': True,
  'retries': 0
}

dag = DAG("Manage_EMR_Cluster", description="Manage Auto Scale of EMR ETL cluster", catchup=False,
          default_args=default_args, start_date=datetime(2019, 12, 30), schedule_interval='1 0 * * *')

setup_reserved_at_start = PythonOperator(
    task_id='setup_reserved_for_infosec',
    dag=dag,
    python_callable=setup_reserved_start,
    retries=3
    )

setup_spot_at_start = PythonOperator(
    task_id='setup_spot_for_ehc',
    dag=dag,
    python_callable=setup_spot_start,
    retries=3
    )

setup_at_end = PythonOperator(
    task_id='setup_at_end',
    dag=dag,
    python_callable=setup_end,
    retries=3
    )

sleeping = PythonOperator(
    task_id='sleeping_for_end',
    dag=dag,
    python_callable=sleep,
    retries=0,
    op_kwargs={'seconds': 14400}
    )

sleeping_ehc = PythonOperator(
    task_id='sleeping_for_ehc',
    dag=dag,
    python_callable=sleep,
    retries=0,
    op_kwargs={'seconds': 6600}
    )

setup_reserved_at_start >> sleeping_ehc >> setup_spot_at_start >> sleeping >> setup_at_end
