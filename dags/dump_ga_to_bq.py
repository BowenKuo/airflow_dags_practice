import os
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'end_date': datetime(2020, 9, 30),
    'email': 'bowen.kuo@bonio.com.tw',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG(
    'dump_ga_to_bq_dag',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=1)

service_account_secret_file = Secret(
    'volume', '/etc/ga_service_account', 'ga-service-account-json', 'ga-service-account.json')
client_secret_secret_file = Secret(
    'volume', '/etc/ga_client_secret', 'ga-client-secret-json', 'ga-client-secret.json')

script_root_path = '/tmp/scripts'
executalbe_r_script_path = "Services/ELT/DA/dumpDA2BQ_testing.R"
executalbe_r_script_whole_path = script_root_path + "/" + executalbe_r_script_path

volume_mount = VolumeMount('git-root-path',
                           mount_path=script_root_path,
                           sub_path=None,
                           read_only=False)
volume_config = {
    'hostPath':
    {
        'path': "/home/DA_git_master/DataAnalysis.git",
        "type": "Directory"
    }
}
volume = Volume(name='git-root-path', configs=volume_config)

start_date = "{{ ds }}"
end_date = "{{ macros.ds_add(next_ds, -1) }}"

gimmy_task = KubernetesPodOperator(namespace='default',
                                   image="bowenkuo/dump-ga-to-bq:1.0.1",
                                   cmds=["Rscript"],
                                   arguments=["--vanilla",
                                              executalbe_r_script_whole_path,
                                              start_date,
                                              end_date],
                                   labels={"script_type": "R"},
                                   secrets=[service_account_secret_file,
                                            client_secret_secret_file],
                                   name="main",
                                   task_id="main",
                                   volumes=[volume],
                                   volume_mounts=[volume_mount],
                                   is_delete_operator_pod=False,
                                   get_logs=True,
                                   dag=dag
                                   )
