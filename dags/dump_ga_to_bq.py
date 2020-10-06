import os
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': 'bowen.kuo@bonio.com.tw',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG(
    'ohoh',
    default_args=default_args,
    catchup=False,
    schedule_interval=timedelta(hours=10))

service_account_secret_file = Secret('volume', '/etc/ga_service_account', 'ga-system-account-json', 'ga-system-account.json')
client_secret_secret_file = Secret('volume', '/etc/ga_client_secret', 'ga-client-secret-json', 'ga-client-secret.json')

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

k = KubernetesPodOperator(namespace='default',
                          image="bowenkuo/dump-ga-to-bq:latest",
                          cmds=["Rscript"],
                          arguments=[executalbe_r_script_whole_path],
                          # cmds=["ls"],
                          # arguments=[script_root_path + "/Services/ELT/DA"],
                          labels={"script_type": "R"},
                          secrets=[service_account_secret_file, client_secret_secret_file],
                          name="dump-ga-to-bq",
                          task_id="dumpping",
                          volumes=[volume],
                          volume_mounts=[volume_mount],
                          is_delete_operator_pod=False,
                          get_logs=True,
                          dag=dag
                          )
