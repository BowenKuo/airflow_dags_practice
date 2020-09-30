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

service_account_secret_file = Secret('volume', '/etc/ga_service_account', 'ga-system-account-json', 'bonio-da-958b900cd287.json')
client_secret_secret_file = Secret('volume', '/etc/ga_client_secret', 'ga-client-secret-json', 'client_secret_953933740389-gdq7ift55a027a26vjvv37j5mkfcvue3.apps.googleusercontent.com.json')

git_root_path = os.environ['AIRFLOW__KUBERNETES__GIT_DAGS_FOLDER_MOUNT_POINT']
executalbe_r_script_name = "dump_ga_to_bq.R"
executalbe_r_script_path = git_root_path + "/" + executalbe_r_script_name

volume_mount = VolumeMount('git-root-path',
                            mount_path=git_root_path,
                            sub_path=None,
                            read_only=False)
volume_config = {
    'hostPath':
    {
        'path': "/home/DA_git_master",
        # 'path': '/home',
        "type": "Directory"
    }
}
volume = Volume(name='git-root-path', configs=volume_config)

k = KubernetesPodOperator(namespace='default',
                          image="bowenkuo/dump-ga-to-bq:latest",
                          cmds=["Rscript", executalbe_r_script_path],
                          arguments=[],
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
