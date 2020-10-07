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
    'start_date': datetime(2020,10,1),
    'email': 'bowen.kuo@bonio.com.tw',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG(
    'dump_GA_to_BQ_DAG2',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False)

service_account_secret_file = Secret('volume', '/etc/ga_service_account', 'ga-service-account-json', 'ga-service-account.json')
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

gimmy_task = KubernetesPodOperator(namespace='default',
                          image="bowenkuo/dump-ga-to-bq:1.0.1",
                          cmds=["Rscript"],
                          arguments=["--vanilla",
                                     executalbe_r_script_whole_path,
                                     "{{ dt(execution_date) }}"],
                          labels={"script_type": "R"},
                          secrets=[service_account_secret_file, client_secret_secret_file],
                          name="gg",
                          task_id="dumpping2",
                          volumes=[volume],
                          volume_mounts=[volume_mount],
                          is_delete_operator_pod=False,
                          get_logs=True,
                          dag=dag
                          )
