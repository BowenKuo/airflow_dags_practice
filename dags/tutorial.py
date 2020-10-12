import os
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,1,1),
    'end_date': datetime(2020,9,30),
    'email': 'bowen.kuo@bonio.com.tw',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=10)
}

MAIN_DAG_ID = "ba_dag"
main_dag = DAG(
    MAIN_DAG_ID,
    default_args = default_args,
    schedule_interval = '@quarterly',
    catchup = True,
    max_active_runs = 1)

service_account_secret_file = Secret('volume', '/etc/ga_service_account', 'ga-service-account-json', 'ga-service-account.json')
client_secret_secret_file = Secret('volume', '/etc/ga_client_secret', 'ga-client-secret-json', 'ga-client-secret.json')

script_root_path = '/tmp/scripts'
get_user_ids_r_script_path = "Services/ELT/DA/getUserIDs.R"
get_user_ids_r_script_whole_path = script_root_path + "/" + get_user_ids_r_script_path
retrieve_user_session_activity_r_script_path = "Services/ELT/DA/getUserIDs.R"
retrieve_user_session_activity_r_script_whole_path = script_root_path + "/" + retrieve_user_session_activity_r_script_path

volume_mount = VolumeMount('git-root-path',
                            mount_path=script_root_path,
                            sub_path=None,
                            read_only=False)
volume_config = {
    "hostPath":
    {
        "path": "/home/DA_git_master/DataAnalysis.git",
        "type": "Directory"
    }
}
volume = Volume(name='git-root-path', configs=volume_config)

start_date = "{{ ds }}"
end_date = "{{ macros.ds_add(next_ds, -1) }}"

get_user_ids_task = KubernetesPodOperator(namespace='default',
                          image="bowenkuo/dump-ga-to-bq:1.0.1",
                          cmds=["Rscript"],
                          arguments=["--vanilla",
                                     get_user_ids_r_script_whole_path,
                                     start_date,
                                     end_date],
                          secrets=[service_account_secret_file, client_secret_secret_file],
                          name="main",
                          task_id="get_user_ids",
                          volumes=[volume],
                          volume_mounts=[volume_mount],
                          is_delete_operator_pod=False,
                          get_logs=True,
                          do_xcom_push=True,
                          dag=main_dag
                          )

SUBDAG_TASK_ID = "session_activity_dag"
from airflow.operators.subdag_operator import SubDagOperator
def get_user_session_activity(dag_id, start_date, end_date, uids):
    sub_dag = DAG(
        dag_id=dag_id,
        schedule_interval='@once')
    for uid in uids[0:9]:
        user_session_activity = KubernetesPodOperator(namespace='default',
                                  image="bowenkuo/dump-ga-to-bq:1.0.1",
                                  cmds=["Rscript"],
                                  arguments=["--vanilla",
                                             retrieve_user_session_activity_r_script_whole_path,
                                             start_date,
                                             end_date,
                                             uid],
                                  secrets=[service_account_secret_file, client_secret_secret_file],
                                  name="main",
                                  task_id="get_user_session_activity-%s" % uid,
                                  volumes=[volume],
                                  volume_mounts=[volume_mount],
                                  is_delete_operator_pod=False,
                                  get_logs=True,
                                  do_xcom_push=False,
                                  dag=sub_dag
                                  )
    return sub_dag

uids = "1000001" # context['ti'].xcom_pull(task_ids='get_user_ids_task', dag_id=MAIN_DAG_ID, key='retrun_value')

user_session_activity = SubDagOperator(
    task_id=SUBDAG_TASK_ID,
    subdag=get_user_session_activity('%s.%s' % (MAIN_DAG_ID, SUBDAG_TASK_ID),
               start_date = start_date,
               end_date = end_date,
               uids = uids),
    dag=main_dag)


get_user_ids_task >> user_session_activity
