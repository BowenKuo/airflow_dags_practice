from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    dag_id="example_bash_operator_1",
    schedule_interval=None,
    start_date=datetime(2018, 12, 31),
)

t1 = BashOperator(
    task_id="t1",
    bash_command='echo "{{ ti.xcom_push(key="k1", value="v1") }}" "{{ti.xcom_push(key="k2", value="v2") }}"',
    dag=dag,
)

t2 = BashOperator(
    task_id="t2",
    bash_command='echo "{{ ti.xcom_pull(key="k1") }}" "{{ ti.xcom_pull(key="k2") }}"',
    dag=dag,
)

t1 >> t2
