#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This is an example dag for using the Kubernetes Executor.
"""
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='dump_example_kubernetes_executor',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
) as dag:

    affinity = {
        'podAntiAffinity': {
            'requiredDuringSchedulingIgnoredDuringExecution': [
                {
                    'topologyKey': 'kubernetes.io/hostname',
                    'labelSelector': {
                        'matchExpressions': [
                            {
                                'key': 'app',
                                'operator': 'In',
                                'values': ['airflow']
                            }
                        ]
                    }
                }
            ]
        }
    }

    tolerations = [{
        'key': 'dedicated',
        'operator': 'Equal',
        'value': 'airflow'
    }]

    # You don't have to use any special KubernetesExecutor configuration if you don't want to
    start_task = BashOperator(
        task_id="start_task",
        bash_command='echo "GG" && ls ./',
        executor_config={"KubernetesExecutor": {"image": "bowenkuo/dump-ga-to-bq:latest",
                                                "tolerations": tolerations,
                                                "affinity": affinity }}
    )
