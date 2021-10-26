#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0


# when running this DAG make sure you select the backfill command
# {"command": "backfill -s 2021-09-10 -e 2021-10-10 timestream-airflow-demo"} #mwaa 1.x
# {"command": "dags backfill -s 2021-09-10 -e 2021-10-10 timestream-airflow-demo"} #mwaa 2.x

from airflow import DAG

from airflow.operators.bash_operator import BashOperator

from datetime import timedelta,time,datetime
import os

DAG_ID = os.path.basename(__file__).replace(".py", "")

default_args = {
            "owner": "airflow",
            "start_date": datetime(2020, 9, 9),
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "email": "youremail@host.com",
            "retries": 0,
            "retry_delay": timedelta(minutes=5)
        }

from time import sleep
from datetime import datetime
from random import random

with DAG(dag_id=DAG_ID, schedule_interval=None, default_args=default_args, catchup=False) as dag:
    bash_task_1 = BashOperator(
        task_id="cli_command",
        bash_command="airflow {{ dag_run.conf['command'] }}"
    )
    bash_task_1