
#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator

import awswrangler as wr
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    #"start_date": datetime(2021, 9, 18),
    "start_date" : days_ago(1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5)
    # 'end_date': datetime(2016, 1, 1),
}

# Get the Timeseries DB and Table details which we stored in AWS Secrets Manager
# make sure you have set these or the script will fail
# If you use AWS Secrets manager set the details there otherwise use the Admin tab
# and add the variables via the Apache Airflow UI

tsdb =  Variable.get("timeseriesdb", default_var="undefined")
tstbl = Variable.get("timeseriesrawtbl", default_var="undefined")
glueiam = Variable.get("gluecrawlerrole", default_var="undefined")
datalake = Variable.get("datalake", default_var="undefined")

def ts_query(**kwargs):
    # get time window for query - uses the delta of the previous and this window
    # must be an idemopotent value, i.e every time it runs it will create the same output

    start = str(kwargs['execution_date']).replace("T", " ")
    finish = str(kwargs['next_execution_date']).replace("T", " ")
    
    # get an idemopotent folder value for the folder
    execution_time = str(kwargs['execution_date'])
    s3folder = execution_time[0:16]

    print ("Data will be uploaded to {target}".format(target=s3folder))
    query = """
                WITH interpolated_timeseries AS (
                SELECT sensor_id,
                        INTERPOLATE_LINEAR(
                        CREATE_TIME_SERIES(time, measure_value::double),
                        SEQUENCE(min(time), max(time), 1s)) AS interpolated_temperature,
                        INTERPOLATE_LOCF(
                        CREATE_TIME_SERIES(time, status),
                        SEQUENCE(min(time), max(time), 1s)) AS locf_status
                    FROM "{db}"."{tbl}"
                WHERE measure_name = 'temperature' AND time BETWEEN '{start}' AND '{finish}'
                GROUP BY sensor_id
                )
                SELECT int.sensor_id, t.time, min(s.status) AS status, avg(t.temp) AS temperature
                FROM interpolated_timeseries AS int
                CROSS JOIN UNNEST(interpolated_temperature) AS t (time, temp)
                CROSS JOIN UNNEST(locf_status) AS s (time, status)
                WHERE t.time = s.time
                GROUP BY int.sensor_id, t.time
                """.format(start=start,finish=finish,db=tsdb,tbl=tstbl)


    print("Query to be run: {query}".format(query=query))
    try:
        wr.s3.to_csv(df=wr.timestream.query(query), path='s3://demo-airflow-ts-output/{s3folder}/my_file.csv'.format(s3folder=s3folder,))
        print ("Timestream query processed successfully and copied to {s3folder}".format(s3folder=s3folder))
    except ValueError:
        print("Query returned no values - no data uploaded")
    except wr.exceptions.EmptyDataFrame:
        print("Query returned nothing - no data uploaded")

with DAG(
        dag_id=os.path.basename(__file__).replace(".py", ""),
        default_args=default_args,
        catchup=False,
        dagrun_timeout=timedelta(hours=2),
        # set to every 10 mins for demo
        #schedule_interval="*/10 * * * *"
        # set to every 2 hours for demo
        #schedule_interval="0 */2 * * *"
        # set to every hour
        #schedule_interval="0 */1 * * *"
        schedule_interval="*/5 * * * *"

) as dag:

    # The name of the Role should be the one created during the deployment of the MWAA environment
    # The output is the datalake created
    # You can add your own exclusions you need

    run_crawler = AwsGlueCrawlerOperator(task_id='run_crawler',
        aws_conn_id='aws_default',
        config={
            'Name':'airflow-timestream-crawler',
            'Role':'service-role/{iamrole}'.format(iamrole=glueiam),
            'DatabaseName' : 'reinvent-airflow-timeseries-datalake',
            'Description': 'Crawler for TimeSeries data',
            'Targets':{'S3Targets' : [{'Path': 's3://{datalake}'.format(datalake=datalake), 'Exclusions': [ 'demo-airflow-flink/**', 'files/**'] }]}}
            )

    ts_query=PythonOperator(task_id='ts_query', python_callable=ts_query, dag=dag)

    ts_query >> run_crawler
    
