# Analysing time series data

Make sure you are running the latest version of CDK before you start.

```
cdk --version
1.134.0
```

## Getting the demo up and running

#### Part 1 - Timestream data ingestion and processing

**Step 1**

After checking out the repository into a local directory, enter the cdk directory and install the required CDK constructs

```
cd cdk
pip install -r requirements.txt
```

**Step 2**

You can now deploy the CDK stack by running the following command:

```
cdk deploy time-series-and-data-lakes
```

You will be prompted to check the security changes (answer Y) and then it will start to deploy the stack. If successfull, you will see something similar to:

```
time-series-and-data-lakes: creating CloudFormation changeset...

 ✅  time-series-and-data-lakes

Outputs:
time-series-and-data-lakes.CepStreamName = time-series-and-data-lakes-CepStreamB47B8F43-0wKXAnI3LAmb
time-series-and-data-lakes.CepTimeSeriesTableName = TimeSeriesCepTable-Ikv45CI0g1Iq
time-series-and-data-lakes.DataLakeBucket = time-series-and-data-lakes-datalake9060eab7-1qga4qb5ly9vn
time-series-and-data-lakes.InputStreamName = time-series-and-data-lakes-InputStreamCFB159EA-NBQ4g7W0AL2L
time-series-and-data-lakes.MWAAPolicyArn = arn:aws:iam::704533066374:policy/time-series-and-data-lakes-MWAAPolicyCBFB7F6C-1M1XY2GN81694
time-series-and-data-lakes.RawTimeSeriesTableName = TimeSeriesRawTable-oFmx41zmBQz4
time-series-and-data-lakes.TimeSeriesDatabaseName = TimeSeriesDb-HYRCSa6VcVaH
```
We will use the information in these outputs in a bit.

We are going to reference the TimeSeries database and tables later on, so we are going to store these in AWS Secrets Manager as follows, using the aws cli:

```
aws secretsmanager create-secret --name airflow/variables/timeseriesdb --description "TS DB" --secret-string "TimeSeriesDb-HYRCSa6VcVaH" 
aws secretsmanager create-secret --name airflow/variables/timeseriesrawtbl --description "TS Raw Table" --secret-string "TimeSeriesRawTable-oFmx41zmBQz4"         
aws secretsmanager create-secret --name airflow/variables/datalake --description "S3 Datalake bucket" --secret-string "time-series-and-data-lakes-datalake9060eab7-1qga4qb5ly9vn" 
```



> Note! Whilst this is running, you can check progress and see the resources being installed by checking the Cloudformation stack that is in progress.

You have now installed the following components:

* Creation of the Amazon Kinesis Data Streams, for RAW and for Complex Event Processing (CEP)
* AWS Lambda functions that will process incoming time series data, both RAW and CEP
* Create an Amazon Timeseries database and table
* Create an Amazon S3 bucket which we will use as our Data Lake
* An IAM policy which provides access to these resources

**Step 3**

We can now try and generate some load, using the load generator python script. It might be a good idea to run this on a virtual machine or another device that has internet access as it can cause the machine running the script to run slowly.

From the parent directory (you are currently in CDK) locate the **random-data-generator** directory. Run the script as follows

```
cd random-data-generator/
./start.sh
```
And you should see the following output

```
appending output to nohup.out
```

> If you want, you can run the script interactively by just running "python3 random_data_generator.py" to see the script running.

If you now go into the Amazon Timestream console, you will see data arriving in two tables within the new database that has been created.

**Step 4**

Open up Amazon Kinesis Datastreams. Find the input stream (in my example above, this is time-series-and-data-lakes-InputStreamCFB159EA-NBQ4g7W0AL2L) and select it. From the Process pull down menu, select "process data in real time". "Select Apache Flink – Studio notebook" and then give the notebook a name (kinesis-timestream) and then use the link to create a new database in AWS Glue (kinesis-flink-timestream). Select this database and then create your Studio Notebook.

Once it has been created, make sure you click on the Edit IAM Permissions and select the "Included destinations in IAM policy" the and add the CEP Kinesis data stream that were created (in my example above, this is time-series-and-data-lakes-CepStreamB47B8F43-0wKXAnI3LAmb), and then SAVE. 

Scroll down and now under AWS Support connectors you add the flink-sql-connector-kinesis, aws-msk-iam-auth and flink-connector-kafka (as of writing this, the version being used was 2_12).

You can now Run (this might take a few minutes) and then open your notebook in Zeppelin.

In Zeppelin, create a new note and use the contents of the kinesis-studio-notebook folder to run queries. You will need to change the stream information for the output that was created when you ran the cdk deploy application. Using the example above, this would be:

```
%flink.ssql

CREATE TABLE sensor_data (
    `sensor_id` VARCHAR(9),
    `temperature` DOUBLE,
    `status` VARCHAR(7),
    `event_time` BIGINT,
    `ptime` AS PROCTIME()
)
PARTITIONED BY (sensor_id)
WITH (
    'connector' = 'kinesis',
    'stream' = 'time-series-and-data-lakes-InputStreamCFB159EA-NBQ4g7W0AL2L',
    'aws.region' = 'eu-west-1',
    'scan.stream.initpos' = 'LATEST',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);
```
You should see the two tables updating as you run the load generator. If you do, you are ready for the next step.

**Step 5**

We can now try out the Amazon Timestream queries within the query editor. You will find these in the timestream-queries folder within the queries.txt file.

You will need to update the query with the tables were created during your installation. In the example above, my database and tables are 
```
time-series-and-data-lakes.RawTimeSeriesTableName = TimeSeriesRawTable-oFmx41zmBQz4
time-series-and-data-lakes.TimeSeriesDatabaseName = TimeSeriesDb-HYRCSa6VcVaH
```

When I run the first query, I see output like the following:

```
|| sensor_id  || time                      || status || temperature ||
| sensor-12	| 2021-10-26 09:20:20.922000000	| OK	     | 61.0 |
| sensor-12	| 2021-10-26 09:20:21.922000000	| OK	     | 65.12938747419133 |
| sensor-12	| 2021-10-26 09:20:22.922000000	| OK	     | 69.25877494838265 |
| sensor-12	| 2021-10-26 09:20:23.922000000	| OK	     | 74.02579365079364 |
...
```

You can repeat the other queries to complete the first part of this demo.

#### Part 2 - Data Lake orchestration

**Step 6**

We have a script that we have created that allows us to use some of the Timestream query capabilities, and then generate output using an open source project called AWS Data Wrangler. This project makes is easy to work with data services on AWS.

The script is in the mwaa folder, and called airflow-query.py.

We need to modify this script and update it with our Timestream database and table, review and adjust if needed the other info in the script (you can keep these default if you do not want to change) and we can run it by

```
python airflow-query.py
```

Which should generate output like the following:

```
0      sensor-12 2021-10-26 11:36:35.099     OK    28.000000
1      sensor-12 2021-10-26 11:36:36.099     OK    30.545339
2      sensor-12 2021-10-26 11:36:37.099     OK    33.090678
3      sensor-12 2021-10-26 11:36:38.099     OK    35.636017
4      sensor-12 2021-10-26 11:36:39.099     OK    36.341655
...          ...                     ...    ...          ...
29154  sensor-88 2021-10-26 11:38:49.961     OK    11.126397
29155  sensor-88 2021-10-26 11:38:50.961     OK    14.794466
29156  sensor-88 2021-10-26 11:38:51.961     OK    18.747036
29157  sensor-88 2021-10-26 11:38:52.961     OK    22.699605
29158  sensor-88 2021-10-26 11:38:53.961     OK    26.652174

[29159 rows x 4 columns]
Timestream query processed successfully and copied to 2021102612
```
(You can ignore the copied to statement - the script does not copy this data, and this is just for reference)

The script could be used to write data to our data lake (the Amazon S3 bucket we created in Part One) by changing the "q = wr.timestream.query(query1)" statement to the following:

```
wr.s3.to_csv(df=wr.timestream.query(query1), path='s3://{our-data-lake-s3folder}/my_file.csv'.
```
In this example we are writing a csv file, but we could use any of the supported data formats (parquet for example).

We are going to use Apache Airflow to orchestrate the running of this script, so that we can export this query into our data lake. For the purpose of the demo, we will schedule this every 5 minutes, just so we can see it in operation. We will use Amazon Managed Workflows for Apache Airflow (MWAA), a managed version of Apache Airflow.

**Step 7**

We will deploy Apache Airflow using CDK. You will be creating a new VPC, so make sure you have capacity within the region you are deploying before proceeding (by default, the soft limit is 5 VPCs per region)


```
cd mwaa/mwaa-cdk
cdk deploy timestream-MWAA-vpc
```
If successful, you will create the required VPC and you will see output similar to the following (may take 5-10 minutes).

```
 ✅  timestream-MWAA-vpc

Outputs:
timestream-MWAA-vpc.ExportsOutputReftimestreamMWAAApacheAirflowVPC8F1A4AAE969F6886 = vpc-082e01fa3d88544df
timestream-MWAA-vpc.ExportsOutputReftimestreamMWAAApacheAirflowVPCprivateSubnet1SubnetD80E882B021683F9 = subnet-000fc9d76fc57ae8b
timestream-MWAA-vpc.ExportsOutputReftimestreamMWAAApacheAirflowVPCprivateSubnet2Subnet4D776BC5BB3BCAA4 = subnet-0a6da1ee21cb2f00e
timestream-MWAA-vpc.VPCId = vpc-082e01fa3d88544df

Stack ARN:
arn:aws:cloudformation:eu-west-1:704533066374:stack/timestream-MWAA-vpc/f9901a50-365a-11ec-808e-06236a8fff2f
```

You will now need to edit the app.py file which contains some parameters that control how the Apache Airflow will be created. You will need to alter the following lines:

```
env_EU=core.Environment(region="eu-west-1", account="xxxxxxx")
mwaa_props = {
    'dagss3location': 'airflow-timestream-datalake-demo',
    'mwaa_env' : 'airflow-timestream-datalake',
    'mwaa_secrets' : 'airflow/variables', 
    'mwaa_ts_iam_arn' : 'arn:aws:iam::704533066374:policy/time-series-and-data-lakes-MWAAPolicyCBFB7F6C-1M1XY2GN81694',
    'datalake_bucket' : 'time-series-and-data-lakes-datalake9060eab7-1qga4qb5ly9vn'
    }
```

Update this to reflect your own AWS account as well as the output from the Timestream CDK deployment (which created the mwaa_ts_iam_arn and datalake_bucket values). You should change the dagss3location to be a unique S3 bucket - the CDK deployment will fail if you do not change this as this has already been created when this demo was put together. 

You can now deploy the MWAA environment using the following command:

First deploy the files
```
cdk deploy timestream-MWAA-deploy
```
Answer Y when prompted, and this will create and upload the Apache Airflow files, you can now deploy the environment running

```
cdk deploy timestream-MWAA-env
```
Answer Y when prompted. If you are successful, you should see something like:

```
(NOTE: There may be security-related changes not in this list. See https://github.com/aws/aws-cdk/issues/1299)

Do you wish to deploy these changes (y/n)? y
timestream-MWAA-env: deploying...
timestream-MWAA-env: creating CloudFormation changeset...

 ✅  timestream-MWAA-env

Outputs:
timestream-MWAA-env.GlueIAMRole = timestream-MWAA-env-mwaagluetimestreamserviceroleA-843SPL9XF745
timestream-MWAA-env.MWAASecurityGroup = sg-0104a570bd980800b

Stack ARN:
arn:aws:cloudformation:eu-west-1:704533066374:stack/timestream-MWAA-env/bf1558c0-367a-11ec-8ad1-024f3dd2760b
```

You will now need to add one more value into AWS Secrets Manager, which we will use in the DAGS. This is to enable the the right level of access for AWS Glue. We have created a new IAM role, which you can see in the output as the value of timestream-MWAA-env.GlueIAMRole (in the example above, this is timestream-MWAA-env-mwaagluetimestreamserviceroleA-843SPL9XF745)

```
aws secretsmanager create-secret --name airflow/variables/gluecrawlerrole --description "AWS Glue Crawler IAM Role" --secret-string "timestream-MWAA-env-mwaagluetimestreamserviceroleA-843SPL9XF745" 
```

You can now open the Apache Airflow UI by accessing the Web URL. To get this, you can either go to the MWAA console, or run this command which will give you the url (this is based on the environment called "airflow-timestream-datalake" which I set in the app.py above)

```
aws mwaa get-environment --name airflow-timestream-datalake  | jq -r '.Environment | .WebserverUrl'
```
And this will provide you with your url you can put in the browser, which will take you to the Apache Airflow UI.

**Step 8**

The DAG called "timestream-airflow-demo.py" will appear in the Apache Airflow UI, but will be paused and so will not be running.

Once enabled (un-paused) it will (for the purposes of being able to easily demo) run every 5 minutes, running one of the queries from Part 1, and storing that in csv file in the data lake we defined in Amazon S3. The folder structure is created to help partition the data, and is based on the date/time stamp of when the query was run.

> The DAG has been configured to not do catchup (catchup=False) which means you will not create large numbers of tasks due to the schedule being every 5 mins.

You can look at the DAG script and see how this works
You can view the folders/files being created in the data lake S3 bucket
You can view the logs for the tasks as they run, both in the Apache Airflow and CloudWatch

**Step 9**

Unpause the DAG called "timestream-airflow-glue-adhoc.py" and then run this manually once. When you run this the first time, this will cause the task to fail (this is a known issue with the operator with this version of MWAA, and is fixed - we just cannot apply this fixed operator to this version of Apache Airflow).

If you go to the AWS Glue console, you will now see that there is a new database that has been created.

Unpause the DAG called "timestream-airflow-glue.py" and this will kick off the crawler again (every 5 mins) post update. When you look at the AWS Glue console, you will now see the tables.

<Add Partition updates here>
 
If we now go to the Amazon Athena console, we can see that we see our Timestream data available within our data lake.
 
 
### Part 3 - Visualsation and analysing the timestream data in the Data Lake 

 <Javier to add>




