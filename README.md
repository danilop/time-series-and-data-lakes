# Analysing time series data

Make sure you are running the latest version of CDK before you start.

```
cdk --version
1.129.0
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
|sensor_id | time                         |status    temperature |
|sensor-12	|2021-10-26 09:20:20.922000000	|OK	     |61.0 |
|sensor-12	|2021-10-26 09:20:21.922000000	|OK	     |65.12938747419133|
|sensor-12	|2021-10-26 09:20:22.922000000	|OK	     |69.25877494838265|
|sensor-12	|2021-10-26 09:20:23.922000000	|OK	     |74.02579365079364|
...
```

You can repeat the other queries to complete the first part of this demo.

#### Part 2 - Data Lake orchestration








