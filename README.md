# Analysing time series data

Make sure you are running the latest version of CDK before you start.

```
cdk --version
1.129.0
```

### Installation of the demo

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

> Note! Whilst this is running, you can check progress and see the resources being installed by checking the Cloudformation stack that is in progress.
