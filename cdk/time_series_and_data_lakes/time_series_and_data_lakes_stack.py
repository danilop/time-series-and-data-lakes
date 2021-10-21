from aws_cdk import (
    aws_iam as iam,
    aws_kinesis as kinesis,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_event_sources,
    aws_s3 as s3,
    aws_timestream as timestream,
    core
)

from from_kds_to_timestream.from_kds_to_timestream import FromKdsToTimestream


class TimeSeriesAndDataLakesStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str,
            input_stream_shard_count: int, cep_stream_shard_count: int, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Input Stream - Amazon Kinesis Data Stream
        input_stream = kinesis.Stream(
            self,
            "InputStream",
            shard_count=input_stream_shard_count
        )

        # Input Stream - Amazon Kinesis Data Stream
        cep_stream = kinesis.Stream(
            self,
            "CepStream",
            shard_count=cep_stream_shard_count
        )

        # Time Series Database - Amazon Timestream Database
        ts_db = timestream.CfnDatabase(
            self,
            "TimeSeriesDb",
            # kms_key_id={string of kms_key_id if you want to use this}
        )

        # Raw Time Series Table - Amazon Timestream Table
        ts_raw_table = timestream.CfnTable(
            self,
            "TimeSeriesRawTable",
            database_name=ts_db.ref,
            retention_properties={
                "MemoryStoreRetentionPeriodInHours": 24,
                "MagneticStoreRetentionPeriodInDays": 7
            }
        )

        # Complex Event Processing (CEP) Time Series Table - Amazon Timestream Table
        ts_cep_table = timestream.CfnTable(
            self,
            "TimeSeriesCepTable",
            database_name=ts_db.ref,
            retention_properties={
                "MemoryStoreRetentionPeriodInHours": 24,
                "MagneticStoreRetentionPeriodInDays": 30
            }
        )

        # Load all records in the Input Stream into the Raw Time Series Table - AWS Lambda Function
        load_raw_data = FromKdsToTimestream(
            self,
            "LoadRawData",
            stream=input_stream,
            ts_db=ts_db,
            ts_table=ts_raw_table
        )

        # Load all records in the CEP Output Stream into the CEP Time Series Table - AWS Lambda Function
        load_cep_data = FromKdsToTimestream(
            self,
            "LoadCepData",
            stream=cep_stream,
            ts_db=ts_db,
            ts_table=ts_cep_table
        )

        # S3 Bucket used by MWAA to export data
        s3_bucket = s3.Bucket(self, "export")

        # IAM policy which we can attach to MWAA that provides
        # access to the Timestream database and tables and the
        # "export" Amazon S3 bucket
        mwaa_policy = iam.ManagedPolicy(
            self,
            "MWAAPolicy",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "s3:GetBucket*",
                        "s3:PutObject",
                        "s3:List*",
                        "s3:GetObject*"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"{s3_bucket.bucket_arn}/*",
                        f"{s3_bucket.bucket_arn}"
                    ],
                ),
                iam.PolicyStatement(
                    actions=[
                        "timestream:Select",
                        "timestream:DescribeTable",
                        "timestream:ListTables"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"{ts_raw_table.attr_arn}",
                        f"{ts_cep_table.attr_arn}"
                    ],
                ),
                iam.PolicyStatement(
                    actions=[
                        "timestream:DescribeEndpoints",
                        "timestream:SelectValues",
                        "timestream:CancelQuery"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=["*"],
                ),
            ]
        )

        # CloudFormation Outputs

        core.CfnOutput(
            self,
            id="InputStreamName",
            value=input_stream.stream_name,
            description="Input Stream Name"
        )

        core.CfnOutput(
            self,
            id="CepStreamName",
            value=cep_stream.stream_name,
            description="CEP Stream Name"
        )

        core.CfnOutput(
            self,
            id="TimeSeriesDatabaseName",
            value=ts_db.ref,
            description="Time Series Database Name"
        )

        core.CfnOutput(
            self,
            id="RawTimeSeriesTableName",
            value=ts_raw_table.attr_name,
            description="Raw Time Series Table Name"
        )

        core.CfnOutput(
            self,
            id="CepTimeSeriesTableName",
            value=ts_cep_table.attr_name,
            description="CEP Time Series Table Name"
        )

        core.CfnOutput(
            self,
            id="MWAAPolicyArn",
            value=mwaa_policy.managed_policy_arn,
            description="MWAA IAM Policy ARN"
        )

