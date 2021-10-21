from aws_cdk import (
    aws_iam as iam,
    aws_kinesis as kinesis,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_event_sources,
    aws_timestream as timestream,
    core
)


class FromKdsToTimestream(core.Construct):

    def __init__(self, scope: core.Construct, construct_id: str,
            stream: kinesis.Stream, ts_db: timestream.CfnDatabase, ts_table: timestream.CfnTable,
            **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Load all records in the Input Stream into the Raw Time Series Table - AWS Lambda Function
        load_data_fn = lambda_.Function(
            self, "LoadRawDataFn",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("load_data_fn"),
            handler="app.lambda_handler",
            environment={
                "TIMESTREAM_DATABASE": ts_db.ref,
                "TIMESTREAM_TABLE": ts_table.attr_name
            }
        )

        # Lambda Function permission to write to the Raw Time Series Table
        load_data_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "timestream:WriteRecords",
                ],
                effect=iam.Effect.ALLOW,
                resources=[
                    f"{ts_table.attr_arn}"
                ],
            )
        )

        # Lambda function permissions to describe Timestream endpoints
        load_data_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "timestream:DescribeEndpoints"
                ],
                effect=iam.Effect.ALLOW,
                resources=["*"],
            )
        )
                
        # To send Input Stream records to the Lambda function
        load_data_fn.add_event_source(
            lambda_event_sources.KinesisEventSource(
                stream,
                starting_position=lambda_.StartingPosition.TRIM_HORIZON,
                retry_attempts=2
            )
        )
