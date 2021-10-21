import json
import pytest

from aws_cdk import core
from time_series_and_data_lakes.time_series_and_data_lakes_stack import TimeSeriesAndDataLakesStack


def get_template():
    app = core.App()
    TimeSeriesAndDataLakesStack(app, "time-series-and-data-lakes")
    return json.dumps(app.synth().get_stack("time-series-and-data-lakes").template)


def test_sqs_queue_created():
    assert("AWS::SQS::Queue" in get_template())


def test_sns_topic_created():
    assert("AWS::SNS::Topic" in get_template())
