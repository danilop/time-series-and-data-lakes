#!/usr/bin/env python3
import io
from aws_cdk import core

from mwaa_cdk.deploy_files import MwaaCdkStackDeployFiles
from mwaa_cdk.mwaa_cdk_backend import MwaaCdkStackBackend
from mwaa_cdk.mwaa_cdk_env import MwaaCdkStackEnv

# Chnage the mwaa_secret to the ARN of the secret you create via the AWS cli
# The example below is a dummy value

env_EU=core.Environment(region="eu-west-1", account="704533066374")
mwaa_props = {
    'dagss3location': 'airflow-timestream-datalake-demo',
    'mwaa_env' : 'airflow-timestream-datalake',
    'mwaa_secrets' : 'airflow/variables', 
    'mwaa_ts_iam_arn' : 'arn:aws:iam::704533066374:policy/time-series-and-data-lakes-MWAAPolicyCBFB7F6C-1M1XY2GN81694',
    'datalake_bucket' : 'time-series-and-data-lakes-datalake9060eab7-1qga4qb5ly9vn'
    }

app = core.App()

mwaa_backend = MwaaCdkStackBackend(
    scope=app,
    id="timestream-MWAA-vpc",
    env=env_EU,
    mwaa_props=mwaa_props
)

mwaa_env = MwaaCdkStackEnv(
    scope=app,
    id="timestream-MWAA-env",
    vpc=mwaa_backend.vpc,
    env=env_EU,
    mwaa_props=mwaa_props
)

mwaa_filedeploy = MwaaCdkStackDeployFiles(
    scope=app,
    id="timestream-MWAA-deploy",
    vpc=mwaa_backend.vpc,
    env=env_EU,
    mwaa_props=mwaa_props
)

app.synth()
