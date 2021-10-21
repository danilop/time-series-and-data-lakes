#!/usr/bin/env python3

from aws_cdk import core

from time_series_and_data_lakes.time_series_and_data_lakes_stack import TimeSeriesAndDataLakesStack


app = core.App()
TimeSeriesAndDataLakesStack(app, "time-series-and-data-lakes", 1, 1)

app.synth()
