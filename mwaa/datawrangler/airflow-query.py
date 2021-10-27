#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

import awswrangler as wr
import pandas as pd
from datetime import datetime, timedelta

# Change the TSDB and TSTBL to your Timestream database and table

TSDB = "TimeSeriesDb-HYRCSa6VcVaH"
TSTBL = "TimeSeriesRawTable-oFmx41zmBQz4"

# This generates the time for the query. Check the timestamps where you 
# are running and adjust. This is currently set for the last 5 min

todays_date = datetime.now()
start = todays_date.strftime("%Y-%m-%d %H:%M:%S")
start_time = todays_date - timedelta(minutes = 60)
end_time = todays_date - timedelta(minutes = 65)

s3folder = todays_date.strftime("%Y%m%d%H")
execution_time = str(start_time).replace("T", "-")
#s3folder2 = execution_time[0:16]
s3folder2 = str(start_time).replace(" ", "-")
s3folder3 = str(s3folder2).replace(":", "-")
s3folder4 = s3folder3[0:16]
print (execution_time)
print (s3folder4)

# You can change the 1s value to larger amounts if you need to change the
# granularity. Higher values will bring back fewer results
# for eg. 10s or 60s

query1 = """WITH interpolated_timeseries AS (
  SELECT sensor_id,
       	 INTERPOLATE_LINEAR(
           CREATE_TIME_SERIES(time, measure_value::double),
           SEQUENCE(min(time), max(time), 1s)) AS interpolated_temperature,
         INTERPOLATE_LOCF(
           CREATE_TIME_SERIES(time, status),
           SEQUENCE(min(time), max(time), 1s)) AS locf_status
    FROM "{db}"."{tbl}"
   WHERE measure_name = 'temperature' AND time BETWEEN '{end}' AND '{start}'
   GROUP BY sensor_id
)
SELECT int.sensor_id, t.time, min(s.status) AS status, avg(t.temp) AS temperature
  FROM interpolated_timeseries AS int
 CROSS JOIN UNNEST(interpolated_temperature) AS t (time, temp)
 CROSS JOIN UNNEST(locf_status) AS s (time, status) 
 WHERE t.time = s.time
 GROUP BY int.sensor_id, t.time
""".format(start=start_time,end=end_time,db=TSDB,tbl=TSTBL)


# We are not writing to S3 here, just running the query at this stage

try:
    q = wr.timestream.query(query1)
    print (q)
    print ("Timestream query processed successfully and copied to {s3folder}".format(s3folder=s3folder))
except Exception as e:
    print(e)
except ValueError:
    print("No values returned")
except wr.exceptions.EmptyDataFrame:
    print("Nothing to return")
