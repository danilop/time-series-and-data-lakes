import os
import base64
import json
import boto3

from botocore.config import Config

TIMESTREAM_BATCH_SIZE = 100

TIMESTREAM_DATABASE = os.environ['TIMESTREAM_DATABASE']
TIMESTREAM_TABLE = os.environ['TIMESTREAM_TABLE']

write_client = boto3.client('timestream-write',
    config=Config(
        read_timeout=20,
        max_pool_connections = 5000,
        retries={'max_attempts': 10}
    )
)

def prepare_record(dimensions, event_time, measure_name, measure_value):
    record = {
        'Time': str(event_time),
        'Dimensions': dimensions,
        'MeasureName': measure_name,
        'MeasureValue': str(measure_value),
        'MeasureValueType': 'DOUBLE'
    }
    return record


def write_records(records):
    try:
        result = write_client.write_records(DatabaseName=TIMESTREAM_DATABASE,
                                            TableName=TIMESTREAM_TABLE,
                                            Records=records,
                                            CommonAttributes={})
        status = result['ResponseMetadata']['HTTPStatusCode']
        print("Processed %d records. WriteRecords Status: %s" %
              (len(records), status))
    except Exception as err:
        print("Error:", err)

def lambda_handler(event, context):
    print(event)
    
    time_series_records = []

    for record in event['Records']:
        payload = base64.b64decode(record["kinesis"]["data"])
        print("Decoded payload: " + str(payload))
        stream_record = json.loads(payload)

        dimensions = []

        for d in ['sensor_id', 'status', 'non_errors', 'history']:
            if d in stream_record:
                dimensions.append({'Name': d, 'Value': str(stream_record[d])})

        for m in ['temperature', 'min_temperature', 'avg_temperature', 'max_temperature', 'elapsed']:
            if m in stream_record:
                time_series_records.append(
                    prepare_record(
                        dimensions, stream_record['event_time'],
                        m, stream_record[m]
                    )               
                )

        if len(time_series_records) >= TIMESTREAM_BATCH_SIZE:
            write_records(time_series_records)
            time_series_records = []

    if len(time_series_records) > 0:
        write_records(time_series_records)
