import time
import json
import random
import boto3

STACK_NAME = 'time-series-and-data-lakes'

kinesis_client = boto3.client('kinesis')
cf_client = boto3.client('cloudformation')


def get_stream_name(stack_name):
    response = cf_client.describe_stacks(StackName=stack_name)
    outputs = response["Stacks"][0]["Outputs"]
    for output in outputs:
        if output["OutputKey"] == "InputStreamName":
            return output["OutputValue"]
    raise Exception(f"Stream not found for stack '{stack_name}'")


def get_random_data(id):
    temperature = round(10 + random.random() * 170)
    if temperature > 160:
        status = "ERROR"
    elif temperature > 140 or random.randrange(1, 100) > 80:
        status = random.choice(["WARNING","ERROR"])
    else:
        status = "OK"
    return {
        'sensor_id': f"sensor-{id:02d}",
        'temperature': temperature,
        'status': status,
        'event_time': int(time.time() * 1000)
    }


def send_data(stream_name):
    try:
        while True:
            for id in range(100):
                if random.random() < 0.5:
                    continue
                data = get_random_data(id)
                partition_key = str(data["sensor_id"])
                print(data)
                kinesis_client.put_record(
                    StreamName=stream_name,
                    Data=json.dumps(data),
                    PartitionKey=partition_key)
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    stream_name = get_stream_name(STACK_NAME)
    print(f"Stream Name: {stream_name}")
    send_data(stream_name)
