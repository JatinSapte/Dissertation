import json
import boto3
import time
from datetime import datetime

# Initialize AWS clients
s3 = boto3.client('s3')
timestream = boto3.client('timestream-write')
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')


heart_rate_table = dynamodb.Table('HeartRateData')
alerts_table = dynamodb.Table('HeartRateAlerts')  # New table for tracking streaks

# Timestream DB/table names
DATABASE_NAME = 'MedicalSensorDB'
TABLE_NAME = 'HeartRateTable'
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:376129842374:heartrate-email-alert'

def lambda_handler(event, context):
    # Get S3 bucket and object key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']

    try:
        # Read uploaded file content from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read().decode('utf-8')

        # Load JSON content
        data = json.loads(content)

        # Handle multiple or single records
        if isinstance(data, list):
            for record in data:
                process_record(record)
        else:
            process_record(data)

        return {
            'statusCode': 200,
            'body': 'Data processed and sent to Timestream and  DynamoDB.'
        }

    except Exception as e:
        print("Error processing file:", e)
        return {
            'statusCode': 500,
            'body': str(e)
        }

def process_record(record):
    # Extract and validate fields
    device_id = record.get('device_id')
    timestamp = record.get('timestamp')
    sensor_type = record.get('sensor_type', 'heart_rate')
    value = record.get('value')

    if not all([device_id, timestamp, value]):
        print("Missing required fields:", record)
        return

    # Convert timestamp to epoch in milliseconds for Timestream
    try:
        ts_epoch_ms = int(datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000)
    except ValueError:
        print(f"Invalid timestamp format: {timestamp}")
        return

    # Write to Timestream
    try:
        timestream.write_records(
            DatabaseName=DATABASE_NAME,
            TableName=TABLE_NAME,
            Records=[
                {
                    'Dimensions': [
                        {'Name': 'device_id', 'Value': device_id},
                        {'Name': 'sensor_type', 'Value': sensor_type},
                    ],
                    'MeasureName': 'value',
                    'MeasureValue': str(value),
                    'MeasureValueType': 'DOUBLE',
                    'Time': str(ts_epoch_ms),
                    'TimeUnit': 'MILLISECONDS'
                }
            ]
        )
        print(f"Timestream write success: {device_id} at {timestamp}")
    except Exception as e:
        print(f"Timestream write failed: {e}")

    # Write to main DynamoDB table
    try:
        heart_rate_table.put_item(
            Item={
                'device_id': device_id,
                'timestamp': timestamp,
                'sensor_type': sensor_type,
                'value': int(value)
            }
        )
        print(f"DynamoDB write success: {device_id} at {timestamp}")
    except Exception as e:
        print(f"DynamoDB write failed: {e}")


    # Check for high heart rate streaks
    try:
        value = int(value)
        alert_record = alerts_table.get_item(Key={'device_id': device_id}).get('Item')

        if alert_record:
            streak = alert_record.get('high_hr_count', 0)
        else:
            streak = 0

        if value > 100:
            streak += 1
            if streak >= 2:
                send_alert(device_id)
                streak = 0  # reset after alert
        else:
            streak = 0

        alerts_table.put_item(
            Item={
                'device_id': device_id,
                'last_timestamp': timestamp,
                'high_hr_count': streak
            }
        )
    except Exception as e:
        print("Alert tracking error:", e)

def send_alert(device_id):
    try:
        message = f"⚠️ Alert: High heart rate detected continuously 5 times for device {device_id}!"
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject='Heart Rate Alert',
            Message=message
        )
        print("SNS alert sent.")
    except Exception as e:
        print("SNS publish failed:", e)
