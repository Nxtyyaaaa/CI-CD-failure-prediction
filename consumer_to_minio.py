from kafka import KafkaConsumer
import json
import boto3

consumer = KafkaConsumer(
    'cicd-builds',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest'
)

s3 = boto3.client(
    's3',
    endpoint_url='http://127.0.0.1:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='password123'
)

print("Listening for messages...")

for msg in consumer:
    event = msg.value

    key = f"build_{event['build_id']}.json"

    s3.put_object(
        Bucket='cicd-logs',
        Key=key,
        Body=json.dumps(event)
    )

    print(f"Stored in MinIO: {key}")