from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(5):
    event = {
        "build_id": i,
        "repo": "demo/repo",
        "status": "failed" if i % 2 == 0 else "success",
        "duration": 100 + i,
        "log": "error occurred" if i % 2 == 0 else "build successful"
    }

    producer.send('cicd-builds', event)
    print(f"Sent: {event}")
    time.sleep(1)

producer.flush()