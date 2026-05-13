import boto3
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# -----------------------------
# Load secrets from .env in this directory
# -----------------------------
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_path)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_GROUP = os.getenv("KAFKA_GROUP")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

missing_vars = [
    name for name, value in [
        ("KAFKA_BOOTSTRAP", KAFKA_BOOTSTRAP),
        ("KAFKA_GROUP", KAFKA_GROUP),
        ("MINIO_ENDPOINT", MINIO_ENDPOINT),
        ("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY),
        ("MINIO_SECRET_KEY", MINIO_SECRET_KEY),
        ("MINIO_BUCKET", MINIO_BUCKET),
    ] if not value
]

if missing_vars:
    raise EnvironmentError(
        f"Missing required environment variables in {dotenv_path}: {', '.join(missing_vars)}"
    )

# Kafka consumer settings
try:
    consumer = KafkaConsumer(
        'banking_server.public.customers',
        'banking_server.public.accounts',
        'banking_server.public.transactions',
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=KAFKA_GROUP,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
except NoBrokersAvailable as exc:
    raise ConnectionError(
        f"Cannot connect to Kafka broker at {KAFKA_BOOTSTRAP}. "
        "Confirm the broker is running and the address is reachable."
    ) from exc

# MinIO client
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

bucket = MINIO_BUCKET

# Create bucket if not exists
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)

# Consume and write function
def write_to_minio(table_name, records):
    if not records:
        return
    df = pd.DataFrame(records)
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_path = f'{table_name}_{date_str}.parquet'
    df.to_parquet(file_path, engine='fastparquet', index=False)
    s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    s3.upload_file(file_path, bucket, s3_key)
    os.remove(file_path)
    print(f'✅ Uploaded {len(records)} records to s3://{bucket}/{s3_key}')

# Batch consume
batch_size = 50
buffer = {
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}

print("✅ Connected to Kafka. Listening for messages...")

for message in consumer:
    topic = message.topic
    event = message.value
    payload = event.get("payload", {})
    record = payload.get("after")  # Only take the actual row

    if record:
        buffer[topic].append(record)
        print(f"[{topic}] -> {record}")  # Debugging

    if len(buffer[topic]) >= batch_size:
        write_to_minio(topic.split('.')[-1], buffer[topic])
        buffer[topic] = []