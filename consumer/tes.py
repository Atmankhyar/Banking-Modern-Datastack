import boto3
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import pandas as pd
from datetime import datetime
import os
import logging
import signal
import sys
from dotenv import load_dotenv

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# -----------------------------
# Load secrets from .env
# -----------------------------
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_path)

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_GROUP      = os.getenv("KAFKA_GROUP")
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET")

missing_vars = [
    name for name, value in [
        ("KAFKA_BOOTSTRAP",  KAFKA_BOOTSTRAP),
        ("KAFKA_GROUP",      KAFKA_GROUP),
        ("MINIO_ENDPOINT",   MINIO_ENDPOINT),
        ("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY),
        ("MINIO_SECRET_KEY", MINIO_SECRET_KEY),
        ("MINIO_BUCKET",     MINIO_BUCKET),
    ] if not value
]
if missing_vars:
    raise EnvironmentError(
        f"Missing required environment variables in {dotenv_path}: {', '.join(missing_vars)}"
    )

# -----------------------------
# Config
# -----------------------------
TOPICS = [
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions',
]

# Transactions get a smaller batch (more files, partitioned by date).
# Other tables flush at 50 records.
BATCH_SIZES = {
    'banking_server.public.customers':    50,
    'banking_server.public.accounts':     50,
    'banking_server.public.transactions': 20,   # flush more often → more parquet files
}

# Column used to partition transactions by date.
# Change this to match your actual column name.
TRANSACTION_DATE_COL = 'transaction_date'

# -----------------------------
# MinIO / S3 client
# -----------------------------
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

def ensure_bucket(bucket: str) -> None:
    existing = [b['Name'] for b in s3.list_buckets().get('Buckets', [])]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        log.info(f"Created bucket: {bucket}")
    else:
        log.info(f"Bucket already exists: {bucket}")

ensure_bucket(MINIO_BUCKET)

# -----------------------------
# Upload helpers
# -----------------------------
def _upload_parquet(df: pd.DataFrame, s3_key: str, local_path: str) -> None:
    """Write df to a local parquet file, upload to MinIO, then clean up."""
    df.to_parquet(local_path, engine='fastparquet', index=False)
    s3.upload_file(local_path, MINIO_BUCKET, s3_key)
    os.remove(local_path)


def write_to_minio(table_name: str, records: list) -> None:
    """
    Upload records to MinIO.
    - transactions  → split by date partition (one parquet file per date)
    - other tables  → single parquet file for the current date
    """
    if not records:
        return

    df = pd.DataFrame(records)
    now_str = datetime.now().strftime('%H%M%S%f')
    date_str = datetime.now().strftime('%Y-%m-%d')

    if table_name == 'transactions':
        _write_transactions(df, now_str)
    else:
        local_path = f'{table_name}_{date_str}_{now_str}.parquet'
        s3_key = f'{table_name}/date={date_str}/{table_name}_{now_str}.parquet'
        _upload_parquet(df, s3_key, local_path)
        log.info(f"✅ [{table_name}] {len(records)} records → s3://{MINIO_BUCKET}/{s3_key}")


def _write_transactions(df: pd.DataFrame, now_str: str) -> None:
    """
    Partition transactions by their date column so each calendar day
    gets its own parquet file — matching a real lakehouse layout.
    """
    if TRANSACTION_DATE_COL in df.columns:
        df['_part_date'] = pd.to_datetime(df[TRANSACTION_DATE_COL], errors='coerce').dt.date
    else:
        # Fallback: use today's date if the column doesn't exist
        log.warning(
            f"Column '{TRANSACTION_DATE_COL}' not found in transactions — "
            "falling back to today's date for partitioning."
        )
        df['_part_date'] = datetime.now().date()

    for part_date, group in df.groupby('_part_date'):
        date_str = str(part_date)
        local_path = f'transactions_{date_str}_{now_str}.parquet'
        s3_key = f'transactions/date={date_str}/transactions_{now_str}.parquet'
        chunk = group.drop(columns='_part_date')
        _upload_parquet(chunk, s3_key, local_path)
        log.info(
            f"✅ [transactions] {len(chunk)} records (date={date_str}) "
            f"→ s3://{MINIO_BUCKET}/{s3_key}"
        )

# -----------------------------
# Kafka consumer
# -----------------------------
try:
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=KAFKA_GROUP,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=-1,   # block forever (use Ctrl+C to stop)
    )
except NoBrokersAvailable as exc:
    raise ConnectionError(
        f"Cannot connect to Kafka broker at {KAFKA_BOOTSTRAP}. "
        "Confirm the broker is running and the address is reachable."
    ) from exc

# -----------------------------
# Graceful shutdown
# -----------------------------
buffer = {topic: [] for topic in TOPICS}

def _flush_all_and_exit(sig, frame):
    log.info("🛑 Shutdown signal received — flushing remaining buffers...")
    for topic, records in buffer.items():
        if records:
            table = topic.split('.')[-1]
            log.info(f"  Flushing {len(records)} remaining records for [{table}]")
            write_to_minio(table, records)
    consumer.close()
    log.info("👋 Consumer closed. Goodbye.")
    sys.exit(0)

signal.signal(signal.SIGINT,  _flush_all_and_exit)
signal.signal(signal.SIGTERM, _flush_all_and_exit)

# -----------------------------
# Main loop
# -----------------------------
log.info("✅ Connected to Kafka. Listening for messages... (Ctrl+C to stop)")

for message in consumer:
    topic  = message.topic
    event  = message.value
    record = event.get("payload", {}).get("after")   # Debezium CDC: only INSERT/UPDATE rows

    if record is None:
        # DELETE event or tombstone — skip
        continue

    buffer[topic].append(record)
    log.debug(f"[{topic}] buffered record: {record}")

    # Flush when batch is full
    if len(buffer[topic]) >= BATCH_SIZES[topic]:
        table = topic.split('.')[-1]
        log.info(f"[{table}] batch full ({BATCH_SIZES[topic]} records) — uploading...")
        write_to_minio(table, buffer[topic])
        buffer[topic] = []