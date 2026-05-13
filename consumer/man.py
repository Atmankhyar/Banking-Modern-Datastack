import boto3
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import os

base_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=base_dir.parent / '.env', override=False)
load_dotenv(dotenv_path=base_dir / '.env', override=True)

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET')

missing_vars = [
    name for name, value in [
        ('POSTGRES_HOST', POSTGRES_HOST),
        ('POSTGRES_PORT', POSTGRES_PORT),
        ('POSTGRES_USER', POSTGRES_USER),
        ('POSTGRES_PASSWORD', POSTGRES_PASSWORD),
        ('POSTGRES_DB', POSTGRES_DB),
        ('MINIO_ENDPOINT', MINIO_ENDPOINT),
        ('MINIO_ACCESS_KEY', MINIO_ACCESS_KEY),
        ('MINIO_SECRET_KEY', MINIO_SECRET_KEY),
        ('MINIO_BUCKET', MINIO_BUCKET),
    ] if not value
]

if missing_vars:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

bucket = MINIO_BUCKET

with psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    dbname=POSTGRES_DB,
) as conn:
    customers = pd.read_sql('SELECT * FROM customers ORDER BY id', conn)
    accounts = pd.read_sql('SELECT * FROM accounts ORDER BY id', conn)
    transactions = pd.read_sql('SELECT * FROM transactions ORDER BY id', conn)


def upload(df, table_name):
    if df.empty:
        print(f'⚠️ No rows found for {table_name}; skipping upload.')
        return

    date_str = datetime.now().strftime('%Y-%m-%d')
    file_path = f'{table_name}_{date_str}.parquet'
    df.to_parquet(file_path, engine='fastparquet', index=False)
    s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S")}.parquet'
    s3.upload_file(file_path, bucket, s3_key)
    os.remove(file_path)
    print(f'✅ Uploaded {table_name} → s3://{bucket}/{s3_key}')

upload(customers, 'customers')
upload(accounts, 'accounts')
upload(transactions, 'transactions')