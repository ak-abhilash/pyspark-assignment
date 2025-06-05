import pandas as pd
import boto3
import time
import uuid
import os
import tempfile
import psycopg2
import gdown

# All these env variable will be deleted post use
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region = 'ap-south-1'
bucket_name = 'devdolphins-assignment-bucket'
folder_prefix = 'transactions_chunks/'

file_id = '1iPOLzXDHD6uadCZlM7gltuwkPtO5eCX2'
local_csv = 'transactions.csv'

db_config = {
    'host': 'dpg-d10n19e3jp1c7395oh4g-a.oregon-postgres.render.com',
    'database': 'devdolphins_temp',
    'user': 'devdolphins_temp_user',
    'password': '0PMec0Rom6SrAwuqpUPwxOvVLRaYBY1e',
    'port': '5432',
    'sslmode': 'require'
}

def download_csv(file_id, output):
    url = f'https://drive.google.com/uc?id={file_id}'
    gdown.download(url, output, quiet=True)

def log_to_postgres(chunk_name):
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO processed_chunks (chunk_name) VALUES (%s) ON CONFLICT (chunk_name) DO NOTHING",
        (chunk_name,)
    )
    conn.commit()
    cur.close()
    conn.close()

# Initialize S3 client
s3 = boto3.client(
    's3',
    region_name=region,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# Download the CSV if not already present
if not os.path.exists(local_csv):
    download_csv(file_id, local_csv)

# Read CSV in chunks and upload each chunk to S3 and log in Postgres
for i, chunk in enumerate(pd.read_csv(local_csv, chunksize=10000)):
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp:
        chunk.to_csv(temp.name, index=False)
        temp_path = temp.name

    # Generate a unique chunk filename for S3
    chunk_name = f"chunk_{i}_{uuid.uuid4().hex[:6]}.csv"
    s3_key = folder_prefix + chunk_name

    try:
        s3.upload_file(temp_path, bucket_name, s3_key)
        print(f"Uploaded: {s3_key}")
    except Exception as e:
        print(f"Failed to upload chunk {i}: {e}")

    # Log the uploaded chunk to Postgres
    log_to_postgres(chunk_name)

    # Remove temporary file
    os.remove(temp_path)

    # Sleep to avoid throttling
    time.sleep(1)

print(f"All chunks processed and uploaded to S3 bucket '{bucket_name}' with prefix '{folder_prefix}'.")
