import pandas as pd
import psycopg2
import boto3
import json
from fpl_fetch import fetch_bootstrap

# --------------------------
# Runtime-safe credential functions
# --------------------------
import os

def get_db_config():
    return {
        'host': os.environ.get('DB_HOST', 'postgres'),
        'dbname': os.environ['POSTGRES_DB'],
        'user': os.environ['POSTGRES_USER'],
        'password': os.environ['POSTGRES_PASSWORD']
    }

def get_s3_client():
    access_key = os.environ.get('MINIO_ROOT_USER')
    secret_key = os.environ.get('MINIO_ROOT_PASSWORD')
    endpoint = os.environ.get('S3_ENDPOINT', 'http://minio:9000')

    if not access_key or not secret_key:
        raise ValueError("MinIO credentials not found in environment variables!")

    s3 = boto3.client(
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    return s3

def get_s3_buckets():
    raw_bucket = os.environ.get('S3_BUCKET_RAW', 'fpl_raw')
    processed_bucket = os.environ.get('S3_BUCKET_PROCESSED', 'fpl_processed')
    return raw_bucket, processed_bucket

# --------------------------
# Pipeline helpers
# --------------------------
def upload_raw_to_s3(file_name: str, data: dict, s3_client):
    raw_bucket, _ = get_s3_buckets()
    s3_client.put_object(
        Bucket=raw_bucket,
        Key=file_name,
        Body=json.dumps(data),
        ContentType='application/json'
    )

def load_df_to_postgres(df, table_name: str):
    db_config = get_db_config()
    conn = psycopg2.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        dbname=db_config['dbname']
    )
    cur = conn.cursor()

    # Create table if not exists
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id          INT PRIMARY KEY,
        first_name  TEXT,
        second_name TEXT,
        team        INT,
        element_type INT,
        now_cost    INT,
        total_points INT,
        points_per_game TEXT
    );
    """
    cur.execute(create_sql)
    conn.commit()

    # Insert / upsert rows
    for _, row in df.iterrows():
        cur.execute(f"""
            INSERT INTO {table_name} (id, first_name, second_name, team, element_type, now_cost, total_points, points_per_game)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
              SET first_name = EXCLUDED.first_name,
                  second_name = EXCLUDED.second_name,
                  team = EXCLUDED.team,
                  element_type = EXCLUDED.element_type,
                  now_cost = EXCLUDED.now_cost,
                  t
