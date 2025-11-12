import os
import json
import pandas as pd
import psycopg2
import boto3
from fpl_fetch import fetch_all_endpoints, fetch_player_summaries

# --------------------------
# Config helpers (keep as before)
# --------------------------
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
        service_name='s3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    return s3

def get_s3_buckets():
    raw_bucket = os.environ.get('S3_BUCKET_RAW', 'fpl_raw')
    processed_bucket = os.environ.get('S3_BUCKET_PROCESSED', 'fpl_processed')
    return raw_bucket, processed_bucket

def ensure_bucket_exists(s3_client, bucket_name):
    existing_buckets = [b['Name'] for b in s3_client.list_buckets().get('Buckets', [])]
    if bucket_name not in existing_buckets:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Created bucket: {bucket_name}")

# --------------------------
# Pipeline helpers
# --------------------------
def upload_raw_to_s3(file_name: str, data: dict, s3_client):
    raw_bucket, _ = get_s3_buckets()
    ensure_bucket_exists(s3_client, raw_bucket)
    s3_client.put_object(
        Bucket=raw_bucket,
        Key=file_name,
        Body=json.dumps(data),
        ContentType='application/json'
    )
    print(f"Uploaded raw data to bucket: {raw_bucket}, file: {file_name}")

def load_df_to_postgres(df, table_name: str):
    db_config = get_db_config()
    conn = psycopg2.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        dbname=db_config['dbname']
    )
    cur = conn.cursor()

    # Example table, can be generalized later
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
                  total_points = EXCLUDED.total_points,
                  points_per_game = EXCLUDED.points_per_game;
        """,
        (row.id, row.first_name, row.second_name, row.team, row.element_type, row.now_cost, row.total_points, row.points_per_game))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(df)} rows into Postgres table: {table_name}")

# --------------------------
# Main pipeline function
# --------------------------
def run_pipeline():
    print("Fetching all FPL endpoints...")
    all_data = fetch_all_endpoints()

    s3_client = get_s3_client()

    # Upload all raw endpoints
    for name, data in all_data.items():
        upload_raw_to_s3(f"{name}.json", data, s3_client)

    # Transform & load players
    print("Transforming players data...")
    players = all_data['elements']['elements']
    df_players = pd.json_normalize(players)
    df_players = df_players[['id','first_name','second_name','team','element_type','now_cost','total_points','points_per_game']]
    print("Loading players data into Postgres...")
    load_df_to_postgres(df_players, 'players')

    # Fetch player summaries individually
    print("Fetching player summaries...")
    player_ids = [p['id'] for p in players]
    player_summaries = fetch_player_summaries(player_ids)
    for pid, data in player_summaries.items():
        upload_raw_to_s3(f"player_{pid}.json", data, s3_client)

    print("Pipeline completed successfully.")
