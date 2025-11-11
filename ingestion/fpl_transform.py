import pandas as pd
import psycopg2
import boto3
import json
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD
from config import S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET_RAW

def load_df_to_postgres(df, table_name: str):
    conn = psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )
    cur = conn.cursor()
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

def upload_raw_to_s3(file_name: str, data: dict, s3_client):
    s3_client.put_object(
        Bucket=S3_BUCKET_RAW,
        Key=file_name,
        Body=json.dumps(data),
        ContentType='application/json'
    )

def run_pipeline():
    from fpl_fetch import fetch_bootstrap
    raw_data = fetch_bootstrap()

    # initialize S3 client
    s3 = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )

    # upload raw JSON
    upload_raw_to_s3('bootstrap_static.json', raw_data, s3)

    # transform players
    players = raw_data['elements']
    df_players = pd.json_normalize(players)
    df_players = df_players[['id','first_name','second_name','team','element_type','now_cost','total_points','points_per_game']]

    # load into Postgres
    load_df_to_postgres(df_players, 'players')

    print("Pipeline completed successfully.")
