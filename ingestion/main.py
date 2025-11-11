import os
import psycopg2
import boto3

def test_postgres():
    try:
        conn = psycopg2.connect(
            host=os.environ['DB_HOST'],          
            user=os.environ['POSTGRES_USER'],         
            password=os.environ['POSTGRES_PASSWORD'],
            dbname=os.environ['POSTGRES_DB']          
        )
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print("Postgres connection successful:", version)
        cur.close()
        conn.close()
    except Exception as e:
        print("Postgres connection failed:", e)

def test_minio():
    try:
        s3 = boto3.client(
            's3',
            endpoint_url=os.environ['S3_ENDPOINT'],
            aws_access_key_id=os.environ['S3_ACCESS_KEY'],
            aws_secret_access_key=os.environ['S3_SECRET_KEY']
        )
        # List buckets (if none exist, should return empty list)
        response = s3.list_buckets()
        print("MinIO connection successful, buckets:", [b['Name'] for b in response.get('Buckets', [])])
    except Exception as e:
        print("MinIO connection failed:", e)

if __name__ == "__main__":
    test_postgres()
    test_minio()
