import os

# Postgres connection
DB_HOST     = os.environ.get('DB_HOST', 'postgres')  # Docker service name
DB_NAME     = os.environ['POSTGRES_DB']             # matches .env
DB_USER     = os.environ['POSTGRES_USER']           # matches .env
DB_PASSWORD = os.environ['POSTGRES_PASSWORD']       # matches .env

# MinIO connection
S3_ENDPOINT = os.environ.get('S3_ENDPOINT', 'http://minio:9000')
S3_ACCESS_KEY = os.environ.get('MINIO_ROOT_USER')
S3_SECRET_KEY = os.environ.get('MINIO_ROOT_PASSWORD')

if not S3_ACCESS_KEY or not S3_SECRET_KEY:
    raise ValueError("MinIO credentials not found in environment variables!")

S3_BUCKET_RAW = os.environ.get('S3_BUCKET_RAW', 'fpl_raw')
S3_BUCKET_PROCESSED = os.environ.get('S3_BUCKET_PROCESSED', 'fpl_processed')

# FPL API
FPL_BASE_URL = "https://fantasy.premierleague.com/api"

