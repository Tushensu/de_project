import os

# Postgres connection
DB_HOST     = os.environ.get('DB_HOST', 'postgres')   # defaults to service name
DB_NAME     = os.environ['POSTGRES_DB']
DB_USER     = os.environ['POSTGRES_USER']
DB_PASSWORD = os.environ['POSTGRES_PASSWORD']

# MinIO connection
S3_ENDPOINT = os.environ.get('S3_ENDPOINT', 'http://minio:9000')
S3_ACCESS_KEY = os.environ['MINIO_ROOT_USER']
S3_SECRET_KEY = os.environ['MINIO_ROOT_PASSWORD']
S3_BUCKET_RAW = os.environ.get('S3_BUCKET_RAW', 'fpl_raw')
S3_BUCKET_PROCESSED = os.environ.get('S3_BUCKET_PROCESSED', 'fpl_processed')

# FPL API
FPL_BASE_URL = "https://fantasy.premierleague.com/api"
