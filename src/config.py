import os

# Base Directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw_taxi_records")
REPORTS_DIR = os.path.join(DATA_DIR, "reports")

# Environment Flag
RUNNING_IN_DOCKER = os.environ.get("RUNNING_IN_DOCKER", "False").lower() == "true"

# Iceberg Catalog Configurations
if RUNNING_IN_DOCKER:
    # Docker Production Setup (REST catalog + MinIO S3)
    CATALOG_NAME = "demo"
    CATALOG_TYPE = "rest"
    CATALOG_URI = os.environ.get("REST_CATALOG_URI", "http://rest-catalog:8181")
    WAREHOUSE_PATH = "s3a://warehouse/"
    
    # MinIO / S3 config
    S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "http://minio:9000")
    S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
    S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
    S3_REGION = "us-east-1"
else:
    # Local Simulation Setup (SQL catalog + SQLite + Local filesystem)
    CATALOG_NAME = "local_catalog"
    CATALOG_TYPE = "sql"
    CATALOG_URI = f"sqlite:///{os.path.join(DATA_DIR, 'iceberg_catalog.db')}"
    WAREHOUSE_PATH = "data/warehouse"
    
    # No active S3 client needed locally
    S3_ENDPOINT_URL = None
    S3_ACCESS_KEY = None
    S3_SECRET_KEY = None
    S3_REGION = None

# Namespaces
NS_BRONZE = "bronze"
NS_SILVER = "silver"
NS_GOLD = "gold"

# Table Names
TABLE_BRONZE_TRIPS = f"{NS_BRONZE}.taxi_trips"
TABLE_SILVER_TRIPS = f"{NS_SILVER}.taxi_trips"
TABLE_GOLD_VOLUME = f"{NS_GOLD}.hourly_trip_volume"
TABLE_GOLD_REVENUE = f"{NS_GOLD}.zone_revenue_summary"
TABLE_GOLD_DRIVER_STATS = f"{NS_GOLD}.driver_trip_stats"

# Local Outputs Config
RAW_BATCH_1_PATH = os.path.join(RAW_DATA_DIR, "yellow_taxi_batch_1.parquet")
RAW_BATCH_2_PATH = os.path.join(RAW_DATA_DIR, "yellow_taxi_batch_2.parquet")
REPORT_PATH = os.path.join(REPORTS_DIR, "lakehouse_summary.html")

# Create local folders
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)
if not RUNNING_IN_DOCKER:
    os.makedirs(os.path.join(DATA_DIR, "warehouse"), exist_ok=True)


