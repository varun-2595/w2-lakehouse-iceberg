import hashlib
import logging
import os
import time
from datetime import datetime
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, LongType, DoubleType, StringType, TimestampType, BooleanType
)
from pyiceberg.table.name_mapping import create_mapping_from_schema

from src.config import (
    CATALOG_NAME, CATALOG_TYPE, CATALOG_URI, WAREHOUSE_PATH,
    NS_BRONZE, NS_SILVER, NS_GOLD,
    TABLE_BRONZE_TRIPS, TABLE_SILVER_TRIPS,
    TABLE_GOLD_VOLUME, TABLE_GOLD_REVENUE, TABLE_GOLD_DRIVER_STATS,
    S3_REGION
)

logger = logging.getLogger(__name__)

# ----------------------------------------------------
# 1. Schemas Definition
# ----------------------------------------------------

BRONZE_SCHEMA = Schema(
    NestedField(field_id=1, name="VendorID", field_type=LongType(), required=False),
    NestedField(field_id=2, name="tpep_pickup_datetime", field_type=TimestampType(), required=False),
    NestedField(field_id=3, name="tpep_dropoff_datetime", field_type=TimestampType(), required=False),
    NestedField(field_id=4, name="passenger_count", field_type=LongType(), required=False),
    NestedField(field_id=5, name="trip_distance", field_type=DoubleType(), required=False),
    NestedField(field_id=6, name="PULocationID", field_type=LongType(), required=False),
    NestedField(field_id=7, name="DOLocationID", field_type=LongType(), required=False),
    NestedField(field_id=8, name="fare_amount", field_type=DoubleType(), required=False),
    NestedField(field_id=9, name="extra", field_type=DoubleType(), required=False),
    NestedField(field_id=10, name="mta_tax", field_type=DoubleType(), required=False),
    NestedField(field_id=11, name="tip_amount", field_type=DoubleType(), required=False),
    NestedField(field_id=12, name="tolls_amount", field_type=DoubleType(), required=False),
    NestedField(field_id=13, name="improvement_surcharge", field_type=DoubleType(), required=False),
    NestedField(field_id=14, name="total_amount", field_type=DoubleType(), required=False),
    NestedField(field_id=15, name="store_and_fwd_flag", field_type=StringType(), required=False),
    NestedField(field_id=16, name="driver_name", field_type=StringType(), required=False),
    NestedField(field_id=17, name="ingest_timestamp", field_type=TimestampType(), required=False),
    NestedField(field_id=18, name="source_file", field_type=StringType(), required=False)
)

SILVER_SCHEMA = Schema(
    NestedField(field_id=1, name="VendorID", field_type=LongType(), required=False),
    NestedField(field_id=2, name="tpep_pickup_datetime", field_type=TimestampType(), required=False),
    NestedField(field_id=3, name="tpep_dropoff_datetime", field_type=TimestampType(), required=False),
    NestedField(field_id=4, name="passenger_count", field_type=LongType(), required=False),
    NestedField(field_id=5, name="trip_distance", field_type=DoubleType(), required=False),
    NestedField(field_id=6, name="PULocationID", field_type=LongType(), required=False),
    NestedField(field_id=7, name="DOLocationID", field_type=LongType(), required=False),
    NestedField(field_id=8, name="fare_amount", field_type=DoubleType(), required=False),
    NestedField(field_id=9, name="extra", field_type=DoubleType(), required=False),
    NestedField(field_id=10, name="mta_tax", field_type=DoubleType(), required=False),
    NestedField(field_id=11, name="tip_amount", field_type=DoubleType(), required=False),
    NestedField(field_id=12, name="tolls_amount", field_type=DoubleType(), required=False),
    NestedField(field_id=13, name="improvement_surcharge", field_type=DoubleType(), required=False),
    NestedField(field_id=14, name="total_amount", field_type=DoubleType(), required=False),
    NestedField(field_id=15, name="store_and_fwd_flag", field_type=BooleanType(), required=False), # Boolean normalized
    NestedField(field_id=16, name="driver_name", field_type=StringType(), required=False),         # PII masked
    NestedField(field_id=17, name="trip_fingerprint", field_type=StringType(), required=False),     # Dedup hash
    NestedField(field_id=18, name="ingest_timestamp", field_type=TimestampType(), required=False),
    NestedField(field_id=19, name="source_file", field_type=StringType(), required=False)
)

# ----------------------------------------------------
# 2. Connection Helpers
# ----------------------------------------------------

def get_catalog():
    """Initializes PyIceberg Catalog connection based on config."""
    properties = {
        "type": CATALOG_TYPE,
        "uri": CATALOG_URI,
        "warehouse": WAREHOUSE_PATH
    }
    
    # Add S3 configurations in Docker mode
    if CATALOG_TYPE == "rest":
        properties.update({
            "s3.endpoint": os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000"),
            "s3.access-key-id": os.environ.get("S3_ACCESS_KEY", "minioadmin"),
            "s3.secret-access-key": os.environ.get("S3_SECRET_KEY", "minioadmin"),
            "s3.path-style-access": "true",
            "s3.region": S3_REGION
        })
        
    return load_catalog(CATALOG_NAME, **properties)

def setup_name_mapping(table):
    """Sets schema name-mapping on the Iceberg table so PyArrow appends work by name."""
    mapping = create_mapping_from_schema(table.schema())
    with table.transaction() as tx:
        tx.set_properties({"schema.name-mapping.default": mapping.model_dump_json()})

def ensure_bucket():
    """Ensures that the MinIO warehouse bucket exists when running in Docker mode."""
    from src.config import RUNNING_IN_DOCKER, S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY
    if not RUNNING_IN_DOCKER:
        return
    
    import boto3
    from botocore.exceptions import ClientError
    
    logger.info("Verifying MinIO bucket 'warehouse' exists...")
    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1"
    )
    try:
        s3_client.create_bucket(Bucket="warehouse")
        logger.info("MinIO Bucket 'warehouse' created successfully.")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            logger.info("MinIO Bucket 'warehouse' already exists.")
        else:
            logger.error(f"Error checking/creating MinIO bucket: {e}")
            raise

def initialize_lakehouse():
    """Creates namespaces and base medallion tables if they don't exist."""
    ensure_bucket()
    catalog = get_catalog()
    logger.info("Initializing lakehouse namespaces and tables...")
    
    # 1. Create Namespaces
    namespaces = [NS_BRONZE, NS_SILVER, NS_GOLD]
    for ns in namespaces:
        try:
            catalog.create_namespace(ns)
            logger.info(f"Namespace '{ns}' created successfully.")
        except Exception:
            # Already exists
            pass
            
    # 2. Create Bronze Table
    try:
        t_bronze = catalog.create_table(TABLE_BRONZE_TRIPS, schema=BRONZE_SCHEMA)
        setup_name_mapping(t_bronze)
        logger.info(f"Table '{TABLE_BRONZE_TRIPS}' created.")
    except Exception as e:
        logger.error(f"Failed to create Table '{TABLE_BRONZE_TRIPS}': {e}")
        
    # 3. Create Silver Table
    try:
        t_silver = catalog.create_table(TABLE_SILVER_TRIPS, schema=SILVER_SCHEMA)
        setup_name_mapping(t_silver)
        logger.info(f"Table '{TABLE_SILVER_TRIPS}' created.")
    except Exception as e:
        logger.error(f"Failed to create Table '{TABLE_SILVER_TRIPS}': {e}")
        
    return catalog


# ----------------------------------------------------
# 3. Medallion Stages
# ----------------------------------------------------

def ingest_to_bronze(parquet_path):
    """
    Bronze Ingestion (Append-only):
    Reads Parquet file, adds ingest_timestamp and source_file metadata columns,
    and appends to bronze.taxi_trips.
    """
    logger.info(f"Ingesting raw Parquet file {parquet_path} to Bronze layer...")
    catalog = get_catalog()
    table = catalog.load_table(TABLE_BRONZE_TRIPS)
    
    # Read Parquet
    df = pd.read_parquet(parquet_path)
    
    # Add metadata columns
    df["ingest_timestamp"] = datetime.now()
    df["source_file"] = os.path.basename(parquet_path)
    
    # Write to Iceberg
    arrow_table = pa.Table.from_pandas(df, schema=pa.schema([
        (name, pa.timestamp("us") if dtype.name.startswith("datetime") else 
               pa.int64() if dtype.name.startswith("int") else 
               pa.float64() if dtype.name.startswith("float") else pa.string())
        for name, dtype in df.dtypes.items()
    ]), preserve_index=False)
    
    table.append(arrow_table)
    logger.info(f"Appended {len(df)} rows to '{TABLE_BRONZE_TRIPS}'.")
    return len(df)

def make_fingerprint(row):
    """Generates fingerprint hash of a trip record for deduplication."""
    val = f"{row['VendorID']}_{row['tpep_pickup_datetime']}_{row['PULocationID']}_{row['DOLocationID']}_{row['fare_amount']}"
    return hashlib.sha256(val.encode()).hexdigest()

def mask_name(name):
    """PII Masking: Converts name (e.g. John Smith) to initials (J.S.)."""
    if not name or not isinstance(name, str):
        return ""
    return "".join([part[0].upper() + "." for part in name.split() if part])

def process_to_silver():
    """
    Silver Processing:
    Loads raw Bronze data, applies trip fingerprint hash deduplication,
    masks driver PII names, normalizes data types, and appends to silver.taxi_trips.
    """
    logger.info("Processing Bronze data to Silver layer...")
    catalog = get_catalog()
    
    t_bronze = catalog.load_table(TABLE_BRONZE_TRIPS)
    t_silver = catalog.load_table(TABLE_SILVER_TRIPS)
    
    # Scan all Bronze data
    df_bronze = t_bronze.scan().to_pandas()
    if df_bronze.empty:
        logger.warning("No records in Bronze table to process.")
        return 0, 0, 0.0
        
    # Get total records
    total_raw = len(df_bronze)
    
    # 1. Compute Trip Fingerprint
    df_bronze["trip_fingerprint"] = df_bronze.apply(make_fingerprint, axis=1)
    
    # 2. PII Masking: initials masking
    df_bronze["driver_name"] = df_bronze["driver_name"].apply(mask_name)
    
    # 3. Type Normalization: store_and_fwd_flag string (Y/N) -> boolean (True/False)
    df_bronze["store_and_fwd_flag"] = df_bronze["store_and_fwd_flag"].apply(
        lambda val: True if str(val).strip().upper() == "Y" else False
    )
    
    # 4. Deduplication by fingerprint
    # Find duplicate records
    dup_rows = df_bronze[df_bronze.duplicated(subset=["trip_fingerprint"], keep="first")]
    num_duplicates = len(dup_rows)
    
    df_clean = df_bronze.drop_duplicates(subset=["trip_fingerprint"], keep="first").copy()
    total_clean = len(df_clean)
    
    dedup_rate = (num_duplicates / total_raw) * 100 if total_raw > 0 else 0.0
    logger.info(f"Deduplication rate: {dedup_rate:.2f}% (Seeded duplicates: {num_duplicates}, Clean: {total_clean})")
    
    # Ensure column ordering matches silver schema
    silver_cols = [f.name for f in t_silver.schema().fields]
    # Filter out columns that might not exist yet (like evolved columns during initial setup)
    existing_silver_cols = [col for col in silver_cols if col in df_clean.columns]
    df_clean_ordered = df_clean[existing_silver_cols].copy()
    
    # Write to Iceberg
    arrow_table = pa.Table.from_pandas(df_clean_ordered, preserve_index=False)
    
    # Idempotency / overwrite style:
    # PyIceberg does not support complete overwrite natively in simple way except tx.overwrite
    # So we write new transactions. For the demo, we append
    t_silver.append(arrow_table)
    logger.info(f"Appended {len(df_clean_ordered)} clean rows to '{TABLE_SILVER_TRIPS}'.")
    
    return total_raw, total_clean, dedup_rate

def generate_gold_tables():
    """
    Gold Aggregations:
    Performs aggregations on Silver data and writes to Gold tables:
    - hourly_trip_volume
    - zone_revenue_summary
    - driver_trip_stats
    """
    logger.info("Generating Gold analytical aggregates...")
    catalog = get_catalog()
    t_silver = catalog.load_table(TABLE_SILVER_TRIPS)
    
    df_silver = t_silver.scan().to_pandas()
    if df_silver.empty:
        logger.warning("No records in Silver table to aggregate.")
        return
        
    # Grouping 1: hourly_trip_volume
    # Convert pickup datetime to hour
    df_silver["pickup_hour"] = pd.to_datetime(df_silver["tpep_pickup_datetime"]).dt.hour
    df_volume = df_silver.groupby(["PULocationID", "pickup_hour"]).size().reset_index(name="trip_count")
    
    # Grouping 2: zone_revenue_summary
    df_revenue = df_silver.groupby(["PULocationID", "DOLocationID"]).agg(
        total_fare=("fare_amount", "sum"),
        total_tip=("tip_amount", "sum"),
        avg_tip_percent=("tip_amount", lambda x: (x / df_silver.loc[x.index, "fare_amount"].clip(lower=0.1)).mean() * 100)
    ).reset_index()
    df_revenue["avg_tip_percent"] = df_revenue["avg_tip_percent"].round(2)
    
    # Grouping 3: driver_trip_stats
    df_drivers = df_silver.groupby("driver_name").agg(
        total_trips=("VendorID", "count"),
        total_revenue=("total_amount", "sum"),
        avg_fare=("fare_amount", "mean")
    ).reset_index()
    df_drivers["avg_fare"] = df_drivers["avg_fare"].round(2)
    
    # Write to Gold Tables
    # In PyIceberg, we can auto-generate or create tables
    for tbl_name, df_agg, schema_types in [
        (TABLE_GOLD_VOLUME, df_volume, {"PULocationID": LongType(), "pickup_hour": LongType(), "trip_count": LongType()}),
        (TABLE_GOLD_REVENUE, df_revenue, {"PULocationID": LongType(), "DOLocationID": LongType(), "total_fare": DoubleType(), "total_tip": DoubleType(), "avg_tip_percent": DoubleType()}),
        (TABLE_GOLD_DRIVER_STATS, df_drivers, {"driver_name": StringType(), "total_trips": LongType(), "total_revenue": DoubleType(), "avg_fare": DoubleType()})
    ]:
        arrow_table = pa.Table.from_pandas(df_agg, preserve_index=False)
        
        # Build schema
        fields = []
        for fid, (col, dtype) in enumerate(schema_types.items(), start=1):
            fields.append(NestedField(field_id=fid, name=col, field_type=dtype, required=False))
        schema = Schema(*fields)
        
        try:
            t_gold = catalog.create_table(tbl_name, schema=schema)
            setup_name_mapping(t_gold)
            logger.info(f"Gold table '{tbl_name}' created.")
        except Exception:
            t_gold = catalog.load_table(tbl_name)
            logger.info(f"Gold table '{tbl_name}' loaded.")
            
        # Append data (for demo simplicity, we overwrite by clearing/deleting table first or appending)
        # To simulate overwrite, we recreate or drop table. Let's do drop and recreate for Gold to ensure clean aggregates
        try:
            catalog.drop_table(tbl_name)
            t_gold = catalog.create_table(tbl_name, schema=schema)
            setup_name_mapping(t_gold)
        except Exception:
            pass
            
        t_gold.append(arrow_table)
        logger.info(f"Populated {len(df_agg)} rows in '{tbl_name}'")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    initialize_lakehouse()
