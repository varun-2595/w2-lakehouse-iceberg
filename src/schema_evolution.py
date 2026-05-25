import logging
import os
import pandas as pd
import pyarrow as pa
from pyiceberg.types import DoubleType

from src.config import TABLE_SILVER_TRIPS, TABLE_BRONZE_TRIPS
from src.lakehouse import get_catalog, setup_name_mapping, make_fingerprint, mask_name

logger = logging.getLogger(__name__)

def evolve_silver_schema():
    """
    Alters the silver.taxi_trips schema by adding a new derived column:
    'trip_duration_minutes'
    """
    logger.info(f"Evolving schema for table '{TABLE_SILVER_TRIPS}'...")
    catalog = get_catalog()
    table = catalog.load_table(TABLE_SILVER_TRIPS)
    
    # Check if column already exists
    current_cols = [field.name for field in table.schema().fields]
    if "trip_duration_minutes" in current_cols:
        logger.warning("Column 'trip_duration_minutes' already exists in Silver schema. Skipping alteration.")
        return False
        
    try:
        # Perform schema evolution: Add derived column
        with table.update_schema() as update:
            update.add_column(
                path="trip_duration_minutes",
                field_type=DoubleType(),
                doc="Derived trip duration in minutes (dropoff - pickup datetime)"
            )

        logger.info(f"Evolved '{TABLE_SILVER_TRIPS}' successfully: added column 'trip_duration_minutes'.")
        
        # Re-set name mapping to include the new column
        # Load table again to get the fresh schema
        table = catalog.load_table(TABLE_SILVER_TRIPS)
        setup_name_mapping(table)
        return True
    except Exception as e:
        logger.error(f"Failed to evolve Silver schema: {e}")
        raise

def process_batch_2_to_silver():
    """
    Processes Batch 2 records from Bronze to Silver.
    Derived column 'trip_duration_minutes' is calculated and loaded into the evolved table.
    """
    logger.info("Processing Batch 2 records from Bronze to Silver with schema evolution...")
    catalog = get_catalog()
    
    t_bronze = catalog.load_table(TABLE_BRONZE_TRIPS)
    t_silver = catalog.load_table(TABLE_SILVER_TRIPS)
    
    # Scan Bronze to find new records (we filter by source_file == yellow_taxi_batch_2.parquet)
    # This allows us to load only Batch 2 records!
    df_bronze = t_bronze.scan().to_pandas()
    df_batch_2 = df_bronze[df_bronze["source_file"] == "yellow_taxi_batch_2.parquet"].copy()
    
    if df_batch_2.empty:
        logger.warning("No Batch 2 records found in Bronze. Make sure you ingested Batch 2 first.")
        return 0, 0, 0.0
        
    total_raw = len(df_batch_2)
    
    # 1. Compute Trip Fingerprint
    df_batch_2["trip_fingerprint"] = df_batch_2.apply(make_fingerprint, axis=1)
    
    # 2. PII Masking: initials masking
    df_batch_2["driver_name"] = df_batch_2["driver_name"].apply(mask_name)
    
    # 3. Type Normalization: store_and_fwd_flag Y/N -> boolean
    df_batch_2["store_and_fwd_flag"] = df_batch_2["store_and_fwd_flag"].apply(
        lambda val: True if str(val).strip().upper() == "Y" else False
    )
    
    # 4. Deduplicate Batch 2 against itself
    dup_rows = df_batch_2[df_batch_2.duplicated(subset=["trip_fingerprint"], keep="first")]
    num_duplicates = len(dup_rows)
    
    df_clean = df_batch_2.drop_duplicates(subset=["trip_fingerprint"], keep="first").copy()
    
    # 5. Derive the new evolved column 'trip_duration_minutes'
    # trip_duration = (tpep_dropoff_datetime - tpep_pickup_datetime) in minutes
    df_clean["tpep_pickup_datetime"] = pd.to_datetime(df_clean["tpep_pickup_datetime"])
    df_clean["tpep_dropoff_datetime"] = pd.to_datetime(df_clean["tpep_dropoff_datetime"])
    
    durations = (df_clean["tpep_dropoff_datetime"] - df_clean["tpep_pickup_datetime"]).dt.total_seconds() / 60.0
    df_clean["trip_duration_minutes"] = durations.round(2)
    
    total_clean = len(df_clean)
    dedup_rate = (num_duplicates / total_raw) * 100 if total_raw > 0 else 0.0
    
    # Ensure column ordering matches silver schema
    silver_cols = [f.name for f in t_silver.schema().fields]
    existing_silver_cols = [col for col in silver_cols if col in df_clean.columns]
    df_clean_ordered = df_clean[existing_silver_cols].copy()
    
    # Write to Iceberg
    arrow_table = pa.Table.from_pandas(df_clean_ordered, preserve_index=False)
    t_silver.append(arrow_table)
    logger.info(f"Appended {len(df_clean_ordered)} Batch 2 rows to Silver.")
    
    return total_raw, total_clean, dedup_rate

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    evolve_silver_schema()
