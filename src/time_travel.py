import logging
import os
import pandas as pd
import duckdb
from src.config import TABLE_SILVER_TRIPS, WAREHOUSE_PATH, RUNNING_IN_DOCKER
from src.lakehouse import get_catalog

logger = logging.getLogger(__name__)

def get_snapshot_history():
    """
    Retrieves the list of snapshots and their timestamps for silver.taxi_trips.
    """
    catalog = get_catalog()
    table = catalog.load_table(TABLE_SILVER_TRIPS)
    
    snapshots = list(table.snapshots())
    logger.info(f"Retrieved {len(snapshots)} snapshots for '{TABLE_SILVER_TRIPS}':")
    
    history = []
    for idx, snap in enumerate(snapshots):
        # Convert timestamp_ms to readable datetime
        dt = pd.to_datetime(snap.timestamp_ms, unit="ms")
        logger.info(f" - Snapshot {idx+1}: ID={snap.snapshot_id}, Timestamp={dt}, Operation={snap.summary.operation}")
        history.append({
            "index": idx + 1,
            "snapshot_id": snap.snapshot_id,
            "timestamp": dt,
            "operation": snap.summary.operation
        })
        
    return history

def run_time_travel_query(snapshot_id):
    """
    Queries the silver.taxi_trips table at a specific historical snapshot ID
    using both PyIceberg and DuckDB.
    """
    catalog = get_catalog()
    table = catalog.load_table(TABLE_SILVER_TRIPS)
    
    # 1. PyIceberg Time Travel Scan
    logger.info(f"Running PyIceberg time travel scan for snapshot ID: {snapshot_id}")
    df_pyiceberg = table.scan(snapshot_id=snapshot_id).to_pandas()
    pyiceberg_count = len(df_pyiceberg)
    logger.info(f"PyIceberg Time-Travel count: {pyiceberg_count} rows")
    
    # 2. DuckDB Time Travel Scan
    duckdb_count = -1
    if not RUNNING_IN_DOCKER:
        # Locally, we can scan the folder directly using DuckDB
        local_table_path = os.path.join(WAREHOUSE_PATH, "silver", "taxi_trips")
        logger.info(f"Running DuckDB time travel query on folder: {local_table_path}")
        
        conn = duckdb.connect()
        conn.execute("INSTALL iceberg; LOAD iceberg;")
        conn.execute("SET unsafe_enable_version_guessing = true;")
        
        try:
            # In DuckDB, we can specify the snapshot ID in the scan parameters
            query = f"""
                SELECT COUNT(*) FROM iceberg_scan(
                    '{local_table_path.replace(os.sep, "/")}',
                    snapshot_from_id = {snapshot_id}
                )
            """
            duckdb_count = conn.execute(query).fetchone()[0]
            logger.info(f"DuckDB Time-Travel count: {duckdb_count} rows")
        except Exception as e:
            logger.error(f"Failed to query time-travel with DuckDB: {e}")
            # Fallback to metadata JSON file path time travel if parameters fail
            try:
                # Find metadata file
                meta_dir = os.path.join(local_table_path, "metadata")
                logger.info(f"Fallback to searching metadata files in {meta_dir}")
                # We can load by passing version or parsing metadata
                pass
            except Exception:
                pass
                
    return pyiceberg_count, duckdb_count

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    history = get_snapshot_history()
    if history:
        run_time_travel_query(history[0]["snapshot_id"])
