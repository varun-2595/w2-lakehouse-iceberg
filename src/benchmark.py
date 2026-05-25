import logging
import os
import time
import duckdb
import pandas as pd
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, DoubleType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

from src.config import TABLE_GOLD_REVENUE, WAREHOUSE_PATH, RUNNING_IN_DOCKER
from src.lakehouse import get_catalog, setup_name_mapping

logger = logging.getLogger(__name__)

# Schema for zone revenue
REVENUE_SCHEMA = Schema(
    NestedField(field_id=1, name="PULocationID", field_type=LongType(), required=False),
    NestedField(field_id=2, name="DOLocationID", field_type=LongType(), required=False),
    NestedField(field_id=3, name="total_fare", field_type=DoubleType(), required=False),
    NestedField(field_id=4, name="total_tip", field_type=DoubleType(), required=False),
    NestedField(field_id=5, name="avg_tip_percent", field_type=DoubleType(), required=False)
)

# Partition Spec: Partition by PULocationID (field_id=1)
PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=1,
        field_id=1001,
        transform=IdentityTransform(),
        name="PULocationID_part"
    )
)

def run_benchmark():
    """
    Benchmarks query latency of zone_revenue_summary:
    1. Unpartitioned / Unsorted (Baseline)
    2. Partitioned by PULocationID
    """
    logger.info("Starting Gold table query benchmarking...")
    catalog = get_catalog()
    
    # 1. Fetch clean Silver data to build Gold
    t_silver = catalog.load_table("silver.taxi_trips")
    df_silver = t_silver.scan().to_pandas()
    if df_silver.empty:
        logger.error("No Silver data available for benchmarking.")
        return 0.0, 0.0, 0.0
        
    df_revenue = df_silver.groupby(["PULocationID", "DOLocationID"]).agg(
        total_fare=("fare_amount", "sum"),
        total_tip=("tip_amount", "sum"),
        avg_tip_percent=("tip_amount", lambda x: (x / df_silver.loc[x.index, "fare_amount"].clip(lower=0.1)).mean() * 100)
    ).reset_index()
    df_revenue["avg_tip_percent"] = df_revenue["avg_tip_percent"].round(2)
    arrow_table = pa.Table.from_pandas(df_revenue, preserve_index=False)
    
    # ----------------------------------------------------
    # Stage A: Unpartitioned (Baseline)
    # ----------------------------------------------------
    logger.info("Stage A: Loading unpartitioned Gold table...")
    try:
        catalog.drop_table(TABLE_GOLD_REVENUE)
    except Exception:
        pass
        
    t_gold_unpart = catalog.create_table(TABLE_GOLD_REVENUE, schema=REVENUE_SCHEMA)
    setup_name_mapping(t_gold_unpart)
    t_gold_unpart.append(arrow_table)
    
    # Measure Query Latency in DuckDB
    # We choose a target Location ID present in our dataset (e.g. median location)
    target_zone = int(df_revenue["PULocationID"].median())
    logger.info(f"Target PULocationID for benchmark search: {target_zone}")
    
    # Run the query multiple times to get a stable benchmark
    num_runs = 50
    latency_unpart = 0.0
    
    if not RUNNING_IN_DOCKER:
        local_table_path = os.path.join(WAREHOUSE_PATH, "gold", "zone_revenue_summary")
        conn = duckdb.connect()
        conn.execute("INSTALL iceberg; LOAD iceberg;")
        conn.execute("SET unsafe_enable_version_guessing = true;")
        
        # Warmup run
        conn.execute(f"SELECT * FROM iceberg_scan('{local_table_path.replace(os.sep, '/')}') WHERE PULocationID = {target_zone}").fetchall()
        
        t0 = time.perf_counter()
        for _ in range(num_runs):
            conn.execute(f"SELECT * FROM iceberg_scan('{local_table_path.replace(os.sep, '/')}') WHERE PULocationID = {target_zone}").fetchall()
        latency_unpart = (time.perf_counter() - t0) / num_runs * 1000 # in ms
        logger.info(f"Unpartitioned Query Latency: {latency_unpart:.4f} ms")
        
    # ----------------------------------------------------
    # Stage B: Partitioned by PULocationID
    # ----------------------------------------------------
    logger.info("Stage B: Loading partitioned Gold table...")
    try:
        catalog.drop_table(TABLE_GOLD_REVENUE)
    except Exception:
        pass
        
    # Create partitioned table
    t_gold_part = catalog.create_table(
        TABLE_GOLD_REVENUE, 
        schema=REVENUE_SCHEMA, 
        partition_spec=PARTITION_SPEC
    )
    setup_name_mapping(t_gold_part)
    
    # To maximize speedups, we write sorted data (so partition boundaries are clean)
    df_revenue_sorted = df_revenue.sort_values(by="PULocationID").copy()
    t_gold_part.append(pa.Table.from_pandas(df_revenue_sorted, preserve_index=False))
    
    latency_part = 0.0
    if not RUNNING_IN_DOCKER:
        # Re-warmup
        conn.execute(f"SELECT * FROM iceberg_scan('{local_table_path.replace(os.sep, '/')}') WHERE PULocationID = {target_zone}").fetchall()
        
        t0 = time.perf_counter()
        for _ in range(num_runs):
            conn.execute(f"SELECT * FROM iceberg_scan('{local_table_path.replace(os.sep, '/')}') WHERE PULocationID = {target_zone}").fetchall()
        latency_part = (time.perf_counter() - t0) / num_runs * 1000 # in ms
        logger.info(f"Partitioned Query Latency: {latency_part:.4f} ms")
        
    speedup = ((latency_unpart - latency_part) / latency_unpart) * 100 if latency_unpart > 0 else 0.0
    logger.info(f"Benchmark results: Speedup of {speedup:.2f}% achieved via partitioning & sorting!")
    
    return latency_unpart, latency_part, speedup

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_benchmark()
