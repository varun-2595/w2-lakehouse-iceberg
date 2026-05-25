import logging
import os
import sys
import time
from datetime import datetime

# Set up logging to console and file
os.makedirs(os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "lakehouse_pipeline.log"), encoding="utf-8")
    ]
)
logger = logging.getLogger("LakehouseRunner")

# Ensure project root is in sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.config import (
    DATA_DIR, RAW_BATCH_1_PATH, RAW_BATCH_2_PATH, REPORT_PATH
)
from src.generator import generate_and_save_data
from src.lakehouse import (
    initialize_lakehouse, ingest_to_bronze, process_to_silver, generate_gold_tables
)
from src.schema_evolution import evolve_silver_schema, process_batch_2_to_silver
from src.time_travel import get_snapshot_history, run_time_travel_query
from src.benchmark import run_benchmark
from src.reporter import generate_lakehouse_report

def print_banner(step_name):
    border = "=" * 60
    logger.info(f"\n{border}\n[RUNNING STAGE] {step_name}\n{border}")

def run_lakehouse_pipeline():
    logger.info("=" * 80)
    logger.info("STARTING WEEK 2 ICEBERG LAKEHOUSE PIPELINE SIMULATION")
    logger.info("=" * 80 + "\n")
    
    start_time = time.time()
    
    # Step 1: Mock data generation
    print_banner("1. Mock Data Generation")
    batch_1_raw, batch_2_raw = generate_and_save_data()
    logger.info(f"Generated raw data batches: Batch 1 ({batch_1_raw} rows), Batch 2 ({batch_2_raw} rows)")
    
    # Step 2: Initialize Iceberg schemas
    print_banner("2. Initialize Catalog Namespaces and Tables")
    catalog = initialize_lakehouse()
    
    # Step 3: Ingest Batch 1 to Bronze & Silver
    print_banner("3. Ingest Batch 1 (Bronze & Silver)")
    ingest_to_bronze(RAW_BATCH_1_PATH)
    total_raw_1, clean_cnt_1, dedup_rate_1 = process_to_silver()
    logger.info(f"Batch 1 Silver Load Complete: {clean_cnt_1} clean records loaded, {dedup_rate_1:.2f}% duplicate records quarantined.")
    
    # Step 4: Schema Evolution
    print_banner("4. Schema Evolution: Alter Silver Table")
    evolve_silver_schema()
    
    # Step 5: Ingest Batch 2 to Bronze & Silver (With evolved derived column)
    print_banner("5. Ingest Batch 2 (Bronze & Silver Evolved)")
    ingest_to_bronze(RAW_BATCH_2_PATH)
    total_raw_2, clean_cnt_2, dedup_rate_2 = process_batch_2_to_silver()
    logger.info(f"Batch 2 Silver Load Complete: {clean_cnt_2} clean records loaded with trip_duration_minutes, {dedup_rate_2:.2f}% duplicates quarantined.")
    
    # Step 6: Generate Gold tables
    print_banner("6. Generate Gold Aggregates")
    generate_gold_tables()
    
    # Step 7: Time Travel Query
    print_banner("7. Time Travel Verification")
    history = get_snapshot_history()
    # Snapshots[0] is Batch 1 loaded, Snapshots[1] is schema evolution transaction, Snapshots[2] is Batch 2 loaded
    first_snapshot_id = history[0]["snapshot_id"]
    historical_count, duckdb_count = run_time_travel_query(first_snapshot_id)
    
    # Step 8: Benchmarking partitioned vs unpartitioned
    print_banner("8. Run Gold Table Query Latency Benchmark")
    latency_unpart, latency_part, speedup = run_benchmark()
    
    # Step 9: HTML report generation
    print_banner("9. Compile HTML Lakehouse Summary Report")
    # Current count is the total silver clean records count (Batch 1 + Batch 2)
    t_silver = catalog.load_table("silver.taxi_trips")
    current_count = len(t_silver.scan().to_pandas())
    
    generate_lakehouse_report(
        batch_1_raw=batch_1_raw,
        batch_2_raw=batch_2_raw,
        silver_batch_1_clean=clean_cnt_1,
        silver_batch_2_clean=clean_cnt_2,
        dedup_rate_1=dedup_rate_1,
        dedup_rate_2=dedup_rate_2,
        latency_unpart=latency_unpart,
        latency_part=latency_part,
        speedup=speedup,
        historical_count=historical_count,
        current_count=current_count
    )
    
    total_duration = time.time() - start_time
    logger.info("=" * 80)
    logger.info(f"LAKEHOUSE PIPELINE SIMULATION COMPLETED SUCCESSFULLY in {total_duration:.2f}s")
    logger.info(f"HTML Summary Report is available at: {REPORT_PATH}")
    logger.info("=" * 80)

if __name__ == "__main__":
    run_lakehouse_pipeline()
