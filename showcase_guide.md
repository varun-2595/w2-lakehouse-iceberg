# Week 2 Showcase Guide: Local Simulation vs. Docker Production

This guide outlines how to execute and showcase both the **Local Simulation Mode** and the **Docker Production Mode** for your team or showcase evaluation.

---

## 💻 1. Local Simulation Mode (Offline Showcase)

This mode runs entirely on your local machine using a SQLite-backed SQL Catalog and local directory writes. It requires **no Docker containers** and is perfect for demonstrating core Iceberg mechanics offline.

### ⚙️ How it Works
- **Metadata Database**: `data/iceberg_catalog.db` (local SQLite file storing namespace and table pointers).
- **Physical Warehouse**: `data/warehouse/` (local directory storing metadata JSONs, manifest lists, manifest files, and Parquet data files).

### 🚀 Execution Steps
1. **Prepare Environment**:
   ```powershell
   # Install Python requirements
   pip install -r requirements.txt
   ```
2. **Execute the Pipeline**:
   ```powershell
   python run_pipeline_local.py
   ```
   *This generates raw mock parquets, creates schemas, deduplicates & masks PII, evolves the schema, runs time-travel verification, benchmarks query performance, and compiles the final report.*

### 🔍 How to Showcase the Outcomes

#### A. Interactive HTML Summary Report
Open the compiled report directly in your browser:
📂 **Path**: `data/reports/lakehouse_summary.html`
- **Showcase Points**:
  - **KPIs**: Highlight that **100% of duplicates** (all 250 in Batch 1, and 100 in Batch 2) were dropped.
  - **Deduplication Rate**: Shows `20.00%` duplicate ratio in raw data (250 duplicates dropped out of 1250 total raw records).
  - **Audit Logs**: Show the transaction histories.
  - **Time Travel & Benchmarks**: Review the query latency and historical count matching.

#### B. Direct File-Level Metadata Inspection (The Iceberg Directory Structure)
Show your team how Apache Iceberg manages state in the file system:
1. Open the folder `data/warehouse/silver/taxi_trips/`.
2. Point out the two main subdirectories:
   - `data/`: Contains partitioned or unpartitioned raw Parquet records containing the actual data.
   - `metadata/`: Contains `.metadata.json`, `.avro` manifests, and manifest lists. Show that every write or schema alteration created a new metadata JSON transaction file.

#### C. SQL Time-Travel Queries using DuckDB CLI/Python
Showcase how DuckDB queries the Iceberg table at a specific historical point in time. Open a python shell and execute:
```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL iceberg; LOAD iceberg; SET unsafe_enable_version_guessing = true;")

# Replace with the actual Snapshot ID listed in your console output during the run:
snapshot_id = 8992893070119261731

query = f"""
    SELECT COUNT(*) FROM iceberg_scan(
        'data/warehouse/silver/taxi_trips',
        snapshot_from_id = {snapshot_id}
    )
"""
count = conn.execute(query).fetchone()[0]
print(f"Historical Row Count: {count}")  # Output: 1000 rows (Batch 1 baseline clean count)
```

---

## 🐳 2. Docker Production Mode (Containerized Showcase)

This mode simulates a production-grade enterprise data platform where metadata is managed by a central catalog and raw data is stored in object storage.

### ⚙️ How it Works
- **Storage Layer**: **MinIO** (an S3-compatible object storage service). Data resides inside the bucket `s3a://warehouse/`.
- **Catalog Layer**: **Tabular Iceberg REST Catalog** (the modern standard for cataloging Iceberg tables).
- **Catalog Database**: **PostgreSQL** (stores metadata, namespaces, schemas, and table transaction pointers).

### 🚀 Execution Steps
1. **Spin Up the Containers**:
   ```bash
   docker-compose up -d
   ```
2. **Verify Services are Running**:
   ```bash
   docker compose ps
   ```
   *You should see postgres, minio, and rest-catalog containers in `running` status.*
3. **Execute Pipeline in Docker Mode**:
   Tell the python orchestrator to connect to the REST Catalog and MinIO by setting the environment flag:
   - **On Windows (PowerShell)**:
     ```powershell
     $env:RUNNING_IN_DOCKER="True"
     python run_pipeline_local.py
     ```
   - **On macOS/Linux (Bash)**:
     ```bash
     RUNNING_IN_DOCKER=True python run_pipeline_local.py
     ```
   - **On Windows (CMD)**:
     ```cmd
     set RUNNING_IN_DOCKER=True
     python run_pipeline_local.py
     ```

### 🔍 How to Showcase the Outcomes

#### A. MinIO Web GUI (Object Storage Visualizer)
1. Open your browser and navigate to the MinIO Console:
   🔗 **URL**: `http://localhost:9001`
   🔑 **Credentials**: Username `minioadmin` / Password `minioadmin`
2. Click on **Object Browser** $\rightarrow$ **warehouse**.
3. Browse the directories: `bronze/`, `silver/`, and `gold/`.
4. Drill down to `silver/taxi_trips/data/` to show the Parquet files stored directly on S3.

#### B. PostgreSQL Central Schema Visualizer
Show how transaction safety and table metadata pointers are kept in SQL:
1. Connect to PostgreSQL:
   - Host: `localhost`
   - Port: `5432`
   - Database: `demo_catalog`
   - User/Password: `admin` / `admin`
2. Show that PostgreSQL tracks namespaces and metadata file locations in the internal catalog schemas.

#### C. Multi-Engine Catalog Consistency
Explain the primary benefit of the REST Catalog:
- Because the table metadata is centrally managed by the REST catalog and PostgreSQL, multiple separate engines (like a python app using **PyIceberg**, a database like **DuckDB**, or a big data engine like **Apache Spark**) can all query the same tables concurrently with ACID guarantees, always seeing the latest committed transaction.
