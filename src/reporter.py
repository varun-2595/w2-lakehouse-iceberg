import logging
import os
import pandas as pd
from src.config import REPORT_PATH, TABLE_SILVER_TRIPS, TABLE_BRONZE_TRIPS
from src.lakehouse import get_catalog

logger = logging.getLogger(__name__)

def generate_lakehouse_report(
    batch_1_raw, batch_2_raw, 
    silver_batch_1_clean, silver_batch_2_clean,
    dedup_rate_1, dedup_rate_2,
    latency_unpart, latency_part, speedup,
    historical_count, current_count
):
    """
    Queries Iceberg tables and compiles a beautiful HTML report demonstrating
    Medallion layers, deduplication, schema evolution, time travel, and query benchmarking.
    """
    logger.info("Compiling Lakehouse HTML summary report...")
    catalog = get_catalog()
    t_bronze = catalog.load_table(TABLE_BRONZE_TRIPS)
    t_silver = catalog.load_table(TABLE_SILVER_TRIPS)
    
    # 1. Fetch data samples for visualization
    df_bronze_sample = t_bronze.scan().to_pandas().head(5)
    df_silver_sample = t_silver.scan().to_pandas()
    
    # Separate Batch 1 and Batch 2 samples to illustrate schema evolution
    # Batch 1 should have trip_duration_minutes as NaN/None
    # Batch 2 should have valid trip_duration_minutes float values
    batch_1_sample = df_silver_sample[df_silver_sample["source_file"] == "yellow_taxi_batch_1.parquet"].head(3).to_dict(orient="records")
    batch_2_sample = df_silver_sample[df_silver_sample["source_file"] == "yellow_taxi_batch_2.parquet"].head(3).to_dict(orient="records")
    
    # Fetch snapshots details
    snapshots = list(t_silver.snapshots())
    snap_details = []
    for snap in snapshots:
        snap_details.append({
            "id": snap.snapshot_id,
            "timestamp": pd.to_datetime(snap.timestamp_ms, unit="ms").strftime("%Y-%m-%d %H:%M:%S"),
            "operation": snap.summary.operation
        })

    # 2. Compile HTML content
    html_content = get_html_template(
        batch_1_raw=batch_1_raw,
        batch_2_raw=batch_2_raw,
        silver_1_clean=silver_batch_1_clean,
        silver_2_clean=silver_batch_2_clean,
        dedup_rate_1=dedup_rate_1,
        dedup_rate_2=dedup_rate_2,
        latency_unpart=latency_unpart,
        latency_part=latency_part,
        speedup=speedup,
        historical_count=historical_count,
        current_count=current_count,
        snapshots=snap_details,
        batch_1_sample=batch_1_sample,
        batch_2_sample=batch_2_sample,
        bronze_total=len(df_bronze_sample)
    )
    
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(html_content)
        
    logger.info(f"Lakehouse report generated at: {REPORT_PATH}")
    return REPORT_PATH

def get_html_template(**kwargs):
    # Snapshot rows
    snap_rows = ""
    for idx, s in enumerate(kwargs["snapshots"]):
        snap_rows += f"""
        <tr>
            <td class="font-mono text-white text-xs">{idx+1}</td>
            <td class="font-mono text-accent-cyan text-xs">{s['id']}</td>
            <td class="font-mono text-xs">{s['timestamp']}</td>
            <td><span class="badge status-success">{s['operation']}</span></td>
        </tr>
        """
        
    # Batch 1 Schema Evolution Sample (Pre-evolution)
    b1_rows = ""
    for r in kwargs["batch_1_sample"]:
        # Handle nan/none check for trip_duration_minutes
        dur_val = r.get("trip_duration_minutes", None)
        dur_str = f"£{dur_val:.2f}" if dur_val is not None and pd.notna(dur_val) else '<span class="text-accent-red font-semibold">NULL</span>'
        
        b1_rows += f"""
        <tr>
            <td class="font-mono text-xs">{r['trip_fingerprint'][:10]}...</td>
            <td class="font-medium text-white">{r['driver_name']}</td>
            <td class="font-mono text-xs">{str(r['tpep_pickup_datetime'])[:19]}</td>
            <td class="font-mono text-xs">{r['store_and_fwd_flag']}</td>
            <td class="text-right font-mono">{dur_str}</td>
        </tr>
        """
    if not b1_rows:
        b1_rows = "<tr><td colspan='5' class='text-center text-gray-500'>No Batch 1 sample data</td></tr>"

    # Batch 2 Schema Evolution Sample (Post-evolution)
    b2_rows = ""
    for r in kwargs["batch_2_sample"]:
        dur_val = r.get("trip_duration_minutes", None)
        dur_str = f'<span class="text-success font-semibold font-mono">{dur_val:.2f} mins</span>' if dur_val is not None and pd.notna(dur_val) else '<span class="text-accent-red font-semibold">NULL</span>'
        
        b2_rows += f"""
        <tr>
            <td class="font-mono text-xs">{r['trip_fingerprint'][:10]}...</td>
            <td class="font-medium text-white">{r['driver_name']}</td>
            <td class="font-mono text-xs">{str(r['tpep_pickup_datetime'])[:19]}</td>
            <td class="font-mono text-xs">{r['store_and_fwd_flag']}</td>
            <td class="text-right font-mono">{dur_str}</td>
        </tr>
        """
    if not b2_rows:
        b2_rows = "<tr><td colspan='5' class='text-center text-gray-500'>No Batch 2 evolved sample data</td></tr>"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Apache Iceberg Medallion Lakehouse Summary (W2)</title>
    <!-- Google Fonts -->
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=Space+Grotesk:wght@400;500;600;700&display=swap" rel="stylesheet">
    
    <style>
        :root {{
            --bg-dark: #07080d;
            --panel-bg: rgba(14, 16, 27, 0.7);
            --border-color: rgba(255, 255, 255, 0.08);
            
            --accent-primary: #3b82f6; /* Blue */
            --accent-purple: #8b5cf6;
            --accent-cyan: #06b6d4;
            --accent-red: #f43f5e;
            --accent-green: #10b981;
            
            --text-primary: #f3f4f6;
            --text-secondary: #9ca3af;
        }}

        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            background-color: var(--bg-dark);
            background-image: 
                radial-gradient(at 0% 0%, rgba(59, 130, 246, 0.1) 0px, transparent 50%),
                radial-gradient(at 100% 100%, rgba(139, 92, 246, 0.1) 0px, transparent 50%);
            color: var(--text-primary);
            font-family: 'Outfit', sans-serif;
            min-height: 100vh;
            padding: 2rem 1.5rem;
            line-height: 1.5;
        }}

        h1, h2, h3, h4 {{
            font-family: 'Space Grotesk', sans-serif;
            font-weight: 700;
        }}

        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}

        /* Header Styling */
        header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2.5rem;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 1.5rem;
        }}

        .title-area h1 {{
            font-size: 2.2rem;
            background: linear-gradient(135deg, #fff 0%, #3b82f6 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            letter-spacing: -0.5px;
        }}

        .title-area p {{
            color: var(--text-secondary);
            font-size: 0.95rem;
            margin-top: 0.25rem;
        }}

        .meta-badge {{
            background: rgba(59, 130, 246, 0.1);
            border: 1px solid rgba(59, 130, 246, 0.2);
            border-radius: 99px;
            padding: 0.5rem 1.2rem;
            display: flex;
            flex-direction: column;
            align-items: flex-end;
        }}

        .meta-badge .engine-tag {{
            font-family: monospace;
            font-size: 0.8rem;
            color: var(--accent-cyan);
            font-weight: bold;
        }}

        .meta-badge .platform {{
            font-size: 0.9rem;
            font-weight: 600;
        }}

        /* Grid Layouts */
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2.5rem;
        }}

        .dashboard-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 2rem;
            margin-bottom: 2.5rem;
        }}

        .full-width {{
            grid-column: 1 / -1;
        }}

        @media (max-width: 900px) {{
            .dashboard-grid {{
                grid-template-columns: 1fr;
            }}
        }}

        /* Glassmorphic Panel */
        .panel {{
            background: var(--panel-bg);
            backdrop-filter: blur(16px);
            -webkit-backdrop-filter: blur(16px);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 1.5rem;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
        }}

        .panel-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1.25rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            padding-bottom: 0.75rem;
        }}

        .panel-header h2 {{
            font-size: 1.25rem;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}

        .panel-header h2::before {{
            content: '';
            display: inline-block;
            width: 4px;
            height: 18px;
            background: var(--accent-primary);
            border-radius: 2px;
        }}

        /* Stat Cards */
        .stat-card {{
            position: relative;
            overflow: hidden;
        }}

        .stat-card::after {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 4px;
        }}

        .stat-card.bronze::after {{ background: var(--accent-cyan); }}
        .stat-card.silver::after {{ background: var(--accent-purple); }}
        .stat-card.benchmark::after {{ background: var(--accent-green); }}

        .stat-label {{
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: var(--text-secondary);
            font-weight: 500;
        }}

        .stat-value {{
            font-size: 2.5rem;
            font-weight: 700;
            font-family: 'Space Grotesk', sans-serif;
            line-height: 1.2;
            margin: 0.25rem 0;
        }}

        .stat-details {{
            font-size: 0.8rem;
            color: var(--text-secondary);
            display: flex;
            justify-content: space-between;
            margin-top: 0.5rem;
            border-top: 1px solid rgba(255, 255, 255, 0.03);
            padding-top: 0.5rem;
        }}

        /* Table Styling */
        table {{
            width: 100%;
            border-collapse: collapse;
            text-align: left;
            font-size: 0.9rem;
        }}

        th {{
            font-family: 'Space Grotesk', sans-serif;
            color: var(--text-secondary);
            font-weight: 600;
            padding: 0.75rem 1rem;
            border-bottom: 2px solid rgba(255, 255, 255, 0.05);
            text-transform: uppercase;
            font-size: 0.75rem;
            letter-spacing: 0.5px;
        }}

        td {{
            padding: 0.85rem 1rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.03);
            color: var(--text-secondary);
        }}

        tr:hover td {{
            color: var(--text-primary);
            background: rgba(255, 255, 255, 0.01);
        }}

        /* Helpers & Badges */
        .text-right {{ text-align: right; }}
        .text-center {{ text-align: center; }}
        .font-mono {{ font-family: monospace; }}
        .font-medium {{ font-weight: 500; }}
        .text-white {{ color: #fff; }}
        .text-accent-cyan {{ color: var(--accent-cyan); }}
        .text-accent-purple {{ color: var(--accent-purple); }}
        .text-accent-red {{ color: var(--accent-red); }}
        .text-success {{ color: var(--accent-green); }}

        .badge {{
            display: inline-block;
            padding: 0.25rem 0.6rem;
            border-radius: 6px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        .status-success {{
            background: rgba(16, 185, 129, 0.1);
            color: var(--accent-green);
            border: 1px solid rgba(16, 185, 129, 0.2);
        }}

        .latency-value {{
            font-size: 1.4rem;
            font-weight: 700;
            font-family: 'Space Grotesk', sans-serif;
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <header>
            <div class="title-area">
                <h1>Apache Iceberg Medallion Lakehouse</h1>
                <p>Cloud Data Lakehouse Architecture - Week 2 Performance Improvement Plan</p>
            </div>
            <div class="meta-badge">
                <span class="platform">PyIceberg + DuckDB</span>
                <span class="engine-tag">REST / SQL Catalog</span>
            </div>
        </header>

        <!-- KPI Cards -->
        <section class="metrics-grid">
            <!-- Bronze Card -->
            <div class="panel stat-card bronze">
                <div class="stat-label">Bronze Ingested</div>
                <div class="stat-value text-accent-cyan">{kwargs['batch_1_raw'] + kwargs['batch_2_raw']}</div>
                <div class="stat-details">
                    <span>Batch 1: {kwargs['batch_1_raw']}</span>
                    <span>Batch 2: {kwargs['batch_2_raw']}</span>
                </div>
            </div>

            <!-- Silver Card -->
            <div class="panel stat-card silver">
                <div class="stat-label">Silver Cleaned (Deduplicated)</div>
                <div class="stat-value text-accent-purple">{kwargs['silver_1_clean'] + kwargs['silver_2_clean']}</div>
                <div class="stat-details">
                    <span>Batch 1: {kwargs['silver_1_clean']} ({kwargs['dedup_rate_1']:.1f}% Dedup)</span>
                    <span>Batch 2: {kwargs['silver_2_clean']} ({kwargs['dedup_rate_2']:.1f}% Dedup)</span>
                </div>
            </div>

            <!-- Benchmark Card -->
            <div class="panel stat-card benchmark">
                <div class="stat-label">Benchmarking Speedup</div>
                <div class="stat-value text-success">{kwargs['speedup']:.2f}%</div>
                <div class="stat-details">
                    <span>Before: {kwargs['latency_unpart']:.2f} ms</span>
                    <span>After: {kwargs['latency_part']:.2f} ms</span>
                </div>
            </div>
        </section>

        <!-- Mid section: Schema Evolution and Time Travel -->
        <section class="dashboard-grid">
            <!-- Left panel: Schema Evolution proof -->
            <div class="panel">
                <div class="panel-header">
                    <h2>Schema Evolution Verification</h2>
                </div>
                <p style="color: var(--text-secondary); margin-bottom: 1rem; font-size: 0.9rem;">
                    We altered the Silver table schema to add the derived column <code>trip_duration_minutes</code>. 
                    Below are samples showing that older records have the column populated as <b>NULL</b>, while new Batch 2 records contain computed durations.
                </p>
                
                <h3 style="font-size: 0.95rem; margin-bottom: 0.5rem; color: var(--text-secondary);">Batch 1 Records (Pre-evolution)</h3>
                <table style="margin-bottom: 1.5rem;">
                    <thead>
                        <tr>
                            <th>Trip Hash</th>
                            <th>Driver Initials</th>
                            <th>Pickup Datetime</th>
                            <th>Normal Flag</th>
                            <th class="text-right">trip_duration_minutes</th>
                        </tr>
                    </thead>
                    <tbody>
                        {b1_rows}
                    </tbody>
                </table>

                <h3 style="font-size: 0.95rem; margin-bottom: 0.5rem; color: var(--text-secondary);">Batch 2 Records (Post-evolution)</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Trip Hash</th>
                            <th>Driver Initials</th>
                            <th>Pickup Datetime</th>
                            <th>Normal Flag</th>
                            <th class="text-right">trip_duration_minutes</th>
                        </tr>
                    </thead>
                    <tbody>
                        {b2_rows}
                    </tbody>
                </table>
            </div>

            <!-- Right panel: Time Travel & Snapshot Log -->
            <div class="panel">
                <div class="panel-header">
                    <h2>Iceberg Time Travel Verification</h2>
                </div>
                
                <!-- Time Travel Results Box -->
                <div style="background: rgba(255, 255, 255, 0.02); border: 1px solid rgba(255, 255, 255, 0.05); border-radius: 8px; padding: 1rem; margin-bottom: 1.5rem; display: flex; justify-content: space-around; text-align: center;">
                    <div>
                        <div style="font-size: 0.75rem; text-transform: uppercase; color: var(--text-secondary);">Historical Count (T-24h / Snapshot 1)</div>
                        <div class="latency-value text-accent-cyan" style="margin-top: 0.25rem;">{kwargs['historical_count']} rows</div>
                        <div style="font-size: 0.7rem; color: var(--text-secondary);">Batch 1 Cleaned Records</div>
                    </div>
                    <div style="border-left: 1px solid rgba(255,255,255,0.08);"></div>
                    <div>
                        <div style="font-size: 0.75rem; text-transform: uppercase; color: var(--text-secondary);">Current Count (Current Snapshot)</div>
                        <div class="latency-value text-accent-purple" style="margin-top: 0.25rem;">{kwargs['current_count']} rows</div>
                        <div style="font-size: 0.7rem; color: var(--text-secondary);">Batch 1 + Batch 2 Cleaned</div>
                    </div>
                </div>

                <div class="panel-header" style="border-bottom: none; margin-bottom: 0.5rem; padding-bottom: 0;">
                    <h3 style="font-size: 1.05rem; font-weight: 600;">Silver Table Snapshot History</h3>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>Snapshot ID</th>
                            <th>Commit Timestamp</th>
                            <th>Operation</th>
                        </tr>
                    </thead>
                    <tbody>
                        {snap_rows}
                    </tbody>
                </table>
            </div>
        </section>

        <!-- Lower Section: Query Benchmarking details -->
        <section class="dashboard-grid full-width">
            <div class="panel">
                <div class="panel-header">
                    <h2>Gold Table Benchmarking Latencies</h2>
                </div>
                <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 2rem; text-align: center; align-items: center; padding: 1rem 0;">
                    <div>
                        <h3 style="font-size: 1rem; color: var(--text-secondary); margin-bottom: 0.5rem;">Unpartitioned Query Latency</h3>
                        <div class="stat-value text-accent-red">{kwargs['latency_unpart']:.4f} ms</div>
                        <p style="font-size: 0.75rem; color: var(--text-secondary); margin-top: 0.25rem;">DuckDB scan of flat table folder</p>
                    </div>
                    <div style="border-left: 1px solid rgba(255,255,255,0.08); border-right: 1px solid rgba(255,255,255,0.08); height: 100%;">
                        <h3 style="font-size: 1rem; color: var(--text-secondary); margin-bottom: 0.5rem;">Partitioned Query Latency</h3>
                        <div class="stat-value text-success">{kwargs['latency_part']:.4f} ms</div>
                        <p style="font-size: 0.75rem; color: var(--text-secondary); margin-top: 0.25rem;">DuckDB scan of PULocationID partitions</p>
                    </div>
                    <div>
                        <h3 style="font-size: 1rem; color: var(--text-secondary); margin-bottom: 0.5rem;">Performance Improvement</h3>
                        <div class="stat-value text-white" style="background: linear-gradient(135deg, #10b981 0%, #3b82f6 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">{kwargs['speedup']:.2f}% Faster</div>
                        <p style="font-size: 0.75rem; color: var(--text-secondary); margin-top: 0.25rem;">Calculated speedup ratio</p>
                    </div>
                </div>
            </div>
        </section>
    </div>
</body>
</html>
"""
    return html
