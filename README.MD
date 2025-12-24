# 🌐 Wikipedia Real-Time Analytics Pipeline

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-E25A1C?style=flat&logo=apache-spark)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6-231F20?style=flat&logo=apache-kafka)](https://kafka.apache.org/)
[![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1.5.2-00C7D4?style=flat&logo=apache)](https://iceberg.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?style=flat&logo=apache-airflow)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)

> A production-grade, real-time streaming analytics platform that ingests Wikipedia edit events, processes them through a medallion architecture (Bronze → Silver → Gold), and visualizes insights through Apache Superset dashboards.

![Architecture](https://img.shields.io/badge/Architecture-Lakehouse-blueviolet)
![Status](https://img.shields.io/badge/Status-Production%20Ready-success)

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Features](#-features)
- [Tech Stack](#-tech-stack)
- [Getting Started](#-getting-started)
- [Project Structure](#-project-structure)
- [Data Pipeline](#-data-pipeline)
- [Monitoring & Maintenance](#-monitoring--maintenance)
- [Dashboards](#-dashboards)
- [Performance Tuning](#-performance-tuning)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)
- [License](#-license)

---

## 🎯 Overview

This project demonstrates a **modern data lakehouse architecture** for real-time analytics. It streams live Wikipedia edit events, processes them through multiple transformation layers, and provides actionable insights through interactive dashboards.

### Key Highlights

- **Real-time Processing**: Sub-minute latency from edit event to dashboard
- **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (aggregated)
- **Scalable Design**: Handles millions of events per day
- **ACID Transactions**: Powered by Apache Iceberg for data reliability
- **Automated Maintenance**: Airflow DAGs for table optimization and late-data handling
- **Rich Analytics**: 10+ KPIs tracking community engagement, bot activity, and content growth

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         WIKIPEDIA EVENTS API                         │
│                  (stream.wikimedia.org/v2/stream)                   │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │   KAFKA PRODUCER       │
                    │   (Python SSEClient)   │
                    └───────────┬────────────┘
                                │
                                ▼
                    ┌────────────────────────┐
                    │    APACHE KAFKA        │
                    │  Topic: wiki.raw.events│
                    └───────────┬────────────┘
                                │
                                ▼
┌───────────────────────────────────────────────────────────────────────┐
│                       SPARK STRUCTURED STREAMING                      │
├───────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐         │
│  │   BRONZE    │      │   SILVER    │      │    GOLD     │         │
│  ├─────────────┤      ├─────────────┤      ├─────────────┤         │
│  │ Raw JSON    │ ───▶ │ Parsed &    │ ───▶ │ Aggregated  │         │
│  │ + Metadata  │      │ Cleaned     │      │ KPIs (5min) │         │
│  │             │      │ + Features  │      │ + Metrics   │         │
│  └─────────────┘      └─────────────┘      └─────────────┘         │
│        │                     │                     │                │
└────────┼─────────────────────┼─────────────────────┼────────────────┘
         │                     │                     │
         ▼                     ▼                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      APACHE ICEBERG TABLES                           │
│                         (HDFS Storage)                               │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │   APACHE SUPERSET      │
                    │   (Dashboards & BI)    │
                    └────────────────────────┘

                    ┌────────────────────────┐
                    │   APACHE AIRFLOW       │
                    │   (Orchestration)      │
                    │                        │
                    │ - Maintenance DAG      │
                    │ - Late Data DAG        │
                    │ - Producer DAG         │
                    └────────────────────────┘
```

---

## ✨ Features

### Real-Time Data Processing
- **Live ingestion** from Wikipedia Recent Changes API
- **Stream processing** with Spark Structured Streaming
- **Watermarking** for late-data handling (10-minute tolerance)
- **Exactly-once semantics** with Kafka + Iceberg integration

### Data Quality & Reliability
- **Schema validation** and data type enforcement
- **Deduplication** using event IDs
- **ACID transactions** via Apache Iceberg
- **Time-travel queries** and snapshot management

### Advanced Analytics
- **10+ KPIs** including edit velocity, bot percentage, community engagement
- **5-minute windowed aggregations** for real-time trends
- **Derived metrics** like knowledge trend (GROWING/SHRINKING/STABLE)
- **Anomaly detection ready** with volatility scores

### Automated Operations
- **Snapshot expiration** (retain last 5-12 snapshots)
- **Orphan file cleanup** (2-3 hour retention)
- **Data file compaction** (optimized file sizes)
- **Manifest consolidation** for query performance

---

## 🛠️ Tech Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Streaming** | Apache Kafka | 3.6 | Event ingestion & buffering |
| **Processing** | Apache Spark | 3.5.0 | Distributed stream processing |
| **Storage Format** | Apache Iceberg | 1.5.2 | Table format with ACID guarantees |
| **Storage Layer** | HDFS | 3.3.6 | Distributed file system |
| **Metastore** | Hive Metastore | 3.1.3 | Metadata management |
| **Orchestration** | Apache Airflow | 2.8 | Workflow scheduling |
| **Visualization** | Apache Superset | 3.0 | Dashboards & reporting |
| **Language** | Python | 3.9+ | Application development |

---

## 🚀 Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.9+
- 16GB+ RAM recommended
- 50GB+ disk space

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/wikipedia-realtime-analytics.git
cd wikipedia-realtime-analytics
```

2. **Set up environment variables**
```bash
cp .env.example .env
# Edit .env with your configurations
```

3. **Start the infrastructure**
```bash
docker-compose up -d
```

4. **Install Python dependencies**
```bash
pip install -r requirements.txt
```

5. **Initialize the database schema**
```bash
python scripts/init_schema.py
```

### Quick Start

**Step 1: Start Kafka Producer**
```bash
# Via Airflow UI (http://localhost:8080)
# Trigger DAG: wiki_kafka_producer

# Or manually:
python kafka/ingestion.py
```

**Step 2: Start Streaming Pipeline**
```bash
# Via Airflow UI
# Trigger DAG: wiki_streaming_pipeline

# Or manually:
python streaming/main.py
```

**Step 3: Access Dashboards**
```bash
# Superset: http://localhost:8088
# Username: admin
# Password: admin
```

---

## 📁 Project Structure

```
wikipedia-realtime-analytics/
│
├── kafka/
│   ├── ingestion.py              # Wikipedia SSE → Kafka producer
│   └── config.py                 # Kafka configuration
│
├── streaming/
│   ├── main.py                   # Streaming pipeline entry point
│   ├── Bronze.py                 # Raw data ingestion
│   ├── Silver.py                 # Data cleaning & enrichment
│   └── Gold.py                   # Aggregations & KPIs
│
├── iceberg/
│   ├── iceberg_maintenance.py    # Table optimization tasks
│   └── late_records.py           # Late-data reprocessing
│
├── airflow/
│   └── dags/
│       ├── wiki_kafka_producer_dag.py
│       ├── wiki_streaming_pipeline_dag.py
│       ├── wiki_iceberg_maintenance_dag.py
│       └── wiki_late_data_correction_dag.py
│
├── superset/
│   └── dashboards/               # Dashboard exports
│
├── scripts/
│   ├── init_schema.py            # Initialize Iceberg tables
│   └── reset_pipeline.py         # Reset checkpoints & data
│
├── docker-compose.yml            # Infrastructure setup
├── requirements.txt              # Python dependencies
├── .env.example                  # Environment variables template
└── README.md                     # This file
```

---

## 🔄 Data Pipeline

### Bronze Layer (Raw)
**Purpose**: Ingest raw JSON events with minimal transformation

**Schema**:
```python
- event_key: string          # Unique event identifier
- raw_json: string           # Original JSON payload
- topic: string              # Kafka topic name
- partition: int             # Kafka partition
- offset: long               # Kafka offset
- kafka_timestamp: timestamp # Event timestamp from Kafka
- ingestion_time: timestamp  # Processing timestamp
```

**Output**: `wiki.bronze_wiki_events`

---

### Silver Layer (Cleaned)
**Purpose**: Parse JSON, validate schema, add derived features

**Transformations**:
- Parse JSON structure
- Extract nested fields (meta, user, length)
- Calculate `bytes_diff` = new_length - old_length
- Compute `processing_delay_seconds`
- Classify `edit_size` (LARGE / SMALL / LARGE_REMOVAL)
- Categorize `user_type` (BOT / REGISTERED / ANONYMOUS)
- Deduplicate by `event_id`

**Schema**:
```python
- event_id: string
- event_time: timestamp
- page_title: string
- namespace: int
- change_type: string
- user_id: long
- is_bot: boolean
- bytes_diff: int
- processing_delay_seconds: long
- edit_size: string
- user_type: string
```

**Output**: `wiki.silver_wiki_events`

---

### Gold Layer (Aggregated)
**Purpose**: Compute business KPIs with 5-minute windows

**KPIs Computed**:
1. **Platform Activity**: Total edits per window
2. **Community Engagement**: Unique editors count
3. **Page Activity**: Active pages count
4. **Automation Volume**: Bot edits count & percentage
5. **Human Contribution**: Non-bot edits count
6. **Knowledge Growth**: Net bytes added/removed
7. **Edit Intensity**: Average bytes per edit, edits per minute
8. **Content Changes**: Large edits (>500 bytes)
9. **Revert Behavior**: Large removals (<-500 bytes)
10. **Volatility**: Total absolute bytes changed

**Schema**:
```python
- window_start: timestamp
- window_end: timestamp
- total_edits: long
- unique_editors: long
- active_pages: long
- bot_edits: long
- human_edits: long
- net_bytes_added: long
- avg_bytes_per_edit: double
- large_edits_count: long
- revert_like_edits: long
- volatility_score: long
- bot_edit_percentage: double
- edits_per_minute: double
- knowledge_trend: string  # GROWING / SHRINKING / STABLE
```

**Output**: `wiki.gold_wiki_kpis`

---

## 🔧 Monitoring & Maintenance

### Airflow DAGs

#### 1. `wiki_iceberg_maintenance` (Every 2 hours)
Optimizes Iceberg tables for query performance:
- **Expire old snapshots** (retain last 5-12)
- **Remove orphan files** (2-3 hour retention)
- **Compact small files** (target 128-512 MB)
- **Rewrite manifests** for faster planning

#### 2. `wiki_late_data_correction` (Manual trigger)
Handles late-arriving data:
- Reprocesses Bronze data from last 24 hours
- Merges updates into Silver using event_id
- Recomputes Gold aggregations

#### 3. `wiki_kafka_producer` (Manual trigger)
Starts the Wikipedia event producer

#### 4. `wiki_streaming_pipeline` (Manual trigger)
Launches the Spark streaming application

---

## 📊 Dashboards

### 1. Real-Time Wiki Events Monitor (Silver)

**Charts**:
- **Events Over Time**: Line chart showing edit velocity
- **User Type Distribution**: Pie chart (BOT / REGISTERED / ANONYMOUS)
- **Edit Size Breakdown**: Bar chart of edit magnitudes
- **Top Active Pages**: Table with edit counts
- **Recent Events Stream**: Real-time event log

**Filters**: Time range, user type, edit size, change type

---

### 2. Wikipedia KPI Analytics (Gold)

**Charts**:
- **Big Numbers**: Total edits, unique editors, active pages, knowledge growth
- **Platform Activity Trend**: Stacked area chart (bot vs human edits)
- **Bot vs Human Activity**: Mixed chart with percentage overlay
- **Edit Intensity Gauge**: Real-time edits/minute
- **Knowledge Trend**: Pie chart (GROWING / SHRINKING / STABLE)
- **Volatility Timeline**: Line chart of content churn
- **Community Engagement**: Table with all KPIs

**Auto-refresh**: Every 30 seconds

---

## ⚡ Performance Tuning

### Spark Configuration
```python
.config("spark.sql.shuffle.partitions", "200")
.config("spark.streaming.backpressure.enabled", "true")
.config("spark.sql.adaptive.enabled", "true")
```

### Iceberg Optimizations
- **Fanout writes**: Enabled for Gold table
- **File size targets**: 128-512 MB per file
- **Snapshot retention**: 5-12 snapshots
- **Manifest caching**: Automatic

### Kafka Tuning
- **Batch size**: 16384 bytes
- **Linger time**: 10 ms
- **Compression**: snappy
- **Replication factor**: 3

---

## 🐛 Troubleshooting

### Issue: No records in Gold table
**Cause**: Watermark too aggressive, windows not closing

**Solution**:
```python
# Reduce watermark in Gold.py
.withWatermark("event_time", "10 minutes")  # instead of 24 hours
```

---

### Issue: Duplicate records in aggregations
**Cause**: Checkpoint corruption or clock skew

**Solution**:
```bash
# Reset pipeline
python scripts/reset_pipeline.py

# Or manually:
hdfs dfs -rm -r /user/hive/checkpoints/wiki/gold
spark-sql -e "TRUNCATE TABLE wiki.gold_wiki_kpis"
```

---

### Issue: Superset can't see tables
**Cause**: Metastore cache or connection issue

**Solution**:
1. In Superset: Settings → Database → Refresh Metadata
2. Use SQL Lab to create virtual datasets:
```sql
SELECT * FROM wiki.silver_wiki_events WHERE event_time >= NOW() - INTERVAL '7' DAY
```
Save as dataset

---


## 🙏 Acknowledgments

- **Wikipedia** for providing the real-time event stream
- **Apache Software Foundation** for the amazing open-source tools
- **Community contributors** for inspiration and best practices

---

## 📧 Contact

SWARAJ KADU - www.linkedin.com/in/swaraj-kadu-b263a7254

---

<div align="center">

**⭐ Star this repo if you find it useful! ⭐**

Made with ❤️ and ☕ by SWARAJ

</div>