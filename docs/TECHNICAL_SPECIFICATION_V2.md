# Technical Specification: Event Data Processing Pipeline

## 0. Introduction

This document provides a detailed technical specification for the Event Data Processing Pipeline. It outlines the system architecture, data flow, component design, and operational procedures for the end-to-end ETL process.

---

## 1. SERVICES OVERVIEW

The pipeline leverages a suite of AWS services and open-source technologies to create a scalable and automated data processing solution:

- **AWS S3 (Simple Storage Service)**: Used as the primary data lake for storing data across all layers: Landing, Normalized, Summarized, and Outbound.
- **AWS EMR (Elastic MapReduce)**: Provides the managed Spark environment for executing data transformation and processing jobs at scale.
- **Apache Airflow**: Acts as the orchestration engine, scheduling, executing, and monitoring the entire ETL workflow.
- **Apache Spark**: The core processing framework used for all data manipulation, aggregation, and analysis tasks, written in Python (PySpark).
- **AWS Glue & Athena**: (Assumed) The Parquet data stored in S3 can be cataloged by AWS Glue, making it available for ad-hoc querying and analysis via Amazon Athena.

---

## 2. DETAILED DESIGN SPECIFICATIONS

### 2.1 SYSTEM ARCHITECTURE

The pipeline is designed with a multi-layered architecture to ensure data quality, modularity, and maintainability.

```
┌─────────────┐    ┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│   Landing   │───▶│ Normalized  │───▶│ Summarized   │───▶│  Outbound   │
│   Layer     │    │   Layer     │    │   Layer      │    │   Layer     │
└─────────────┘    └─────────────┘    └──────────────┘    └─────────────┘
```

### 2.1.1 Workflow Steps

#### 2.1.1.1 Steps For Transformation Of Data From Inbound to Summarized

1.  **Inbound Preprocessing**: Raw data files are initially processed to ensure basic schema conformity and data integrity.
2.  **Landing**: The preprocessed files are ingested into the Landing Layer, with each core entity (events, participants, etc.) handled by a dedicated script.
3.  **Normalization**: Landing data is transformed into a structured, relational model. This involves joining datasets, enriching data from master files, and standardizing identifiers.
4.  **Summarization**: The normalized data is aggregated to create Key Performance Indicators (KPIs) and other analytical metrics.
5.  **Outbound Generation**: The latest normalized data is filtered and exported into CSV files for consumption by downstream systems.

#### 2.1.1.2 Steps For Airflow Creation And Layer Wise Logic

The Airflow DAG orchestrates the entire process by submitting jobs to an EMR cluster in a specific sequence:

1.  **EMR Cluster Creation**: The DAG begins by spinning up a new EMR cluster.
2.  **Landing Jobs**: A series of Spark jobs are run in sequence to process inbound files into the Landing Layer.
3.  **Normalization Jobs**: Once Landing is complete, Normalization jobs run to process `events` and `participants`.
4.  **Summarization & Outbound Jobs**: After Normalization, the `generate_kpis` and `export_latest_data` jobs run.
5.  **EMR Cluster Termination**: The DAG terminates the EMR cluster after all steps are complete (or if a step fails).

#### 2.1.1.3 AirFlow Execution Tracking

- **Airflow UI**: The primary tool for monitoring DAG runs, viewing logs, and tracking task status.
- **EMR Logs**: All Spark job logs are persisted to a configured S3 bucket for detailed debugging.
- **Email Notifications**: The DAG is configured to send email alerts upon successful completion or failure of the pipeline.

#### 2.1.1.4 Python scripts Name and DAG Name

- **DAG Name**: `airflow_events_daily`
- **Python Scripts**:
    - `src/Preprocess/inbound_preprocess_lm_onboarding.py`
    - `src/Landing/preprocess_lnd_*.py` (multiple files)
    - `src/Normalized/process_events.py`
    - `src/Normalized/process_participants.py`
    - `src/Summarized/generate_kpis.py`
    - `src/Outbound/export_latest_data.py`

#### 2.1.1.5 Athena Table Names and Attributes

*(Based on S3 data structure and AWS Glue crawler configuration)*

- **`normalized_lm_events`**
    - `event_id`, `event_name`, `event_date`, `brand_id`, `brand_name`, `indication_id`, `oasis_modified_date`
- **`normalized_lm_participants`**
    - `participant_id`, `event_id`, `mdm_id`, `final_mdm_id`, `participant_type`, `attended`, `modified_date`
- **`summarized_event_performance_kpi`**
    - `event_date`, `total_events`, `total_registrations`, `total_attendees`, `attendance_rate`
- **`summarized_product_performance_kpi`**
    - `brand_id`, `brand_name`, `event_count`, `total_registrations`, `total_attendees`

### 2.2 Layers Component Deep Dive

*(This section provides a detailed breakdown of the logic within each layer's scripts, as previously generated in `TECHNICAL_SPECIFICATION_DETAILED.md`)*

### 2.3 Data Flow, Data Models & Schemas

*(This section includes the data flow diagram and detailed JSON schemas for the core data models like Events and Participants, as previously generated)*

### 2.4 DOWNSTREAM SYSTEM AND EXTERNAL VENDORS

The primary downstream system consumes the pipe-delimited CSV files generated by the `export_latest_data.py` script. These files are deposited in the `s3://dev-lmhc-datasource/common/outbound/` S3 path.

### 2.5 SCHEDULING

The `airflow_events_daily` DAG is configured to run automatically on a daily schedule.
- **Schedule Interval**: `0 1 * * *` (Daily at 1:00 AM UTC).
