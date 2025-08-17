# Event Data Processing Pipeline

A scalable data processing solution for managing event and participant data, built with Apache Spark and orchestrated using Apache Airflow.

## Project Structure

```
POC/
├── airflow_dags/                # Airflow DAG definitions
│   └── airflow_events_daily.py  # Main DAG for daily processing
├── src/
│   ├── Export/                  # Data export functionality
│   ├── Landing/                 # Data landing zone processors
│   ├── Normalized/              # Data normalization scripts
│   ├── Preprocess/              # Data preprocessing scripts
│   └── Summarized/              # KPI generation scripts
└── tests/                       # Test cases
```

## Key Features

- **Automated Data Processing**: End-to-end pipeline orchestrated with Airflow
- **Scalable Architecture**: Built on Apache Spark for handling large datasets
- **Comprehensive KPIs**: Multiple performance metrics and analytics
- **Modular Design**: Easy to extend and maintain

## Prerequisites

- Python 3.8+
- Apache Spark 3.0+
- Apache Airflow 2.0+
- AWS Account with S3 access

## Installation

1. Clone the repository
2. Set up a virtual environment
3. Install dependencies from requirements.txt

## Configuration

1. Set up AWS credentials
2. Update S3 bucket paths in configuration
3. Configure email notifications

## Running the Pipeline

### Local Development
1. Start Airflow services
2. Access the Airflow UI at `http://localhost:8080`
3. Trigger the DAG manually or wait for scheduled execution

### Production Deployment
1. Deploy to EMR cluster
2. Configure Airflow connections
3. Monitor through Airflow UI

## Data Flow

1. **Landing**: Raw data ingestion and validation
2. **Normalized**: Data standardization and relationship mapping
3. **Summarized**: KPI calculation and analytics
4. **Outbound**: Data export and delivery

## Support

For questions or issues, please contact the development team.
