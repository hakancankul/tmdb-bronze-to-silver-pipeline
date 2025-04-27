# TMDB Bronze to Silver Pipeline

This project builds a **Bronze → Silver** data pipeline using the TMDB dataset.  
It leverages **Apache Airflow**, **Apache Spark**, **MinIO**, and **PostgreSQL** as core technologies.

## Technologies Used
- **Apache Airflow**: Workflow orchestration
- **Apache Spark**: Big data processing
- **MinIO**: S3-compatible object storage
- **PostgreSQL**: Metadata database (used by Airflow)
- **Docker-Compose**: For setting up the environment easily

## Pipeline Flow

1. CSV files from the `datasets/` folder are uploaded to MinIO.
2. The DAG (`final_project_dag.py`) runs inside Airflow to:
   - Load raw data into the **Bronze** layer.
   - Transform and clean data using Spark.
   - Save structured data into the **Silver** layer as Delta tables.
3. At the end, a ready-to-query **Silver** layer is generated for analysis.

## How to Start
First, bring up the Docker environment. To do this, navigate to the directory where the docker-compose.yaml file is located and run:
```bash
cd 01_airflow_spark_minio
docker-compose up -d --build


