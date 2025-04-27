from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

# ======================= DEFAULT ARGS =======================

def_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 27),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

# ======================= DAG DEFINITION =======================

def build_ssh_task(task_id, cmd):
    return SSHOperator(
        task_id=task_id,
        ssh_conn_id='spark_ssh_conn',
        conn_timeout=None,
        cmd_timeout=None,
        command=cmd
    )

with DAG(
    dag_id='pipeline_dag',
    default_args=def_args,
    description='Bronze to Silver pipeline',
    schedule_interval='@daily',
    catchup=False
) as dag:

    create_buckets = build_ssh_task(
        "create_buckets",
        """
        cd /dataops && \
        source airflowenv/bin/activate && \
        python create_bucket.py
        """
    )

    credits_to_bronze = build_ssh_task(
        "credits_to_bronze",
        """
        cd /data-generator && \
        source datagen/bin/activate && \
        python dataframe_to_s3.py \
        -buc tmdb-bronze \
        -k credits/credits_part \
        -aki dataops -sac Ankara06 \
        -eu http://minio:9000 \
        -i /dataops/tmdb_5000_credits.csv \
        -ofp False -z 99999 -b 0 -oh True -shf False -r 1
        """
    )

    movies_to_bronze = build_ssh_task(
        "movies_to_bronze",
        """
        cd /data-generator && \
        source datagen/bin/activate && \
        python dataframe_to_s3.py \
        -buc tmdb-bronze \
        -k movies/movies_part \
        -aki dataops -sac Ankara06 \
        -eu http://minio:9000 \
        -i /dataops/tmdb_5000_movies.csv \
        -ofp False -z 99999 -b 0 -oh True -shf False -r 1
        """
    )

    credits_to_silver = build_ssh_task(
        "credits_to_silver",
        """
        cd /dataops && \
        source airflowenv/bin/activate && \
        python credits_bronze_to_silver.py
        """
    )

    movies_to_silver = build_ssh_task(
        "movies_to_silver",
        """
        cd /dataops && \
        source airflowenv/bin/activate && \
        python movies_bronze_to_silver.py
        """
    )

    # Task Dependencies
    create_buckets >> credits_to_bronze >> movies_to_bronze >> credits_to_silver >> movies_to_silver
