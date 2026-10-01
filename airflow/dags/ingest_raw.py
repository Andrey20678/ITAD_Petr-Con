"""DAG ЛР № 2: дождаться HTTP-источника и загрузить его в raw."""

from airflow.utils.dates import days_ago
from urllib.parse import urlsplit

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from ingestion.load_raw import load_to_raw

# Публичные параметры конкретного учебного источника.
# Host URL должен совпадать с AIRFLOW_CONN_SOURCE_HTTP_CONN в .env.
SOURCE_URL = "https://raw.githubusercontent.com/Andrey20678/ITAD_Petr-Con/65770360f01d328163cdc5446dc46f46578c6853/data/source/bike%2Bsharing%2Bdataset.zip"
SOURCE_FILENAME = "bike+sharing+dataset.zip"
# Эти два параметра указываются в том случае, если исходный файл окажется zip
# и при этом из него потребуется только некоторые файлы, иначе None
# SOURCE_FILE_CONTENT = ["a.csv", "b.csv"]
SOURCE_FILE_CONTENT = ["day.csv", "hour.csv"]
# SOURCE_FILE_PASSWORD = "SomePassword"
SOURCE_FILE_PASSWORD = None

DATASET_SLUG = "bike_sharing"
HTTP_CONN_ID = "source_http_conn"
S3_CONN_ID = "seaweedfs_s3_conn"

parsed_url = urlsplit(SOURCE_URL)
endpoint = parsed_url.path or "/"
if parsed_url.query:
    endpoint = f"{endpoint}?{parsed_url.query}"

with DAG(
    dag_id="ingest_raw",
    description="Проверяет HTTP-источник и записывает сырые данные в raw-слой MinIO.",
    start_date=days_ago(2),
    schedule="@daily",
    catchup=True,
    tags=["raw"],
    max_active_runs=1,
) as dag:
    
    wait_for_primary_source = HttpSensor(
        task_id="wait_for_primary_source",
        http_conn_id=HTTP_CONN_ID,
        endpoint=endpoint,
        method="GET",
        poke_interval=60,
        timeout=600,
    )

    load_to_raw_task = PythonOperator(
        task_id="load_to_raw",
        python_callable=load_to_raw,
        op_kwargs={
            "source_url": SOURCE_URL,
            "source_filename": SOURCE_FILENAME,
            "source_file_content": SOURCE_FILE_CONTENT,
            "source_file_password": SOURCE_FILE_PASSWORD,
            "dataset_slug": DATASET_SLUG,
            "s3_conn_id": S3_CONN_ID,
        },
    )

    wait_for_primary_source >> load_to_raw_task
