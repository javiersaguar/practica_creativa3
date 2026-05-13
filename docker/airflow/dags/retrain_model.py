from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "practica",
    "retries": 0,
}

TRAIN_CMD = r"""/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode cluster \
  --class es.upm.dit.ging.predictor.TrainModel \
  --conf spark.standalone.submit.waitAppCompletion=true \
  --conf spark.driver.cores=1 \
  --conf spark.driver.memory=1g \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1g \
  --conf spark.cores.max=2 \
  --conf spark.jars.ivy=/home/spark/.ivy2 \
  --conf spark.driverEnv.MLFLOW_TRACKING_URI=http://mlflow:5000 \
  --conf spark.driverEnv.MODEL_BASE_PATH=s3a://flight-data/models \
  --conf spark.driverEnv.TRAINING_TABLE=minio.flights.training_data \
  --conf spark.driverEnv.S3_ENDPOINT=http://minio:9000 \
  --conf spark.driverEnv.AWS_ACCESS_KEY_ID=minioadmin \
  --conf spark.driverEnv.AWS_SECRET_ACCESS_KEY=minioadmin \
  --conf "spark.driver.extraJavaOptions=-DMLFLOW_TRACKING_URI=http://mlflow:5000 -DTRAINING_TABLE=minio.flights.training_data -DMODEL_BASE_PATH=s3a://flight-data/models -DS3_ENDPOINT=http://minio:9000 -DAWS_ACCESS_KEY_ID=minioadmin -DAWS_SECRET_ACCESS_KEY=minioadmin" \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.minio=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.minio.type=hadoop \
  --conf spark.sql.catalog.minio.warehouse=s3a://flight-data/warehouse \
  file:///shared-jars/flight_prediction_2.13-0.1.jar"""

IN_K8S = os.path.exists("/var/run/secrets/kubernetes.io")

if IN_K8S:
    bash_cmd = "echo 'Usa el DAG K8S especifico para GKE'; exit 1"
else:
    bash_cmd = f"""
set -euo pipefail

echo "=== Preflight ==="
docker ps --format "table {{{{.Names}}}}\\t{{{{.Status}}}}" | grep -E "spark|mlflow|airflow|minio" || true
docker exec spark-worker-1 getent hosts mlflow || true
docker exec spark-worker-2 getent hosts mlflow || true

echo "=== Stop predictor to free Spark resources ==="
docker stop spark-predictor || true

cleanup() {{
  echo "=== Restart predictor ==="
  docker start spark-predictor || true
}}
trap cleanup EXIT

echo "=== Submit TrainModel in Spark cluster mode and wait ==="
docker exec spark-master bash -lc '{TRAIN_CMD}'

echo "=== Training finished ==="
"""

with DAG(
    "retrain_flight_delay_model",
    default_args=default_args,
    schedule_interval="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["practica", "spark", "mlflow"],
) as dag:
    BashOperator(
        task_id="train_model_cluster_mode",
        bash_command=bash_cmd,
        execution_timeout=timedelta(minutes=40),
    )
