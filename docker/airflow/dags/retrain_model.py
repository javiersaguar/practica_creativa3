from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os


default_args = {
    "owner": "practica",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def spark_train_command(jar_uri):
    return f"""/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode cluster \
  --class es.upm.dit.ging.predictor.TrainModel \
  --conf spark.standalone.submit.waitAppCompletion=true \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=1 \
  --conf spark.jars.ivy=/home/spark/.ivy2 \
  --conf spark.driverEnv.MLFLOW_TRACKING_URI=http://mlflow:5000 \
  --conf spark.driverEnv.MODEL_BASE_PATH=s3a://flight-data/models \
  --conf spark.driverEnv.TRAINING_TABLE=minio.flights.training_data \
  --conf spark.driverEnv.S3_ENDPOINT=http://minio:9000 \
  --conf spark.driverEnv.AWS_ACCESS_KEY_ID=minioadmin \
  --conf spark.driverEnv.AWS_SECRET_ACCESS_KEY=minioadmin \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.minio=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.minio.type=hadoop \
  --conf spark.sql.catalog.minio.warehouse=s3a://flight-data/warehouse \
  {jar_uri}"""


IN_K8S = os.path.exists("/var/run/secrets/kubernetes.io")

if IN_K8S:
    train_cmd = spark_train_command("local:///app/jars/flight_prediction_2.13-0.1.jar")
    bash_cmd = f"""
set -e
/tmp/kubectl config set-cluster gke \
  --server=https://kubernetes.default.svc \
  --certificate-authority=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt
/tmp/kubectl config set-credentials airflow \
  --token=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
/tmp/kubectl config set-context default --cluster=gke --user=airflow
/tmp/kubectl config use-context default

SPARK_POD=$(/tmp/kubectl get pod -l app=spark-master --field-selector=status.phase=Running -o jsonpath="{{.items[0].metadata.name}}")
echo "Spark pod: $SPARK_POD"
/tmp/kubectl exec "$SPARK_POD" -- bash -lc '{train_cmd}'
"""
else:
    train_cmd = spark_train_command("file:///shared-jars/flight_prediction_2.13-0.1.jar")
    bash_cmd = f"""docker exec spark-master bash -lc '{train_cmd}'"""


with DAG(
    "retrain_flight_delay_model",
    default_args=default_args,
    description="Reentrenar modelo de retraso de vuelos con Spark cluster mode",
    schedule_interval="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["practica", "spark", "mlflow"],
) as dag:
    retrain = BashOperator(
        task_id="train_model_cluster_mode",
        bash_command=bash_cmd,
        execution_timeout=timedelta(minutes=28),
    )
