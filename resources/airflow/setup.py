from datetime import timedelta
import os

import iso8601
from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_HOME = os.getenv("PROJECT_HOME", "/opt/spark")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": iso8601.parse_date("2016-12-01"),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


training_dag = DAG(
    "agile_data_science_batch_prediction_model_training",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)


spark_cluster_training_command = """
docker exec spark-master bash -lc '/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode cluster \
  --class es.upm.dit.ging.predictor.TrainModel \
  --conf spark.standalone.submit.waitAppCompletion=true \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=1 \
  --conf spark.jars.ivy=/home/spark/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=true \
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
  local:///app/jars/flight_prediction_2.13-0.1.jar'
"""


train_classifier_model_operator = BashOperator(
    task_id="spark_train_classifier_model_cluster_mode",
    bash_command=spark_cluster_training_command,
    dag=training_dag,
)
