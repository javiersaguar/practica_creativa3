from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'practica',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

IN_K8S = os.path.exists('/var/run/secrets/kubernetes.io')

if IN_K8S:
    bash_cmd = '''
set -e
# Configurar kubectl
/tmp/kubectl config set-cluster gke \
  --server=https://kubernetes.default.svc \
  --certificate-authority=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt
/tmp/kubectl config set-credentials airflow \
  --token=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
/tmp/kubectl config set-context default --cluster=gke --user=airflow
/tmp/kubectl config use-context default

# Obtener pod spark-master
SPARK_POD=$(/tmp/kubectl get pod -l app=spark-master --field-selector=status.phase=Running -o jsonpath="{.items[0].metadata.name}")
echo "Spark pod: $SPARK_POD"

# Lanzar entrenamiento en background dentro de spark-master
/tmp/kubectl exec $SPARK_POD -- bash -c "nohup /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.port=7078 \
  --conf spark.blockManager.port=7079 \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memoryOverhead=256m \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=true \
  --conf spark.executor.extraClassPath=/home/spark/.ivy2/jars/org.apache.iceberg_iceberg-spark-runtime-4.0_2.13-1.10.1.jar:/home/spark/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.4.2.jar:/home/spark/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.367.jar \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.minio=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.minio.type=hadoop \
  --conf spark.sql.catalog.minio.warehouse=s3a://flight-data/warehouse \
  /tmp/train_spark_mllib_model_iceberg.py /home/javisaguarantona/practica_creativa \
  > /tmp/spark_train.log 2>&1 </dev/null &
echo LAUNCHED"

# Esperar a que termine (max 25 min) leyendo el log
echo "Esperando resultado del entrenamiento..."
for i in $(seq 1 300); do
  sleep 5
  RESULT=$(/tmp/kubectl exec $SPARK_POD -- bash -c "tail -3 /tmp/spark_train.log 2>/dev/null" 2>/dev/null || echo "")
  echo "$RESULT"
  if echo "$RESULT" | grep -q "Successfully stopped SparkContext"; then
    echo "=== ENTRENAMIENTO COMPLETADO ==="
    /tmp/kubectl exec $SPARK_POD -- bash -c "grep -E 'Accuracy|MLflow run completado|exitCode' /tmp/spark_train.log" 2>/dev/null
    exit 0
  fi
  if echo "$RESULT" | grep -qE "ERROR|Exception"; then
    echo "=== ERROR EN ENTRENAMIENTO ==="
    exit 1
  fi
done
echo "Timeout esperando entrenamiento"
exit 1
'''
else:
    bash_cmd = '''docker exec -e MLFLOW_TRACKING_URI=http://mlflow:5000 spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=true \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.minio=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.minio.type=hadoop \
  --conf "spark.sql.catalog.minio.warehouse=s3a://flight-data/warehouse" \
  /tmp/train_spark_mllib_model_mlflow.py /home/javisaguarantona/practica_creativa'''

with DAG(
    'retrain_flight_delay_model',
    default_args=default_args,
    description='Reentrenar modelo de retraso de vuelos con Iceberg/MinIO/MLflow',
    schedule_interval='@weekly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['practica', 'spark', 'mlflow'],
) as dag:
    retrain = BashOperator(
        task_id='retrain_model',
        bash_command=bash_cmd,
        execution_timeout=timedelta(minutes=28),
    )
    retrain
