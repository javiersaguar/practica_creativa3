#!/bin/bash
# ============================================================
#   PRACTICA BIGDATA — SCRIPT UNIFICADO
#   ETSIT UPM 2026
# ============================================================

ZONE="europe-southwest1-a"
CLUSTER="practica-k8s"
PROJECT_HOME="${PROJECT_HOME:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
MANIFESTS="${MANIFESTS:-$PROJECT_HOME/k8s-gke}"
SPARK_HOME=~/spark-4.1.1
export PROJECT_HOME

# PATH para spark-submit
export PATH=$SPARK_HOME/bin:$PATH
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m'

# ============================================================
#   FUNCIONES AUXILIARES
# ============================================================

header() {
  echo ""
  echo -e "${BOLD}${BLUE}============================================${NC}"
  echo -e "${BOLD}${BLUE}  $1${NC}"
  echo -e "${BOLD}${BLUE}============================================${NC}"
  echo ""
}

subheader() {
  echo ""
  echo -e "${BOLD}${CYAN}  -- $1 --${NC}"
  echo ""
}

ok()   { echo -e "  ${GREEN}OK${NC} $1"; }
info() { echo -e "  ${CYAN}->${NC} $1"; }
warn() { echo -e "  ${YELLOW}!!${NC} $1"; }
err()  { echo -e "  ${RED}XX${NC} $1"; }

pause() {
  echo ""
  echo -n "  Pulsa ENTER para continuar..."
  read
}

show_urls_docker() {
  IP=$(curl -s ifconfig.me 2>/dev/null || echo "IP_DESCONOCIDA")
  header "URLS DOCKER COMPOSE"
  echo -e "  ${BOLD}Flask (prediccion):${NC}"
  echo -e "    http://$IP:5001/flights/delays/predict_kafka"
  echo ""
  echo -e "  ${BOLD}Spark UI:${NC}           http://$IP:8080"
  echo -e "  ${BOLD}Grafana:${NC}            http://$IP:3000  (admin/admin)"
  echo -e "  ${BOLD}Prometheus:${NC}         http://$IP:9090"
  echo -e "  ${BOLD}MinIO consola:${NC}      http://$IP:9001  (minioadmin/minioadmin)"
  echo -e "  ${BOLD}MLflow:${NC}             http://$IP:5002"
  echo -e "  ${BOLD}Airflow:${NC}            http://$IP:8081  (admin/admin)"
  echo ""
  pause
}

show_urls_k8s() {
  NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}' 2>/dev/null)
  FLASK_IP=$(kubectl get service flask -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
  header "URLS KUBERNETES (GKE)"
  if [ -n "$FLASK_IP" ]; then
    echo -e "  ${BOLD}Flask (LoadBalancer):${NC}"
    echo -e "    http://$FLASK_IP:5001/flights/delays/predict_kafka"
  else
    echo -e "  ${BOLD}Flask (NodePort):${NC}   http://$NODE_IP:30001/flights/delays/predict_kafka"
  fi
  echo -e "  ${BOLD}Grafana:${NC}            http://$NODE_IP:30300  (admin/admin)"
  echo -e "  ${BOLD}Prometheus:${NC}         http://$NODE_IP:30909"
  echo -e "  ${BOLD}MinIO consola:${NC}      http://$NODE_IP:30901  (minioadmin/minioadmin)"
  echo -e "  ${BOLD}MLflow:${NC}             http://$NODE_IP:30502"
  echo -e "  ${BOLD}Airflow:${NC}            http://$NODE_IP:30808  (admin/admin)"
  echo ""
  pause
}

# ============================================================
#   ARRANQUE DOCKER COMPOSE
# ============================================================

arrancar_docker() {
  header "ARRANCANDO STACK DOCKER COMPOSE"
  cd $PROJECT_HOME

  info "Compilando JAR Scala si no existe o es antiguo..."
  if [ ! -f "$PROJECT_HOME/shared-jars/flight_prediction_2.13-0.1.jar" ] ||      [ "$PROJECT_HOME/flight_prediction/src/main/scala/es/upm/dit/ging/predictor/MakePrediction.scala" -nt "$PROJECT_HOME/shared-jars/flight_prediction_2.13-0.1.jar" ] ||      [ "$PROJECT_HOME/flight_prediction/src/main/scala/es/upm/dit/ging/predictor/TrainModel.scala" -nt "$PROJECT_HOME/shared-jars/flight_prediction_2.13-0.1.jar" ]; then
    info "Recompilando JAR con sbt..."
    docker run --rm       -v "$PROJECT_HOME/flight_prediction":/app       -w /app       sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.12.10_2.13.18       sbt assembly
    mkdir -p "$PROJECT_HOME/shared-jars"
    cp "$PROJECT_HOME/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar"        "$PROJECT_HOME/shared-jars/flight_prediction_2.13-0.1.jar"
    cp "$PROJECT_HOME/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar"        "$PROJECT_HOME/docker/spark/flight_prediction_2.13-0.1.jar"
    ok "JAR compilado"
    info "Reconstruyendo imagenes Spark con nuevo JAR..."
    docker compose build spark-master spark-worker-1 spark-worker-2 spark-predictor
    ok "Imagenes reconstruidas"
  else
    ok "JAR ya existe y esta actualizado"
  fi

  info "Levantando contenedores..."
  docker compose up -d

  info "Esperando inicializacion (30s)..."
  sleep 30

  info "Esperando a que Cassandra este lista..."
  until docker exec cassandra cqlsh -e "describe keyspaces" > /dev/null 2>&1; do
    echo "    Cassandra aun arrancando, esperando 10s..."
    sleep 10
  done
  ok "Cassandra lista"

  info "Spark predictor se creara cuando MinIO tenga los modelos..."

  info "Reiniciando Flask..."
  docker compose stop flask
  docker compose start flask

  info "Esperando inicializacion final (20s)..."
  sleep 20

  info "Configurando Airflow..."
  docker exec airflow airflow users create \
    --username admin --password admin \
    --firstname Admin --lastname Admin \
    --role Admin --email admin@example.com 2>/dev/null || true

  info "Configurando MinIO bucket..."
  docker exec minio sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin && mc mb local/flight-data 2>/dev/null || true" 2>/dev/null

  info "Asegurando JAR en shared-jars..."
  mkdir -p "$PROJECT_HOME/shared-jars"
  cp "$PROJECT_HOME/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar"      "$PROJECT_HOME/shared-jars/flight_prediction_2.13-0.1.jar" 2>/dev/null     && ok "JAR copiado a shared-jars" || warn "JAR no encontrado en target"

  info "Creando keyspace y tablas en Cassandra..."
  docker exec cassandra cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS agile_data_science
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS agile_data_science.origin_dest_distances (
  origin TEXT, dest TEXT, distance DOUBLE, PRIMARY KEY (origin, dest));
CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_classification_response (
  origin TEXT, dest TEXT, flight_date TEXT, dep_delay DOUBLE,
  carrier TEXT, uuid TEXT, prediction DOUBLE, timestamp TIMESTAMP,
  PRIMARY KEY (uuid));" 2>/dev/null && ok "Keyspace creado" || warn "Error Cassandra"

  info "Importando distancias en Cassandra..."
  python3 -c "
import json
lines_cql = []
with open('$PROJECT_HOME/data/origin_dest_distances.jsonl') as f:
    for line in f:
        r = json.loads(line)
        lines_cql.append(\"INSERT INTO agile_data_science.origin_dest_distances (origin, dest, distance) VALUES ('{}', '{}', {});\".format(r['Origin'], r['Dest'], float(r['Distance'])))
with open('/tmp/distances.cql', 'w') as f:
    f.write(chr(10).join(lines_cql))
" 2>/dev/null
  docker cp /tmp/distances.cql cassandra:/tmp/distances.cql 2>/dev/null
  docker exec cassandra cqlsh -f /tmp/distances.cql 2>/dev/null && ok "Distancias importadas" || warn "Error importando distancias"

  info "Reiniciando workers para montar shared-jars..."
  docker compose restart spark-master spark-worker-1 spark-worker-2
  sleep 15

  info "Subiendo datos de entrenamiento a MinIO..."
  docker cp "$PROJECT_HOME/data/simple_flight_delay_features.jsonl.bz2" minio:/tmp/ 2>/dev/null
  docker exec minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin && mc cp /tmp/simple_flight_delay_features.jsonl.bz2 local/flight-data/data/simple_flight_delay_features.jsonl.bz2 2>/dev/null"     && ok "Datos subidos a MinIO" || warn "Error subiendo datos"

  info "Creando tabla Iceberg (2-3 min)..."
  cat > /tmp/load_iceberg.py << 'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("load-iceberg") \
  .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
  .config("spark.sql.catalog.minio","org.apache.iceberg.spark.SparkCatalog") \
  .config("spark.sql.catalog.minio.type","hadoop") \
  .config("spark.sql.catalog.minio.warehouse","s3a://flight-data/warehouse") \
  .config("spark.hadoop.fs.s3a.endpoint","http://minio:9000") \
  .config("spark.hadoop.fs.s3a.access.key","minioadmin") \
  .config("spark.hadoop.fs.s3a.secret.key","minioadmin") \
  .config("spark.hadoop.fs.s3a.path.style.access","true") \
  .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
  .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false") \
  .getOrCreate()
df = spark.read.json("s3a://flight-data/data/simple_flight_delay_features.jsonl.bz2")
spark.sql("CREATE NAMESPACE IF NOT EXISTS minio.flights")
spark.sql("DROP TABLE IF EXISTS minio.flights.training_data")
df.writeTo("minio.flights.training_data").create()
print("Registros Iceberg:", spark.table("minio.flights.training_data").count())
spark.stop()
PY
  docker cp /tmp/load_iceberg.py spark-master:/tmp/load_iceberg.py
  docker exec spark-master bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.367 --conf spark.jars.ivy=/home/spark/.ivy2 /tmp/load_iceberg.py" 2>&1 | grep -E "Registros|ERROR" | tail -3 && ok "Tabla Iceberg creada" || warn "Error Iceberg"

  info "Entrenando modelo TrainModel en deploy-mode cluster (3-4 min)..."
  docker exec spark-master bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode cluster --class es.upm.dit.ging.predictor.TrainModel --conf spark.standalone.submit.waitAppCompletion=true --conf spark.driver.memory=1g --conf spark.executor.memory=1g --conf spark.executor.cores=1 --conf spark.jars.ivy=/home/spark/.ivy2 --conf spark.driverEnv.MLFLOW_TRACKING_URI=http://mlflow:5000 --conf "spark.driver.extraJavaOptions=-DMLFLOW_TRACKING_URI=http://mlflow:5000 -DMODEL_BASE_PATH=s3a://flight-data/models -DS3_ENDPOINT=http://minio:9000 -DAWS_ACCESS_KEY_ID=minioadmin -DAWS_SECRET_ACCESS_KEY=minioadmin" --conf spark.driverEnv.MODEL_BASE_PATH=s3a://flight-data/models --conf spark.driverEnv.TRAINING_TABLE=minio.flights.training_data --conf spark.driverEnv.S3_ENDPOINT=http://minio:9000 --conf spark.driverEnv.AWS_ACCESS_KEY_ID=minioadmin --conf spark.driverEnv.AWS_SECRET_ACCESS_KEY=minioadmin --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.access.key=minioadmin --conf spark.hadoop.fs.s3a.secret.key=minioadmin --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.minio=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.minio.type=hadoop --conf spark.sql.catalog.minio.warehouse=s3a://flight-data/warehouse file:///shared-jars/flight_prediction_2.13-0.1.jar" 2>&1 | grep -E "Training completed|Accuracy|ERROR" | tail -3 && ok "Modelo entrenado" || warn "Error entrenamiento"

  info "Arrancando Spark predictor en modo cluster..."
  docker compose --profile predictor up -d spark-predictor
  ok "Predictor enviado al cluster"

  info "Disparando DAG Airflow para reentrenamientos futuros..."
  docker exec airflow airflow dags unpause retrain_flight_delay_model 2>/dev/null || true

  ok "Stack Docker listo"
  show_urls_docker
  pause
}

# ============================================================
#   ARRANQUE KUBERNETES (GKE)
# ============================================================

arrancar_k8s() {
  header "ARRANCANDO KUBERNETES GKE"

  info "Autenticando con el cluster..."
  gcloud container clusters get-credentials $CLUSTER --zone $ZONE

  NODE_COUNT=$(kubectl get nodes --no-headers 2>/dev/null | wc -l)
  if [ "$NODE_COUNT" -lt "2" ]; then
    info "Escalando cluster a 2 nodos..."
    gcloud container clusters resize $CLUSTER --num-nodes=2 --zone $ZONE --quiet
    info "Esperando nodos Ready (60s)..."
    sleep 60
    kubectl wait --for=condition=Ready nodes --all --timeout=120s
  else
    ok "Cluster ya tiene $NODE_COUNT nodos activos"
  fi

  info "Creando ConfigMap del DAG de Airflow..."
  kubectl create configmap airflow-dags \
    --from-file=retrain_model.py=$PROJECT_HOME/docker/airflow/dags/retrain_model.py \
    --dry-run=client -o yaml | kubectl apply -f - 2>/dev/null \
    && ok "ConfigMap airflow-dags creado" || warn "Error creando ConfigMap"


  info "Aplicando manifests..."
  kubectl apply -f $MANIFESTS/mongo.yaml
  kubectl apply -f $MANIFESTS/cassandra.yaml
  kubectl apply -f $MANIFESTS/minio.yaml
  kubectl apply -f $MANIFESTS/kafka.yaml
  kubectl apply -f $MANIFESTS/spark.yaml
  kubectl apply -f $MANIFESTS/flask.yaml
  kubectl apply -f $MANIFESTS/prometheus.yaml
  kubectl apply -f $MANIFESTS/grafana.yaml
  kubectl apply -f $MANIFESTS/mlflow.yaml
  kubectl apply -f $MANIFESTS/airflow.yaml
  kubectl apply -f $MANIFESTS/spark-predictor-patch.yaml

  info "Esperando pods criticos (60s)..."
  sleep 30
  kubectl wait --for=condition=Ready pod -l app=mongo --timeout=120s 2>/dev/null || true
  kubectl wait --for=condition=Ready pod -l app=kafka --timeout=120s 2>/dev/null || true
  kubectl wait --for=condition=Ready pod -l app=flask --timeout=120s 2>/dev/null || true

  info "Configurando MinIO y subiendo modelos..."
  MINIO_POD=$(kubectl get pod -l app=minio -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  if [ -n "$MINIO_POD" ]; then
    kubectl exec $MINIO_POD -- sh -c \
      "mc alias set local http://localhost:9000 minioadmin minioadmin && mc mb local/flight-data 2>/dev/null || true" 2>/dev/null || true
    kubectl exec $MINIO_POD -- mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null
    kubectl exec $MINIO_POD -- mc mb local/flight-data 2>/dev/null || true
    find "$PROJECT_HOME/models" -type f | while read f; do
      REL="${f#$PROJECT_HOME/models/}"
      cat "$f" | kubectl exec -i $MINIO_POD -- mc pipe "local/flight-data/models/$REL" 2>/dev/null
    done
    ok "MinIO configurado y modelos subidos"
  fi

  info "Esperando Cassandra lista..."
  kubectl wait --for=condition=Ready pod -l app=cassandra --timeout=180s 2>/dev/null || true
  sleep 10

  info "Creando keyspace y tablas en Cassandra..."
  kubectl exec deployment/cassandra -- cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS agile_data_science
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS agile_data_science.origin_dest_distances (
  origin TEXT, dest TEXT, distance DOUBLE, PRIMARY KEY (origin, dest));
CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_classification_response (
  origin TEXT, dest TEXT, flight_date TEXT, dep_delay DOUBLE,
  carrier TEXT, uuid TEXT, prediction DOUBLE, timestamp TIMESTAMP,
  PRIMARY KEY (uuid));" 2>/dev/null && ok "Keyspace y tablas creados" || warn "Error creando tablas Cassandra"

  info "Importando distancias en Cassandra..."
  python3 << 'PYEOF'
import json, os
lines = []
with open(os.environ.get('PROJECT_HOME', '.') + '/data/origin_dest_distances.jsonl') as f:
    for line in f:
        r = json.loads(line)
        origin = r['Origin']
        dest = r['Dest']
        distance = float(r['Distance'])
        lines.append(f"INSERT INTO agile_data_science.origin_dest_distances (origin, dest, distance) VALUES ('{origin}', '{dest}', {distance});")
with open('/tmp/distances_k8s.cql', 'w') as f:
    f.write('\n'.join(lines))
print(f'Generadas {len(lines)} sentencias')
PYEOF
  kubectl cp /tmp/distances_k8s.cql \
    $(kubectl get pod -l app=cassandra -o jsonpath='{.items[0].metadata.name}'):/tmp/distances.cql 2>/dev/null
  kubectl exec deployment/cassandra -- cqlsh -f /tmp/distances.cql 2>/dev/null \
    && ok "Distancias importadas" || warn "Error importando distancias"

  info "Reiniciando Kafka y Flask para asegurar conexion..."
  kubectl rollout restart deployment/kafka 2>/dev/null
  sleep 15
  kubectl rollout restart deployment/flask 2>/dev/null
  sleep 15
  kubectl rollout restart deployment/spark-predictor 2>/dev/null

  info "Verificando que el predictor K8s arranca correctamente..."
  sleep 60
  PREDICTOR_POD=$(kubectl get pod -l app=spark-predictor --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  if [ -n "$PREDICTOR_POD" ]; then
    ASSERT_ERR=$(kubectl logs $PREDICTOR_POD 2>/dev/null | grep -c "AssertionError\|Decision Tree load failed" 2>/dev/null)
    if [ "$ASSERT_ERR" -gt "0" ]; then
      warn "Modelos incompatibles detectados - lanzando reentrenamiento automatico..."
      SPARK_POD=$(kubectl get pod -l app=spark-master --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
      AIRFLOW_POD=$(kubectl get pod -l app=airflow --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
      kubectl exec $AIRFLOW_POD -- bash -c "nohup airflow scheduler >> /tmp/scheduler.log 2>&1 &" 2>/dev/null
      sleep 5
      kubectl exec $AIRFLOW_POD -- airflow dags trigger retrain_flight_delay_model 2>/dev/null
      ok "Reentrenamiento disparado - el predictor estara listo en ~10 min"
    else
      ok "Predictor K8s arrancado correctamente"
    fi
  fi

  info "Descargando kubectl binary en la VM..."
  if [ ! -f /tmp/kubectl ]; then
    curl -sL "https://dl.k8s.io/release/v1.29.0/bin/linux/amd64/kubectl" -o /tmp/kubectl
    chmod +x /tmp/kubectl
  fi
  ok "kubectl binary listo en /tmp/kubectl"

  info "Esperando pods Airflow y Spark Running..."
  kubectl wait --for=condition=Ready pod -l app=airflow --timeout=180s 2>/dev/null || true
  kubectl wait --for=condition=Ready pod -l app=spark-master --timeout=120s 2>/dev/null || true
  sleep 10

  AIRFLOW_POD=$(kubectl get pod -l app=airflow --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  SPARK_POD=$(kubectl get pod -l app=spark-master --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

  if [ -n "$AIRFLOW_POD" ]; then
    info "Configurando Airflow (kubectl + scheduler + DAG unpause)..."
    kubectl cp /tmp/kubectl $AIRFLOW_POD:/tmp/kubectl 2>/dev/null
    kubectl exec $AIRFLOW_POD -- chmod +x /tmp/kubectl 2>/dev/null
    kubectl exec $AIRFLOW_POD -- bash -c "nohup airflow scheduler >> /tmp/scheduler.log 2>&1 &" 2>/dev/null
    sleep 5
    kubectl exec $AIRFLOW_POD -- airflow dags unpause retrain_flight_delay_model 2>/dev/null || true
    ok "Airflow configurado"
  fi

  if [ -n "$SPARK_POD" ]; then
    kubectl exec $SPARK_POD -- test -f /app/jars/flight_prediction_2.13-0.1.jar 2>/dev/null \
      && ok "spark-master preparado con JAR Scala" \
      || warn "No se encuentra el JAR en /app/jars dentro de spark-master"
  fi

  echo ""
  info "Estado de pods:"
  kubectl get pods -o wide
  echo ""

  ok "Stack Kubernetes listo"
  show_urls_k8s
  pause
}

# ============================================================
#   REENTRENAMIENTO
# ============================================================

reentrenar_docker() {
  header "REENTRENAMIENTO -- DOCKER"
  warn "El reentrenamiento en Docker ejecuta el DAG de Airflow"

  info "Disparando DAG retrain_flight_delay_model en Airflow..."
  docker exec airflow airflow dags unpause retrain_flight_delay_model 2>/dev/null || true
  docker exec airflow airflow dags trigger retrain_flight_delay_model 2>/dev/null \
    && ok "DAG disparado correctamente" \
    || warn "Error disparando DAG -- comprueba Airflow UI"

  IP=$(curl -s ifconfig.me 2>/dev/null || echo "IP_DESCONOCIDA")
  info "Sigue el progreso en: http://$IP:8081 -> DAGs -> retrain_flight_delay_model"
  info "MLflow: http://$IP:5002 -> flight_delay_prediction"
  info "El entrenamiento tarda ~4 minutos. Puedes monitorizar en la UI."
  pause
}

reentrenar_k8s() {
  header "REENTRENAMIENTO -- KUBERNETES"
  warn "El reentrenamiento en K8s ejecuta el DAG de Airflow"

  info "Verificando conexion al cluster..."
  gcloud container clusters get-credentials $CLUSTER --zone $ZONE 2>/dev/null

  AIRFLOW_POD=$(kubectl get pod -l app=airflow --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  SPARK_POD=$(kubectl get pod -l app=spark-master --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

  if [ -z "$AIRFLOW_POD" ]; then
    err "Pod de Airflow no encontrado. Esta el cluster arrancado?"
    return 1
  fi

  info "Asegurando kubectl y script de entrenamiento..."
  if [ ! -f /tmp/kubectl ]; then
    curl -sL "https://dl.k8s.io/release/v1.29.0/bin/linux/amd64/kubectl" -o /tmp/kubectl
    chmod +x /tmp/kubectl
  fi
  kubectl cp /tmp/kubectl $AIRFLOW_POD:/tmp/kubectl 2>/dev/null || true
  kubectl exec $AIRFLOW_POD -- chmod +x /tmp/kubectl 2>/dev/null || true

  if [ -n "$SPARK_POD" ]; then
    kubectl exec $SPARK_POD -- test -f /app/jars/flight_prediction_2.13-0.1.jar 2>/dev/null \
      || warn "No se encuentra el JAR en /app/jars dentro de spark-master"
  fi

  info "Asegurando scheduler activo..."
  kubectl exec $AIRFLOW_POD -- bash -c "nohup airflow scheduler >> /tmp/scheduler.log 2>&1 &" 2>/dev/null || true
  sleep 3

  info "Disparando DAG retrain_flight_delay_model..."
  kubectl exec $AIRFLOW_POD -- airflow dags unpause retrain_flight_delay_model 2>/dev/null || true
  kubectl exec $AIRFLOW_POD -- airflow dags trigger retrain_flight_delay_model 2>/dev/null \
    && ok "DAG disparado correctamente" \
    || warn "Error disparando DAG -- comprueba Airflow UI"

  NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}' 2>/dev/null)
  info "Sigue el progreso en: http://$NODE_IP:30808 -> DAGs -> retrain_flight_delay_model"
  info "MLflow: http://$NODE_IP:30502 -> flight_delay_prediction"
  pause
}

apagar_k8s() {
  header "APAGANDO CLUSTER GKE"
  warn "Esto apaga los nodos del cluster (ahorra dinero)"
  echo -n "  Confirmas? (s/N): "
  read confirm
  if [[ "$confirm" =~ ^[sS]$ ]]; then
    gcloud container clusters resize $CLUSTER --num-nodes=0 --zone $ZONE --quiet
    ok "Cluster apagado"
  else
    info "Operacion cancelada"
  fi
  pause
}

# ============================================================
#   PARAR DOCKER
# ============================================================

parar_docker() {
  header "PARANDO DOCKER COMPOSE"
  cd $PROJECT_HOME
  docker compose stop
  ok "Docker Compose parado"
  pause
}

# ============================================================
#   DIAGNOSTICO
# ============================================================

diag_cassandra() {
  header "DIAGNOSTICO -- CASSANDRA"

  subheader "Keyspaces y tablas"
  docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES;" 2>/dev/null || err "Cassandra no disponible"

  subheader "Tablas en agile_data_science"
  docker exec cassandra cqlsh -e "DESCRIBE TABLES;" agile_data_science 2>/dev/null

  subheader "Distancias origin-dest (primeras 5)"
  docker exec cassandra cqlsh -e "SELECT origin, dest, distance FROM agile_data_science.origin_dest_distances LIMIT 5;" 2>/dev/null

  subheader "Total distancias almacenadas"
  docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM agile_data_science.origin_dest_distances;" 2>/dev/null

  subheader "Ultimas 5 predicciones en Cassandra"
  docker exec cassandra cqlsh -e "SELECT uuid, origin, dest, prediction, timestamp FROM agile_data_science.flight_delay_classification_response LIMIT 5;" 2>/dev/null

  subheader "Total predicciones almacenadas"
  docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM agile_data_science.flight_delay_classification_response;" 2>/dev/null

  pause
}

diag_kafka() {
  header "DIAGNOSTICO -- KAFKA"

  subheader "Topics activos"
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list 2>/dev/null || err "Kafka no disponible"

  subheader "Detalles del topic de requests"
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --describe --topic flight-delay-ml-request 2>/dev/null

  subheader "Detalles del topic de responses"
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --describe --topic flight-delay-ml-response 2>/dev/null

  subheader "Ultimos 3 mensajes en flight-delay-ml-response"
  echo "  (Espera hasta 10s si no hay mensajes recientes)"
  docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic flight-delay-ml-response \
    --from-beginning --max-messages 3 \
    --timeout-ms 10000 2>/dev/null | python3 -m json.tool 2>/dev/null || warn "Sin mensajes en el topic"

  subheader "Consumer group de Flask (WebSockets)"
  docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe --group flask-ws-1777393487 2>/dev/null

  pause
}

diag_minio() {
  header "DIAGNOSTICO -- MINIO (Data Lakehouse)"

  subheader "Buckets disponibles"
  docker exec minio sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null && mc ls local/" 2>/dev/null

  subheader "Modelos en MinIO (s3a://flight-data/models/)"
  docker exec minio sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null && mc ls local/flight-data/models/" 2>/dev/null

  subheader "Tabla Iceberg -- Training Data (warehouse)"
  docker exec minio sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null && mc ls local/flight-data/warehouse/flights/training_data/" 2>/dev/null || warn "Tabla Iceberg no encontrada"

  subheader "Metadatos Iceberg (snapshots)"
  docker exec minio sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null && mc ls local/flight-data/warehouse/flights/training_data/metadata/" 2>/dev/null

  subheader "Tamano total del bucket flight-data"
  docker exec minio sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null && mc du local/flight-data/" 2>/dev/null

  pause
}

diag_spark() {
  header "DIAGNOSTICO -- SPARK STREAMING"

  subheader "Master efectivo del Spark Predictor"
  docker logs spark-predictor 2>&1 | grep "Spark master:" | tail -3

  subheader "Estado del Spark Predictor (4 sinks)"
  docker logs spark-predictor 2>&1 | grep -E "sink=|Sink|Stream started|MicroBatch|Started" | tail -10

  subheader "Micro-batches procesados"
  docker logs spark-predictor 2>&1 | grep -E "Batch|batch|committed" | tail -10

  subheader "Workers conectados al master"
  docker logs spark-master 2>&1 | grep -E "worker|Worker|registered|Registered" | tail -8

  subheader "Errores recientes en Spark"
  docker logs spark-predictor 2>&1 | grep "ERROR" | tail -10

  pause
}

diag_mongodb() {
  header "DIAGNOSTICO -- MONGODB"

  subheader "Bases de datos disponibles"
  docker exec mongo mongosh --quiet --eval "db.adminCommand('listDatabases').databases.map(d => d.name)" 2>/dev/null

  subheader "Total predicciones en MongoDB"
  docker exec mongo mongosh --quiet agile_data_science \
    --eval "db.flight_delay_ml_response.countDocuments()" 2>/dev/null

  subheader "Ultimas 3 predicciones en MongoDB"
  docker exec mongo mongosh --quiet agile_data_science \
    --eval "db.flight_delay_ml_response.find({},{UUID:1,Prediction:1,Origin:1,Dest:1,_id:0}).sort({Timestamp:-1}).limit(3).toArray()" 2>/dev/null

  subheader "Tamano de la coleccion"
  docker exec mongo mongosh --quiet agile_data_science \
    --eval "db.flight_delay_ml_response.stats().size" 2>/dev/null

  pause
}

diag_prometheus() {
  header "DIAGNOSTICO -- PROMETHEUS & GRAFANA"

  subheader "Targets de Prometheus (estado)"
  curl -s http://localhost:9090/api/v1/targets | python3 -m json.tool 2>/dev/null | \
    grep -E '"job"|"health"|"lastError"' | head -20 || err "Prometheus no disponible"

  subheader "Metricas Flask -- Total requests a Kafka"
  curl -s "http://localhost:9090/api/v1/query?query=flight_requests_total" | \
    python3 -m json.tool 2>/dev/null | grep -E "value|result" | head -5

  subheader "Metricas Flask -- Predicciones por categoria"
  curl -s "http://localhost:9090/api/v1/query?query=flight_predictions_total" | \
    python3 -m json.tool 2>/dev/null | grep -E "category|value" | head -15

  subheader "Metricas disponibles en Flask /metrics"
  curl -s http://localhost:5001/metrics | grep -E "^flight_|^flask_http_request_total" | head -15

  subheader "Dashboard Grafana cargado"
  curl -s http://admin:admin@localhost:3000/api/search | \
    python3 -m json.tool 2>/dev/null | grep -E "title|uid" | head -5

  pause
}

diag_pipeline_completo() {
  header "DIAGNOSTICO -- PIPELINE COMPLETO"

  subheader "Estado de todos los contenedores"
  docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null

  subheader "Verificacion end-to-end"
  echo ""

  FLASK_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5001/flights/delays/predict_kafka 2>/dev/null)
  [ "$FLASK_STATUS" = "200" ] && ok "Flask responde (HTTP $FLASK_STATUS)" || err "Flask no responde (HTTP $FLASK_STATUS)"

  TOPICS=$(docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | tr '\n' ' ')
  echo "$TOPICS" | grep -q "flight-delay-ml-request" && ok "Kafka topic request existe" || err "Topic request no encontrado"
  echo "$TOPICS" | grep -q "flight-delay-ml-response" && ok "Kafka topic response existe" || err "Topic response no encontrado"

  CASS_COUNT=$(docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM agile_data_science.origin_dest_distances;" 2>/dev/null | grep -E "[0-9]+" | tr -d ' ')
  [ "$CASS_COUNT" = "4696" ] && ok "Cassandra: 4696 distancias cargadas" || warn "Cassandra: $CASS_COUNT distancias (esperado 4696)"

  MONGO_COUNT=$(docker exec mongo mongosh --quiet agile_data_science \
    --eval "db.flight_delay_ml_response.countDocuments()" 2>/dev/null)
  ok "MongoDB: $MONGO_COUNT predicciones almacenadas"

  MODELS=$(docker exec minio sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null && mc ls local/flight-data/models/" 2>/dev/null | wc -l)
  [ "$MODELS" -gt "0" ] && ok "MinIO: modelos disponibles ($MODELS ficheros)" || err "MinIO: no hay modelos"

  PROM_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9090/-/healthy 2>/dev/null)
  [ "$PROM_STATUS" = "200" ] && ok "Prometheus healthy" || err "Prometheus no responde"

  FLASK_METRICS=$(curl -s http://localhost:5001/metrics 2>/dev/null | grep -c "flight_requests_total")
  [ "$FLASK_METRICS" -gt "0" ] && ok "Flask /metrics activo (prometheus-flask-exporter)" || err "Flask /metrics no disponible"

  SPARK_RUNNING=$(docker logs spark-predictor 2>&1 | grep -c "Stream started" 2>/dev/null)
  [ "$SPARK_RUNNING" -gt "0" ] && ok "Spark Streaming activo ($SPARK_RUNNING streams)" || warn "Spark Streaming: verificar logs"

  pause
}

diag_websockets() {
  header "DIAGNOSTICO -- WEBSOCKETS"

  subheader "Thread consumer Kafka en Flask"
  docker logs flask 2>&1 | grep -E "kafka|Kafka|consumer|Consumer|socket|Socket|prediction" | tail -10

  subheader "Eventos Socket.IO emitidos"
  docker logs flask 2>&1 | grep -E "emit|prediction_response|socketio" | tail -10

  subheader "Polling REST activo"
  docker logs flask 2>&1 | grep "classify_realtime" | tail -10

  subheader "Prueba end-to-end -- enviar prediccion y verificar respuesta Kafka"
  echo ""
  echo "  Enviando prediccion de prueba..."
  curl -s -X POST http://localhost:5001/flights/delays/predict/classify_realtime \
    -d "DepDelay=15&Carrier=AA&FlightDate=2016-12-25&Origin=ATL&Dest=SFO&FlightNum=1234" \
    > /tmp/pred_response.json 2>/dev/null
  UUID_RESP=$(cat /tmp/pred_response.json | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('id','ERROR'))" 2>/dev/null)
  ok "Prediccion enviada con UUID: $UUID_RESP"

  echo ""
  echo "  Esperando respuesta de Spark via Kafka (10s)..."
  sleep 10

  RESULT=$(curl -s "http://localhost:5001/flights/delays/predict/classify_realtime/response/$UUID_RESP" 2>/dev/null)
  STATUS=$(echo $RESULT | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status','?'))" 2>/dev/null)
  if [ "$STATUS" = "OK" ]; then
    PRED=$(echo $RESULT | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('prediction',{}).get('Prediction','?'))" 2>/dev/null)
    ok "Prediccion recibida: categoria $PRED -- Pipeline WebSocket/Kafka FUNCIONA"
  else
    warn "Estado: $STATUS -- puede que Spark aun este procesando"
  fi

  pause
}

diag_mlflow() {
  header "DIAGNOSTICO -- MLFLOW"

  subheader "Experimentos registrados"
  curl -s http://localhost:5002/api/2.0/mlflow/experiments/list | \
    python3 -m json.tool 2>/dev/null | grep -E "name|experiment_id" | head -10 || err "MLflow no disponible"

  subheader "Ultimas ejecuciones (runs)"
  curl -s "http://localhost:5002/api/2.0/mlflow/runs/search" \
    -H "Content-Type: application/json" \
    -d '{"max_results": 5}' | \
    python3 -m json.tool 2>/dev/null | grep -E "run_id|status|start_time|run_name" | head -20

  pause
}

diag_airflow() {
  header "DIAGNOSTICO -- AIRFLOW"

  subheader "DAGs disponibles"
  docker exec airflow airflow dags list 2>/dev/null | grep -v "^$"

  subheader "Ultimas ejecuciones del DAG de reentrenamiento"
  docker exec airflow airflow dags list-runs -d retrain_flight_delay_model 2>/dev/null | head -10

  subheader "Estado del scheduler"
  docker logs airflow 2>&1 | grep -E "scheduler|Scheduler|DAG|dag" | tail -8

  pause
}

menu_diagnostico() {
  while true; do
    clear
    echo ""
    echo -e "${BOLD}${MAGENTA}============================================${NC}"
    echo -e "${BOLD}${MAGENTA}  DIAGNOSTICO -- PRACTICA BIGDATA${NC}"
    echo -e "${BOLD}${MAGENTA}============================================${NC}"
    echo ""
    echo -e "  ${BOLD}COMPONENTES${NC}"
    echo -e "  ${GREEN}1)${NC} Pipeline completo (resumen rapido)"
    echo -e "  ${GREEN}2)${NC} Cassandra (keyspace, tablas, datos)"
    echo -e "  ${GREEN}3)${NC} Kafka (topics, mensajes, consumer groups)"
    echo -e "  ${GREEN}4)${NC} WebSockets (prueba end-to-end automatica)"
    echo -e "  ${GREEN}5)${NC} MinIO -- Data Lakehouse (modelos, Iceberg)"
    echo -e "  ${GREEN}6)${NC} Spark Streaming (sinks, micro-batches)"
    echo -e "  ${GREEN}7)${NC} MongoDB (predicciones almacenadas)"
    echo -e "  ${GREEN}8)${NC} Prometheus & Grafana (metricas)"
    echo -e "  ${GREEN}9)${NC} MLflow (experimentos y runs)"
    echo -e "  ${GREEN}a)${NC} Airflow (DAGs y ejecuciones)"
    echo ""
    echo -e "  ${GREEN}0)${NC} Volver al menu principal"
    echo -e "${BOLD}${MAGENTA}============================================${NC}"
    echo ""
    echo -n "  Selecciona una opcion: "
    read subopcion

    case $subopcion in
      1) diag_pipeline_completo ;;
      2) diag_cassandra ;;
      3) diag_kafka ;;
      4) diag_websockets ;;
      5) diag_minio ;;
      6) diag_spark ;;
      7) diag_mongodb ;;
      8) diag_prometheus ;;
      9) diag_mlflow ;;
      a|A) diag_airflow ;;
      0) break ;;
      *) warn "Opcion no valida" ;;
    esac
  done
}

# ============================================================
#   LOGS
# ============================================================

menu_logs() {
  while true; do
    clear
    echo ""
    echo -e "${BOLD}${YELLOW}============================================${NC}"
    echo -e "${BOLD}${YELLOW}  LOGS -- PRACTICA BIGDATA${NC}"
    echo -e "${BOLD}${YELLOW}============================================${NC}"
    echo ""
    echo -e "  ${GREEN}1)${NC}  Flask (API + WebSockets + Kafka consumer)"
    echo -e "  ${GREEN}2)${NC}  Spark Predictor (Streaming + 4 sinks)"
    echo -e "  ${GREEN}3)${NC}  Spark Master"
    echo -e "  ${GREEN}4)${NC}  Kafka (KRaft broker)"
    echo -e "  ${GREEN}5)${NC}  MongoDB"
    echo -e "  ${GREEN}6)${NC}  Cassandra"
    echo -e "  ${GREEN}7)${NC}  MinIO"
    echo -e "  ${GREEN}8)${NC}  Airflow (scheduler + webserver)"
    echo -e "  ${GREEN}9)${NC}  MLflow"
    echo -e "  ${GREEN}a)${NC}  Prometheus"
    echo -e "  ${GREEN}b)${NC}  Grafana"
    echo -e "  ${GREEN}c)${NC}  Todos los servicios (ultimas 5 lineas c/u)"
    echo ""
    echo -e "  ${GREEN}0)${NC}  Volver"
    echo -e "${BOLD}${YELLOW}============================================${NC}"
    echo ""
    echo -n "  Selecciona servicio: "
    read logop

    LINES=50
    case $logop in
      1)
        header "LOGS -- FLASK"
        docker logs flask --tail=$LINES 2>&1
        pause ;;
      2)
        header "LOGS -- SPARK PREDICTOR"
        docker logs spark-predictor --tail=$LINES 2>&1
        pause ;;
      3)
        header "LOGS -- SPARK MASTER"
        docker logs spark-master --tail=$LINES 2>&1
        pause ;;
      4)
        header "LOGS -- KAFKA"
        docker logs kafka --tail=$LINES 2>&1
        pause ;;
      5)
        header "LOGS -- MONGODB"
        docker logs mongo --tail=$LINES 2>&1
        pause ;;
      6)
        header "LOGS -- CASSANDRA"
        docker logs cassandra --tail=$LINES 2>&1
        pause ;;
      7)
        header "LOGS -- MINIO"
        docker logs minio --tail=$LINES 2>&1
        pause ;;
      8)
        header "LOGS -- AIRFLOW"
        docker logs airflow --tail=$LINES 2>&1
        pause ;;
      9)
        header "LOGS -- MLFLOW"
        docker logs mlflow --tail=$LINES 2>&1
        pause ;;
      a|A)
        header "LOGS -- PROMETHEUS"
        docker logs prometheus --tail=$LINES 2>&1
        pause ;;
      b|B)
        header "LOGS -- GRAFANA"
        docker logs grafana --tail=$LINES 2>&1
        pause ;;
      c|C)
        header "LOGS -- TODOS LOS SERVICIOS"
        for svc in flask spark-predictor spark-master kafka mongo cassandra minio airflow mlflow prometheus grafana; do
          subheader "$svc"
          docker logs $svc --tail=5 2>&1
        done
        pause ;;
      0) break ;;
      *) warn "Opcion no valida" ;;
    esac
  done
}

# ============================================================
#   MENU PRINCIPAL
# ============================================================

while true; do
  clear
  echo ""
  echo -e "${BOLD}${CYAN}"
  echo "  ██████╗ ██╗ ██████╗     ██████╗  █████╗ ████████╗ █████╗ "
  echo "  ██╔══██╗██║██╔════╝     ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗"
  echo "  ██████╔╝██║██║  ███╗    ██║  ██║███████║   ██║   ███████║"
  echo "  ██╔══██╗██║██║   ██║    ██║  ██║██╔══██║   ██║   ██╔══██║"
  echo "  ██████╔╝██║╚██████╔╝    ██████╔╝██║  ██║   ██║   ██║  ██║"
  echo "  ╚═════╝ ╚═╝ ╚═════╝     ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝"
  echo -e "${NC}"
  echo -e "  ${BOLD}Practica Creativa Big Data -- ETSIT UPM 2026${NC}"
  echo -e "  ${CYAN}Iria Lozano y Javier Saguar${NC}"
  echo ""
  echo -e "${BOLD}${BLUE}============================================${NC}"
  echo -e "  ${BOLD}ARRANQUE${NC}"
  echo -e "  ${GREEN}1)${NC} Arrancar con Docker Compose"
  echo -e "  ${GREEN}2)${NC} Arrancar con Kubernetes (GKE)"
  echo ""
  echo -e "  ${BOLD}REENTRENAMIENTO${NC}"
  echo -e "  ${GREEN}3)${NC} Reentrenar modelo -- Docker"
  echo -e "  ${GREEN}4)${NC} Reentrenar modelo -- Kubernetes (DAG Airflow)"
  echo ""
  echo -e "  ${BOLD}GESTION${NC}"
  echo -e "  ${GREEN}5)${NC} Ver URLs actuales -- Docker"
  echo -e "  ${GREEN}6)${NC} Ver URLs actuales -- Kubernetes"
  echo -e "  ${GREEN}7)${NC} Apagar cluster GKE (ahorra dinero)"
  echo -e "  ${GREEN}8)${NC} Parar Docker Compose"
  echo ""
  echo -e "  ${BOLD}DIAGNOSTICO & LOGS${NC}"
  echo -e "  ${GREEN}9)${NC}  Diagnostico del sistema"
  echo -e "  ${GREEN}10)${NC} Ver logs por servicio"
  echo ""
  echo -e "  ${GREEN}0)${NC} Salir"
  echo -e "${BOLD}${BLUE}============================================${NC}"
  echo ""
  echo -n "  Selecciona una opcion: "
  read opcion

  case $opcion in
    1) arrancar_docker ;;
    2) arrancar_k8s ;;
    3) reentrenar_docker ;;
    4) reentrenar_k8s ;;
    5) show_urls_docker ;;
    6) show_urls_k8s ;;
    7) apagar_k8s ;;
    8) parar_docker ;;
    9) menu_diagnostico ;;
    10) menu_logs ;;
    0) echo ""; info "Hasta luego!"; echo ""; break ;;
    *) warn "Opcion no valida" ;;
  esac
done
