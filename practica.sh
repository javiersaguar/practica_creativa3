#!/bin/bash
# ============================================================
#   PRACTICA BIGDATA — SCRIPT UNIFICADO
#   ETSIT UPM 2026
# ============================================================

# Parametros fijos del entorno GKE y rutas base del proyecto.
ZONE="europe-southwest1-a"
CLUSTER="practica-k8s"
PROJECT_HOME="${PROJECT_HOME:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
MANIFESTS="${MANIFESTS:-$PROJECT_HOME/k8s-gke}"
SPARK_HOME=~/spark-4.1.1
export PROJECT_HOME

# Expone Spark y Java 17 a los comandos lanzados desde este script.
export PATH=$SPARK_HOME/bin:$PATH
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Codigos ANSI usados para dar formato uniforme a menus y mensajes.
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

# Imprime una cabecera principal con el titulo recibido.
header() {
  echo ""
  echo -e "${BOLD}${BLUE}============================================${NC}"
  echo -e "${BOLD}${BLUE}  $1${NC}"
  echo -e "${BOLD}${BLUE}============================================${NC}"
  echo ""
}

# Imprime un separador secundario dentro de un diagnostico.
subheader() {
  echo ""
  echo -e "${BOLD}${CYAN}  -- $1 --${NC}"
  echo ""
}

# Mensajes breves reutilizados para estados, avisos y comprobaciones.
ok()   { echo -e "  ${GREEN}OK${NC} $1"; }
info() { echo -e "  ${CYAN}->${NC} $1"; }
warn() { echo -e "  ${YELLOW}!!${NC} $1"; }
err()  { echo -e "  ${RED}XX${NC} $1"; }
pass_check() { echo -e "  ${GREEN}✓${NC} $1"; }
fail_check() { echo -e "  ${RED}✗${NC} $1"; }

# Detiene el menu hasta que el usuario pulse ENTER.
pause() {
  echo ""
  echo -n "  Pulsa ENTER para continuar..."
  read
}

# Obtiene la IP publica de la VM y muestra los puertos publicados por Docker.
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

# Obtiene las IP de GKE y construye las URLs expuestas por LoadBalancer/NodePort.
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
  echo -e "  ${BOLD}Spark UI:${NC}           http://$NODE_IP:30880"
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

  # Recompila el assembly solo cuando falta o alguna fuente Scala es mas reciente.
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

  # Levanta primero los servicios base definidos en docker-compose.yml.
  info "Levantando contenedores..."
  docker compose up -d

  # Da tiempo a los servicios con inicializacion lenta antes de consultarlos.
  info "Esperando inicializacion (30s)..."
  sleep 30

  # Cassandra no acepta CQL inmediatamente; se espera hasta obtener respuesta.
  info "Esperando a que Cassandra este lista..."
  until docker exec cassandra cqlsh -e "describe keyspaces" > /dev/null 2>&1; do
    echo "    Cassandra aun arrancando, esperando 10s..."
    sleep 10
  done
  ok "Cassandra lista"

  info "Spark predictor se creara cuando MinIO tenga los modelos..."

  # Reinicia Flask una vez disponibles Kafka y Cassandra para renovar conexiones.
  info "Reiniciando Flask..."
  docker compose stop flask
  docker compose start flask

  info "Esperando inicializacion final (20s)..."
  sleep 20

  # Crea el usuario administrador si Airflow aun no lo tiene.
  info "Configurando Airflow..."
  docker exec airflow airflow users create \
    --username admin --password admin \
    --firstname Admin --lastname Admin \
    --role Admin --email admin@example.com 2>/dev/null || true

  # Registra un alias local de MinIO y crea el bucket principal de forma idempotente.
  info "Configurando MinIO bucket..."
  docker exec minio sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin && mc mb local/flight-data 2>/dev/null || true" 2>/dev/null

  # Copia el JAR compilado al directorio compartido que montan los nodos Spark.
  info "Asegurando JAR en shared-jars..."
  mkdir -p "$PROJECT_HOME/shared-jars"
  cp "$PROJECT_HOME/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar"      "$PROJECT_HOME/shared-jars/flight_prediction_2.13-0.1.jar" 2>/dev/null     && ok "JAR copiado a shared-jars" || warn "JAR no encontrado en target"

  # Prepara el esquema evaluable: distancias por ruta y resultados de prediccion.
  info "Creando keyspace y tablas en Cassandra..."
  docker exec cassandra cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS agile_data_science
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS agile_data_science.origin_dest_distances (
  origin TEXT, dest TEXT, distance DOUBLE, PRIMARY KEY (origin, dest));
DROP TABLE IF EXISTS agile_data_science.flight_delay_classification_response;
CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_classification_response (
  uuid TEXT PRIMARY KEY,
  origin TEXT,
  dayofweek INT,
  dayofyear INT,
  dayofmonth INT,
  dest TEXT,
  depdelay DOUBLE,
  timestamp TIMESTAMP,
  flightdate DATE,
  carrier TEXT,
  distance DOUBLE,
  route TEXT,
  prediction DOUBLE);" 2>/dev/null && ok "Keyspace creado" || warn "Error Cassandra"

  # Convierte el JSONL de distancias en sentencias CQL y las importa en bloque.
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

  # Reinicia Spark para asegurar que todos los procesos ven el JAR compartido actual.
  info "Reiniciando workers para montar shared-jars..."
  docker compose restart spark-master spark-worker-1 spark-worker-2
  sleep 15

  # Copia el dataset comprimido al bucket S3 que alimenta el Lakehouse.
  info "Subiendo datos de entrenamiento a MinIO..."
  docker cp "$PROJECT_HOME/data/simple_flight_delay_features.jsonl.bz2" minio:/tmp/ 2>/dev/null
  docker exec minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin && mc cp /tmp/simple_flight_delay_features.jsonl.bz2 local/flight-data/data/simple_flight_delay_features.jsonl.bz2 2>/dev/null"     && ok "Datos subidos a MinIO" || warn "Error subiendo datos"

  # Genera un job PySpark temporal que crea el namespace y la tabla Iceberg.
  # Las config sql.* activan Iceberg; las hadoop.fs.s3a.* conectan Spark con MinIO.
  info "Creando tabla Iceberg (2-3 min)..."
  cat > /tmp/load_iceberg.py << 'PY'
from pyspark.sql import SparkSession

# Crea una sesion con catalogo Iceberg sobre el endpoint S3 de MinIO.
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

# Lee el dataset original y reemplaza la tabla de entrenamiento.
df = spark.read.json("s3a://flight-data/data/simple_flight_delay_features.jsonl.bz2")
spark.sql("CREATE NAMESPACE IF NOT EXISTS minio.flights")
spark.sql("DROP TABLE IF EXISTS minio.flights.training_data")
df.writeTo("minio.flights.training_data").create()
print("Registros Iceberg:", spark.table("minio.flights.training_data").count())
spark.stop()
PY
  docker cp /tmp/load_iceberg.py spark-master:/tmp/load_iceberg.py
  # --packages descarga Iceberg y el conector S3A; spark.jars.ivy fija su cache.
  docker exec spark-master bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.367 --conf spark.jars.ivy=/home/spark/.ivy2 /tmp/load_iceberg.py" 2>&1 | grep -E "Registros|ERROR" | tail -3 && ok "Tabla Iceberg creada" || warn "Error Iceberg"

  # Envia TrainModel al master Standalone y ejecuta el driver dentro de un worker.
  # waitAppCompletion mantiene spark-submit esperando hasta que termine el entrenamiento.
  # driver.cores/memory reservan 1 core y 1 GiB para el driver.
  # executor.instances/cores/memory crean un executor de 1 core y 1 GiB.
  # cores.max limita a 2 los cores totales que puede reservar esta aplicacion.
  # jars.ivy fija la cache de dependencias descargadas por Spark.
  # driverEnv.* entrega al driver las URLs de MLflow, modelos, tabla y credenciales.
  # extraJavaOptions replica esos valores como propiedades Java para el codigo Scala.
  # hadoop.fs.s3a.* configura endpoint, credenciales, implementacion y HTTP de MinIO.
  # sql.extensions/catalog.* registra el catalogo Iceberg respaldado por S3A.
  info "Entrenando modelo TrainModel en deploy-mode cluster (3-4 min)..."
  docker exec spark-master /opt/spark/bin/spark-submit \
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
    --conf 'spark.driver.extraJavaOptions=-DMLFLOW_TRACKING_URI=http://mlflow:5000 -DTRAINING_TABLE=minio.flights.training_data -DMODEL_BASE_PATH=s3a://flight-data/models -DS3_ENDPOINT=http://minio:9000 -DAWS_ACCESS_KEY_ID=minioadmin -DAWS_SECRET_ACCESS_KEY=minioadmin' \
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
    file:///shared-jars/flight_prediction_2.13-0.1.jar \
    && ok "Modelo entrenado" || warn "Error entrenamiento (ver logs del worker)"

  # Activa el perfil opcional que envia MakePrediction al cluster Spark.
  info "Arrancando Spark predictor en modo cluster..."
  docker compose --profile predictor up -d spark-predictor
  ok "Predictor enviado al cluster"

  # Deja disponible el DAG para reentrenamientos posteriores desde Airflow.
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

  # Situa la ejecucion en el proyecto y habilita el plugin de autenticacion de GKE.
  cd "$PROJECT_HOME"
  export USE_GKE_GCLOUD_AUTH_PLUGIN=True
  export PATH="/usr/local/bin:/tmp:$PATH"

  # Descarga kubectl si la VM no lo tiene instalado.
  info "Asegurando kubectl al inicio..."
  if ! command -v kubectl >/dev/null 2>&1; then
    KUBECTL_VERSION=$(curl -L -s https://dl.k8s.io/release/stable.txt 2>/dev/null || echo "v1.35.0")
    curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" -o /tmp/kubectl || {
      err "No se pudo descargar kubectl"
      return 1
    }
    chmod +x /tmp/kubectl
    sudo install -m 755 /tmp/kubectl /usr/local/bin/kubectl 2>/dev/null || export PATH="/tmp:$PATH"
  fi
  ok "kubectl: $(command -v kubectl)"

  # Instala un shim compatible con ExecCredential cuando falta el plugin oficial de GKE.
  info "Asegurando gke-gcloud-auth-plugin..."
  if ! command -v gke-gcloud-auth-plugin >/dev/null 2>&1; then
    cat > /tmp/gke-gcloud-auth-plugin << 'SHIM'
#!/usr/bin/env bash
set -euo pipefail

# Implementa la respuesta ExecCredential que kubectl espera del plugin de GKE.
if [[ "${1:-}" == "--version" ]]; then
  echo "gke-gcloud-auth-plugin shim 1.0.0"
  exit 0
fi
GCLOUD_BIN="${GCLOUD_BIN:-gcloud}"
TOKEN="$("$GCLOUD_BIN" auth print-access-token)"
EXPIRATION="$("$GCLOUD_BIN" config config-helper --format='value(credential.token_expiry)' 2>/dev/null || true)"

# Incluye la caducidad cuando gcloud puede proporcionarla.
if [[ -n "$EXPIRATION" ]]; then
  printf '{"kind":"ExecCredential","apiVersion":"client.authentication.k8s.io/v1beta1","status":{"expirationTimestamp":"%s","token":"%s"}}\n' "$EXPIRATION" "$TOKEN"
else
  printf '{"kind":"ExecCredential","apiVersion":"client.authentication.k8s.io/v1beta1","status":{"token":"%s"}}\n' "$TOKEN"
fi
SHIM
    chmod +x /tmp/gke-gcloud-auth-plugin
    sudo install -m 755 /tmp/gke-gcloud-auth-plugin /usr/local/bin/gke-gcloud-auth-plugin 2>/dev/null || export PATH="/tmp:$PATH"
  fi
  gke-gcloud-auth-plugin --version >/dev/null 2>&1 \
    && ok "gke-gcloud-auth-plugin: $(command -v gke-gcloud-auth-plugin)" \
    || { err "gke-gcloud-auth-plugin no funciona"; return 1; }

  # Comprueba que gcloud tenga proyecto y una cuenta de usuario utilizables.
  PROJECT_ID=$(gcloud config get-value project 2>/dev/null || true)
  if [ -z "$PROJECT_ID" ]; then
    err "gcloud no tiene proyecto configurado"
    echo "  Ejecuta: gcloud config set project PROJECT_ID"
    return 1
  fi

  ACTIVE_ACCOUNT=$(gcloud config get-value account 2>/dev/null || true)
  USER_ACCOUNT=$(gcloud auth list --format='value(account)' 2>/dev/null | grep -v 'developer.gserviceaccount.com' | grep '@' | head -n 1 || true)
  if [[ "$ACTIVE_ACCOUNT" == *developer.gserviceaccount.com ]] && [ -n "$USER_ACCOUNT" ]; then
    info "Cambiando gcloud a cuenta de usuario: $USER_ACCOUNT"
    gcloud config set account "$USER_ACCOUNT" >/dev/null
    ACTIVE_ACCOUNT="$USER_ACCOUNT"
  fi
  if [[ "$ACTIVE_ACCOUNT" == *developer.gserviceaccount.com ]] || [ -z "$ACTIVE_ACCOUNT" ]; then
    err "Solo hay credenciales de service account con scopes insuficientes"
    echo "  En la consola GCloud abre:"
    echo "  https://console.cloud.google.com/kubernetes/clusters/details/$ZONE/$CLUSTER/details?project=$PROJECT_ID"
    echo "  Luego en esta VM ejecuta una sola vez:"
    echo "  gcloud auth login --no-launch-browser"
    echo "  gcloud config set account TU_USUARIO"
    echo "  gcloud container clusters get-credentials $CLUSTER --zone $ZONE --project $PROJECT_ID"
    return 1
  fi
  ok "gcloud activo: $ACTIVE_ACCOUNT / $PROJECT_ID"

  # Regenera kubeconfig y valida el acceso al API server de Kubernetes.
  info "Autenticando con el cluster..."
  gcloud container clusters get-credentials "$CLUSTER" --zone "$ZONE" --project "$PROJECT_ID" || {
    err "No se pudo generar kubeconfig para $CLUSTER"
    return 1
  }
  kubectl cluster-info >/dev/null || {
    err "kubectl no puede conectar con el API server"
    return 1
  }
  ok "Cluster GKE accesible"

  # Detiene temporalmente el submitter del predictor durante el bootstrap.
  kubectl scale deployment/spark-predictor --replicas=0 2>/dev/null || true

  # Escala el node pool y usa un nodo como fallback si la cuota no permite dos.
  TARGET_NODES="${K8S_NODE_COUNT:-2}"
  NODE_COUNT=$(kubectl get nodes --no-headers 2>/dev/null | sed '/^$/d' | wc -l)
  if [ "$NODE_COUNT" -lt "$TARGET_NODES" ]; then
    info "Escalando cluster a $TARGET_NODES nodos..."
    if ! gcloud container clusters resize "$CLUSTER" --num-nodes="$TARGET_NODES" --zone "$ZONE" --project "$PROJECT_ID" --quiet; then
      if [ "$TARGET_NODES" -gt "1" ]; then
        warn "No hay cuota para $TARGET_NODES nodos; intentando fallback a 1 nodo"
        TARGET_NODES=1
        gcloud container clusters resize "$CLUSTER" --num-nodes="$TARGET_NODES" --zone "$ZONE" --project "$PROJECT_ID" --quiet || {
          err "No se pudo escalar el cluster ni siquiera a 1 nodo"
          echo "  Abre:"
          echo "  https://console.cloud.google.com/kubernetes/clusters/details/$ZONE/$CLUSTER/nodes?project=$PROJECT_ID"
          echo "  O libera/aumenta cuota regional SSD_TOTAL_GB:"
          echo "  https://console.cloud.google.com/iam-admin/quotas?usage=USED&project=$PROJECT_ID"
          echo "  Despues relanza: ./practica.sh -> opcion 2"
          return 1
        }
      else
        err "No se pudo escalar el cluster"
        echo "  Abre:"
        echo "  https://console.cloud.google.com/kubernetes/clusters/details/$ZONE/$CLUSTER/nodes?project=$PROJECT_ID"
        echo "  O libera/aumenta cuota regional SSD_TOTAL_GB:"
        echo "  https://console.cloud.google.com/iam-admin/quotas?usage=USED&project=$PROJECT_ID"
        echo "  Despues relanza: ./practica.sh -> opcion 2"
        return 1
      fi
    fi
    info "Esperando a que aparezcan $TARGET_NODES nodos..."
    for _ in {1..60}; do
      NODE_COUNT=$(kubectl get nodes --no-headers 2>/dev/null | sed '/^$/d' | wc -l)
      [ "$NODE_COUNT" -ge "$TARGET_NODES" ] && break
      sleep 10
    done
  fi
  kubectl wait --for=condition=Ready nodes --all --timeout=300s
  NODE_COUNT=$(kubectl get nodes --no-headers 2>/dev/null | sed '/^$/d' | wc -l)
  ok "Cluster con $NODE_COUNT nodos Ready"

  # Calcula la URL regional de Artifact Registry usada por las imagenes del despliegue.
  AR_LOCATION="${ZONE%-*}"
  REGISTRY="${AR_LOCATION}-docker.pkg.dev/${PROJECT_ID}/practica"
  # Reconstruye y publica las imagenes que contienen codigo o configuracion del proyecto.
  if command -v docker >/dev/null 2>&1; then
    info "Publicando imagenes K8s en Artifact Registry..."
    gcloud auth configure-docker "${AR_LOCATION}-docker.pkg.dev" --quiet >/dev/null
    docker build \
      -t "$REGISTRY/spark-master:latest" \
      -t "$REGISTRY/spark-worker:latest" \
      -t "$REGISTRY/spark-predictor:latest" \
      "$PROJECT_HOME/docker/spark" \
      && docker push "$REGISTRY/spark-master:latest" \
      && docker push "$REGISTRY/spark-worker:latest" \
      && docker push "$REGISTRY/spark-predictor:latest" \
      && ok "Imagenes Spark publicadas con el JAR actual" \
      || { err "Error publicando imagenes Spark"; return 1; }

    docker build -t "$REGISTRY/kafka:latest" "$PROJECT_HOME/docker/kafka" \
      && docker push "$REGISTRY/kafka:latest" \
      && ok "Imagen Kafka publicada" \
      || { err "Error publicando imagen Kafka"; return 1; }

    docker build -f "$PROJECT_HOME/docker/flask/Dockerfile" -t "$REGISTRY/flask:latest" "$PROJECT_HOME" \
      && docker push "$REGISTRY/flask:latest" \
      && ok "Imagen Flask publicada" \
      || { err "Error publicando imagen Flask"; return 1; }
  else
    warn "docker no esta disponible; se usaran las imagenes ya publicadas en $REGISTRY"
  fi

  # Publica el DAG como ConfigMap para montarlo dentro del pod de Airflow.
  info "Creando ConfigMap del DAG de Airflow..."
  kubectl create configmap airflow-dags \
    --from-file=retrain_model.py="$PROJECT_HOME/docker/airflow/dags/retrain_model.py" \
    --dry-run=client -o yaml | kubectl apply -f - 2>/dev/null \
    && ok "ConfigMap airflow-dags creado" || { err "Error creando ConfigMap"; return 1; }

  # Aplica todos los servicios base definidos en los manifests de GKE.
  info "Aplicando manifests base..."
  for manifest in mongo cassandra minio kafka spark flask prometheus grafana mlflow airflow; do
    kubectl apply -f "$MANIFESTS/$manifest.yaml" || return 1
  done

  # Espera cada rollout para no inicializar datos antes de que los pods esten disponibles.
  info "Esperando deployments base..."
  kubectl rollout status deployment/mongo --timeout=180s || true
  kubectl rollout status deployment/minio --timeout=180s || true
  kubectl rollout status deployment/kafka --timeout=240s || true
  kubectl rollout status deployment/spark-master --timeout=240s || true
  kubectl rollout status deployment/spark-worker --timeout=240s || true
  kubectl rollout status deployment/mlflow --timeout=180s || true
  kubectl rollout status deployment/flask --timeout=180s || true
  kubectl rollout status deployment/airflow --timeout=240s || true

  # Crea el bucket, carga el dataset y sincroniza los modelos locales con MinIO.
  info "Configurando MinIO y subiendo datos..."
  kubectl wait --for=condition=Ready pod -l app=minio --timeout=180s
  MINIO_POD=$(kubectl get pod -l app=minio -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  kubectl exec "$MINIO_POD" -- sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null && mc mb -p local/flight-data 2>/dev/null || true"
  kubectl exec -i "$MINIO_POD" -- sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null && mc pipe local/flight-data/data/simple_flight_delay_features.jsonl.bz2" \
    < "$PROJECT_HOME/data/simple_flight_delay_features.jsonl.bz2"
  # Conserva en MinIO la misma estructura relativa que existe bajo models/.
  find "$PROJECT_HOME/models" -type f | while IFS= read -r f; do
    REL="${f#$PROJECT_HOME/models/}"
    kubectl exec -i "$MINIO_POD" -- sh -c \
      "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null && mc pipe \"local/flight-data/models/$REL\"" \
      < "$f" >/dev/null 2>&1 || true
  done
  # Elimina el estado anterior del streaming para iniciar el predictor desde cero.
  kubectl exec "$MINIO_POD" -- sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null && mc rm --recursive --force local/flight-data/checkpoints/predictor 2>/dev/null || true"
  ok "MinIO listo con datos de entrenamiento"

  info "Esperando Cassandra lista..."
  kubectl wait --for=condition=Ready pod -l app=cassandra --timeout=240s
  sleep 10

  # Crea el esquema evaluable: distancias de rutas y resultados de prediccion.
  info "Creando keyspace y tablas en Cassandra..."
  kubectl exec deployment/cassandra -- cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS agile_data_science
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS agile_data_science.origin_dest_distances (
  origin TEXT, dest TEXT, distance DOUBLE, PRIMARY KEY (origin, dest));
DROP TABLE IF EXISTS agile_data_science.flight_delay_classification_response;
CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_classification_response (
  uuid TEXT PRIMARY KEY,
  origin TEXT,
  dayofweek INT,
  dayofyear INT,
  dayofmonth INT,
  dest TEXT,
  depdelay DOUBLE,
  timestamp TIMESTAMP,
  flightdate DATE,
  carrier TEXT,
  distance DOUBLE,
  route TEXT,
  prediction DOUBLE);" \
    && ok "Keyspace y tablas creados" || { err "Error creando tablas Cassandra"; return 1; }

  # Convierte el JSONL de distancias en sentencias CQL y las ejecuta en Cassandra.
  info "Importando distancias en Cassandra..."
  python3 << 'PYEOF'
import json, os
lines = []

# Convierte cada ruta del JSONL en un INSERT para la carga posterior.
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
  kubectl exec -i deployment/cassandra -- cqlsh < /tmp/distances_k8s.cql \
    && ok "Distancias importadas" || { err "Error importando distancias"; return 1; }

  # Confirma que la imagen Spark contiene el artefacto Scala necesario.
  info "Verificando JAR en Spark..."
  kubectl wait --for=condition=Ready pod -l app=spark-master --timeout=180s
  SPARK_POD=$(kubectl get pod -l app=spark-master -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  kubectl exec "$SPARK_POD" -- test -f /app/jars/flight_prediction_2.13-0.1.jar \
    && ok "JAR Scala presente en /app/jars" \
    || { err "No se encuentra el JAR en /app/jars"; return 1; }

  # Usa las APIs REST del master para retirar entrenamientos o predictores anteriores.
  info "Deteniendo drivers Spark activos antes del bootstrap..."
  kubectl exec -i "$SPARK_POD" -- python3 <<'PYKILL'
import json, time, urllib.request

MASTER_JSON = "http://spark-master:8080/json/"
KILL_URL = "http://spark-master:6066/v1/submissions/kill/"

def get_master():
    with urllib.request.urlopen(MASTER_JSON, timeout=10) as r:
        return json.load(r)

def kill_driver(driver_id):
    # Solicita al master Standalone la terminacion del driver.
    req = urllib.request.Request(KILL_URL + driver_id, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            print("kill", driver_id, r.read().decode("utf-8", "ignore"))
    except Exception as exc:
        print("kill failed", driver_id, exc)

# Detiene solo aplicaciones de esta practica y conserva otros posibles drivers.
for d in get_master().get("activedrivers", []):
    mainclass = d.get("mainclass", "")
    if "MakePrediction" in mainclass or "TrainModel" in mainclass:
        print("Stopping", d["id"], mainclass, d.get("state"))
        kill_driver(d["id"])

# Espera hasta confirmar que el master ya no anuncia esos drivers.
for _ in range(30):
    time.sleep(2)
    busy = [
        d for d in get_master().get("activedrivers", [])
        if "MakePrediction" in d.get("mainclass", "") or "TrainModel" in d.get("mainclass", "")
    ]
    if not busy:
        print("Spark drivers cleared")
        break
    print("Waiting:", [(d.get("id"), d.get("state")) for d in busy])
else:
    raise RuntimeError("Spark drivers did not stop in time")
PYKILL

  # Compila un helper Java que transforma el dataset S3A en una tabla Iceberg.
  info "Creando tabla Iceberg en MinIO..."
  cat > /tmp/LoadIcebergK8s.java << 'JAVA'
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LoadIcebergK8s {
  public static void main(String[] args) throws Exception {
    // Configura Spark para usar Iceberg y MinIO como almacenamiento S3A.
    SparkSession spark = SparkSession.builder()
      .appName("load-iceberg-k8s")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.minio", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.minio.type", "hadoop")
      .config("spark.sql.catalog.minio.warehouse", "s3a://flight-data/warehouse")
      .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
      .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate();

    // Recrea la tabla de entrenamiento desde el JSON comprimido del bucket.
    Dataset<Row> df = spark.read().json("s3a://flight-data/data/simple_flight_delay_features.jsonl.bz2");
    spark.sql("CREATE NAMESPACE IF NOT EXISTS minio.flights");
    spark.sql("DROP TABLE IF EXISTS minio.flights.training_data");
    df.writeTo("minio.flights.training_data").create();
    System.out.println("Registros Iceberg: " + spark.table("minio.flights.training_data").count());
    spark.stop();
  }
}
JAVA
  # Compila el helper en spark-master y lo distribuye a todos los workers.
  kubectl cp /tmp/LoadIcebergK8s.java "$SPARK_POD":/tmp/LoadIcebergK8s.java
  kubectl exec "$SPARK_POD" -- bash -lc \
    "rm -rf /tmp/load_iceberg_classes /tmp/load-iceberg-k8s.jar && mkdir -p /tmp/load_iceberg_classes && javac -cp '/opt/spark/jars/*' -d /tmp/load_iceberg_classes /tmp/LoadIcebergK8s.java && jar cf /tmp/load-iceberg-k8s.jar -C /tmp/load_iceberg_classes ." \
    || { err "Error compilando helper Iceberg"; return 1; }
  kubectl cp "$SPARK_POD":/tmp/load-iceberg-k8s.jar /tmp/load-iceberg-k8s.jar \
    || { err "Error copiando helper Iceberg desde spark-master"; return 1; }
  kubectl cp /tmp/load-iceberg-k8s.jar "$SPARK_POD":/tmp/load-iceberg-k8s.jar \
    || { err "Error copiando helper Iceberg a spark-master"; return 1; }
  for WORKER_POD in $(kubectl get pod -l app=spark-worker -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}' 2>/dev/null); do
    kubectl cp /tmp/load-iceberg-k8s.jar "$WORKER_POD":/tmp/load-iceberg-k8s.jar
  done
  # waitAppCompletion espera el resultado del driver; driver/executor fijan sus recursos.
  # cores.max limita el total del job y jars.ivy define la cache de dependencias.
  # fs.s3a.* configura MinIO como S3 sin TLS y con acceso path-style.
  # sql.extensions/catalog.* activa Iceberg y apunta su warehouse al bucket flight-data.
  kubectl exec "$SPARK_POD" -- /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode cluster \
    --conf spark.standalone.submit.waitAppCompletion=true \
    --conf spark.driver.cores=1 \
    --conf spark.driver.memory=1g \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=1g \
    --conf spark.cores.max=2 \
    --conf spark.jars.ivy=/home/spark/.ivy2 \
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
    --class LoadIcebergK8s \
    file:///tmp/load-iceberg-k8s.jar \
    && ok "Tabla Iceberg creada" || { err "Error creando tabla Iceberg"; return 1; }

  # Ejecuta TrainModel en cluster usando Iceberg como entrada, MinIO para modelos y MLflow.
  # driverEnv.* y extraJavaOptions propagan la configuracion al driver remoto del worker.
  # executor.* y cores.max reservan recursos; fs.s3a.* e Iceberg acceden al Lakehouse.
  info "Entrenando modelo TrainModel en K8s (deploy-mode cluster)..."
  kubectl exec "$SPARK_POD" -- bash -lc '
MLFLOW_TRACKING_URI=http://mlflow:5000 \
MODEL_BASE_PATH=s3a://flight-data/models \
TRAINING_TABLE=minio.flights.training_data \
S3_ENDPOINT=http://minio:9000 \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
/opt/spark/bin/spark-submit \
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
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.minio=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.minio.type=hadoop \
  --conf spark.sql.catalog.minio.warehouse=s3a://flight-data/warehouse \
  file:///app/jars/flight_prediction_2.13-0.1.jar
' \
    && ok "Modelo entrenado y registrado en MLflow" || { err "Error entrenando modelo"; return 1; }

  # Elimina drivers MakePrediction antiguos antes de crear un unico predictor nuevo.
  info "Arrancando Spark predictor en K8s..."
  kubectl scale deployment/spark-predictor --replicas=0 2>/dev/null || true
  kubectl exec "$SPARK_POD" -- python3 <<'PY'
import json
import time
import urllib.request

MASTER_JSON = "http://spark-master:8080/json/"
KILL_URL = "http://spark-master:6066/v1/submissions/kill/"

def get_master():
    with urllib.request.urlopen(MASTER_JSON, timeout=10) as r:
        return json.load(r)

def kill_driver(driver_id):
    # Envia la orden REST de parada al master Spark.
    req = urllib.request.Request(KILL_URL + driver_id, method="POST")
    with urllib.request.urlopen(req, timeout=10) as r:
        print(r.read().decode("utf-8", "ignore"))

# Retira cualquier predictor anterior para evitar consumidores duplicados.
for d in get_master().get("activedrivers", []):
    if "MakePrediction" in d.get("mainclass", ""):
        print("Stopping old predictor driver", d["id"], d.get("state"))
        kill_driver(d["id"])

# Comprueba que desaparezca antes de escalar el nuevo submitter.
for _ in range(30):
    time.sleep(2)
    active = [
        d for d in get_master().get("activedrivers", [])
        if "MakePrediction" in d.get("mainclass", "")
    ]
    if not active:
        print("Predictor drivers cleared")
        break
    print("Waiting:", [(d.get("id"), d.get("state")) for d in active])
else:
    raise RuntimeError("Predictor drivers did not stop in time")
PY
  # Aplica la configuracion del predictor, lo escala y comprueba sus logs iniciales.
  kubectl apply -f "$MANIFESTS/spark-predictor-patch.yaml"
  kubectl scale deployment/spark-predictor --replicas=1
  kubectl rollout status deployment/spark-predictor --timeout=240s || true
  sleep 30
  PREDICTOR_POD=$(kubectl get pod -l app=spark-predictor --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  if [ -z "$PREDICTOR_POD" ]; then
    err "spark-predictor no esta Running"
    kubectl get pods -o wide
    return 1
  fi
  ASSERT_ERR=$(kubectl logs "$PREDICTOR_POD" --tail=200 2>/dev/null | grep -c "AssertionError\|Decision Tree load failed\|Exception" 2>/dev/null || true)
  if [ "$ASSERT_ERR" -gt "0" ]; then
    warn "El predictor tiene errores en logs recientes"
    kubectl logs "$PREDICTOR_POD" --tail=120
  else
    ok "Predictor K8s arrancado correctamente"
  fi

  # Copia kubectl al pod para que el DAG pueda enviar trabajos Spark al mismo cluster.
  info "Configurando Airflow (kubectl + DAG unpause)..."
  AIRFLOW_POD=$(kubectl get pod -l app=airflow --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  if [ -n "$AIRFLOW_POD" ]; then
    KUBECTL_BIN=$(command -v kubectl)
    kubectl cp "$KUBECTL_BIN" "$AIRFLOW_POD":/tmp/kubectl 2>/dev/null || true
    kubectl exec "$AIRFLOW_POD" -- chmod +x /tmp/kubectl 2>/dev/null || true
    kubectl exec "$AIRFLOW_POD" -- airflow dags unpause retrain_flight_delay_model 2>/dev/null || true
    ok "Airflow configurado"
  else
    warn "Airflow no esta Running; revisa kubectl get pods"
  fi

  # Muestra el estado final para detectar pods pendientes o reinicios.
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

# Desbloquea y dispara manualmente el DAG de entrenamiento en Docker.
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

# Prepara el pod de Airflow y dispara el mismo DAG dentro de Kubernetes.
reentrenar_k8s() {
  header "REENTRENAMIENTO -- KUBERNETES"
  warn "El reentrenamiento en K8s ejecuta el DAG de Airflow"

  # Actualiza las credenciales locales antes de buscar los pods.
  info "Verificando conexion al cluster..."
  gcloud container clusters get-credentials $CLUSTER --zone $ZONE 2>/dev/null

  AIRFLOW_POD=$(kubectl get pod -l app=airflow --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  SPARK_POD=$(kubectl get pod -l app=spark-master --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

  # Airflow es obligatorio para ejecutar el DAG.
  if [ -z "$AIRFLOW_POD" ]; then
    err "Pod de Airflow no encontrado. Esta el cluster arrancado?"
    return 1
  fi

  # Inyecta kubectl en Airflow para que las tareas del DAG controlen recursos K8s.
  info "Asegurando kubectl y script de entrenamiento..."
  if [ ! -f /tmp/kubectl ]; then
    curl -sL "https://dl.k8s.io/release/v1.29.0/bin/linux/amd64/kubectl" -o /tmp/kubectl
    chmod +x /tmp/kubectl
  fi
  kubectl cp /tmp/kubectl $AIRFLOW_POD:/tmp/kubectl 2>/dev/null || true
  kubectl exec $AIRFLOW_POD -- chmod +x /tmp/kubectl 2>/dev/null || true

  # Verifica el JAR de entrenamiento si el master Spark esta disponible.
  if [ -n "$SPARK_POD" ]; then
    kubectl exec $SPARK_POD -- test -f /app/jars/flight_prediction_2.13-0.1.jar 2>/dev/null \
      || warn "No se encuentra el JAR en /app/jars dentro de spark-master"
  fi

  # Arranca el scheduler en segundo plano si todavia no estaba ejecutandose.
  info "Asegurando scheduler activo..."
  kubectl exec $AIRFLOW_POD -- bash -c "nohup airflow scheduler >> /tmp/scheduler.log 2>&1 &" 2>/dev/null || true
  sleep 3

  # Habilita el DAG y crea una nueva ejecucion.
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

# Reduce el cluster a cero nodos para detener el consumo de computacion.
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

# Detiene los contenedores sin eliminar sus volumenes persistentes.
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

# Inspecciona esquema, distancias y predicciones persistidas en Cassandra.
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

# Revisa topics, mensajes recientes y el consumer group usado por Flask.
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

# Comprueba el contenido del bucket, los modelos y los metadatos Iceberg.
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

# Resume el estado del predictor, sus micro-batches y los workers Spark.
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

# Consulta la copia de predicciones mantenida en MongoDB.
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

# Valida targets, metricas del pipeline y la carga del dashboard Grafana.
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

# Ejecuta una comprobacion rapida de disponibilidad de todos los componentes Docker.
diag_pipeline_completo() {
  header "DIAGNOSTICO -- PIPELINE COMPLETO"

  subheader "Estado de todos los contenedores"
  docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null

  subheader "Verificacion end-to-end"
  echo ""

  # Comprueba que la pagina de prediccion de Flask responde.
  FLASK_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5001/flights/delays/predict_kafka 2>/dev/null)
  [ "$FLASK_STATUS" = "200" ] && ok "Flask responde (HTTP $FLASK_STATUS)" || err "Flask no responde (HTTP $FLASK_STATUS)"

  # Verifica los topics de entrada y salida del streaming.
  TOPICS=$(docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | tr '\n' ' ')
  echo "$TOPICS" | grep -q "flight-delay-ml-request" && ok "Kafka topic request existe" || err "Topic request no encontrado"
  echo "$TOPICS" | grep -q "flight-delay-ml-response" && ok "Kafka topic response existe" || err "Topic response no encontrado"

  # Comprueba datos de referencia y resultados almacenados en las bases de datos.
  CASS_COUNT=$(docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM agile_data_science.origin_dest_distances;" 2>/dev/null | grep -E "[0-9]+" | tr -d ' ')
  [ "$CASS_COUNT" = "4696" ] && ok "Cassandra: 4696 distancias cargadas" || warn "Cassandra: $CASS_COUNT distancias (esperado 4696)"

  MONGO_COUNT=$(docker exec mongo mongosh --quiet agile_data_science \
    --eval "db.flight_delay_ml_response.countDocuments()" 2>/dev/null)
  ok "MongoDB: $MONGO_COUNT predicciones almacenadas"

  # Confirma que MinIO contiene artefactos de modelo.
  MODELS=$(docker exec minio sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null && mc ls local/flight-data/models/" 2>/dev/null | wc -l)
  [ "$MODELS" -gt "0" ] && ok "MinIO: modelos disponibles ($MODELS ficheros)" || err "MinIO: no hay modelos"

  # Valida la salud de Prometheus y la exposicion de metricas en Flask.
  PROM_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9090/-/healthy 2>/dev/null)
  [ "$PROM_STATUS" = "200" ] && ok "Prometheus healthy" || err "Prometheus no responde"

  FLASK_METRICS=$(curl -s http://localhost:5001/metrics 2>/dev/null | grep -c "flight_requests_total")
  [ "$FLASK_METRICS" -gt "0" ] && ok "Flask /metrics activo (prometheus-flask-exporter)" || err "Flask /metrics no disponible"

  # Busca en logs la confirmacion de que el streaming ha arrancado.
  SPARK_RUNNING=$(docker logs spark-predictor 2>&1 | grep -c "Stream started" 2>/dev/null)
  [ "$SPARK_RUNNING" -gt "0" ] && ok "Spark Streaming activo ($SPARK_RUNNING streams)" || warn "Spark Streaming: verificar logs"

  pause
}

# Genera un cliente Python que valida handshake Socket.IO y recepcion por WebSocket real.
websocket_probe_code() {
  cat <<'PY'
import json
import sys
import time
import urllib.parse
import urllib.request

from simple_websocket import Client

HTTP_BASE = "http://localhost:5001"
WS_URL = "ws://localhost:5001/socket.io/?EIO=4&transport=websocket"


def receive(ws, timeout=1):
    # Mantiene compatibilidad con versiones que no aceptan timeout.
    try:
        return ws.receive(timeout=timeout)
    except TypeError:
        return ws.receive()


def handle_socket_packet(ws, packet):
    # Procesa apertura, ping/pong y eventos Socket.IO sobre Engine.IO.
    if not isinstance(packet, str):
        return None

    if packet.startswith("0"):
        ws.send("40")
        return None

    if packet == "2":
        ws.send("3")
        return None

    if packet.startswith("42"):
        event = json.loads(packet[2:])
        if event and event[0] == "prediction_response":
            return event[1]

    return None


# Abre directamente el transporte WebSocket, sin fallback a polling.
try:
    ws = Client.connect(WS_URL)
except Exception as exc:
    print("  ERROR No se pudo abrir WebSocket Socket.IO: {}".format(exc))
    sys.exit(2)

connected = False
deadline = time.time() + 5
# Completa el handshake de Socket.IO dentro del plazo previsto.
while time.time() < deadline and not connected:
    packet = receive(ws, timeout=1)
    if packet is None:
        continue
    if isinstance(packet, str) and packet.startswith("40"):
        connected = True
        break
    handle_socket_packet(ws, packet)

if not connected:
    print("  ERROR Socket.IO no completo el handshake por WebSocket")
    ws.close()
    sys.exit(2)

print("  OK WebSocket Socket.IO conectado")
print("")
print("  Enviando prediccion de prueba...")

# Envia una solicitud HTTP mientras mantiene abierto el cliente WebSocket.
payload = urllib.parse.urlencode({
    "DepDelay": "15",
    "Carrier": "AA",
    "FlightDate": "2016-12-25",
    "Origin": "ATL",
    "Dest": "SFO",
    "FlightNum": "1234",
}).encode("utf-8")

request = urllib.request.Request(
    HTTP_BASE + "/flights/delays/predict/classify_realtime",
    data=payload,
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    method="POST",
)
body = urllib.request.urlopen(request, timeout=10).read().decode("utf-8")
response = json.loads(body)
uuid = response.get("id")
if not uuid:
    print("  ERROR Flask no devolvio UUID: {}".format(body))
    ws.close()
    sys.exit(2)

print("  OK Prediccion enviada con UUID: {}".format(uuid))
print("")
print("  Esperando respuesta de Spark via Kafka por WebSocket (60s)...")

deadline = time.time() + 60
# Ignora eventos de otras peticiones hasta encontrar el UUID enviado.
while time.time() < deadline:
    packet = receive(ws, timeout=1)
    prediction = handle_socket_packet(ws, packet)
    if not prediction:
        continue
    if prediction.get("UUID") != uuid:
        continue
    pred = prediction.get("Prediction", "?")
    print("  OK Prediccion recibida por WebSocket: categoria {}".format(pred))
    ws.close()
    sys.exit(0)

print("  ERROR No se recibio la prediccion por WebSocket para UUID {}".format(uuid))
ws.close()
sys.exit(2)
PY
}

# Genera un script shell que comprueba los assets web y descarta polling REST en el frontend.
websocket_static_check_code() {
  cat <<'SH'
set -eu

TEMPLATE="/app/templates/flight_delays_predict_kafka.html"
SOCKET_JS="/app/static/socket.io.min.js"
WEBSOCKET_JS="/app/static/flight_delay_predict_websocket.js"

# Los tres ficheros son necesarios para que la pagina use Socket.IO.
for file in "$TEMPLATE" "$SOCKET_JS" "$WEBSOCKET_JS"; do
  if [ ! -f "$file" ]; then
    echo "  ERROR No existe $file"
    exit 2
  fi
done

# La plantilla debe cargar el cliente especifico de predicciones.
if ! grep -q "flight_delay_predict_websocket.js" "$TEMPLATE"; then
  echo "  ERROR La plantilla Kafka no carga flight_delay_predict_websocket.js"
  exit 2
fi

# Rechaza implementaciones antiguas que consultaban el resultado por REST.
if grep -Eq "_pollForResult|classify_realtime/response" "$WEBSOCKET_JS"; then
  echo "  ERROR flight_delay_predict_websocket.js todavia contiene polling REST"
  exit 2
fi

if grep -q "classify_realtime/response" "$SOCKET_JS"; then
  echo "  ERROR socket.io.min.js todavia simula Socket.IO con polling REST"
  exit 2
fi

# Confirma que el cliente abre un WebSocket nativo.
if ! grep -q "new WebSocket" "$SOCKET_JS"; then
  echo "  ERROR socket.io.min.js no abre WebSocket real"
  exit 2
fi

echo "  OK Frontend WebSocket sin polling REST"
SH
}

# Comprueba logs, assets y el recorrido Kafka-WebSocket dentro del contenedor Flask.
diag_websockets_docker() {
  header "DIAGNOSTICO -- WEBSOCKETS DOCKER"

  subheader "Thread consumer Kafka en Flask Docker"
  docker logs flask 2>&1 | grep -E "kafka|Kafka|consumer|Consumer|socket|Socket|prediction" | tail -10

  subheader "Eventos Socket.IO emitidos"
  docker logs flask 2>&1 | grep -E "emit|prediction_response|socketio" | tail -10

  subheader "Polling REST activo"
  docker logs flask 2>&1 | grep "classify_realtime" | tail -10

  subheader "JS servido al navegador"
  if websocket_static_check_code | docker exec -i flask sh; then
    ok "Frontend WebSocket Docker correcto"
  else
    err "Frontend Docker no confirmado: el navegador puede estar usando polling REST"
  fi

  subheader "Prueba end-to-end Docker -- Kafka por WebSocket"
  echo ""
  if websocket_probe_code | docker exec -i flask python3 -; then
    ok "Pipeline WebSocket/Kafka Docker FUNCIONA"
  else
    err "Pipeline WebSocket/Kafka Docker no confirmado"
  fi
}

# Ejecuta las mismas validaciones WebSocket dentro del deployment Flask de GKE.
diag_websockets_k8s() {
  header "DIAGNOSTICO -- WEBSOCKETS K8S/GKE"

  if ! command -v kubectl >/dev/null 2>&1; then
    err "kubectl no esta disponible"
    return
  fi

  if ! kubectl get deployment flask >/dev/null 2>&1; then
    err "No se encontro deployment/flask en Kubernetes"
    return
  fi

  subheader "Thread consumer Kafka en Flask K8s"
  kubectl logs deployment/flask --tail=120 2>&1 | grep -E "kafka|Kafka|consumer|Consumer|socket|Socket|prediction" | tail -10

  subheader "Eventos Socket.IO emitidos"
  kubectl logs deployment/flask --tail=120 2>&1 | grep -E "emit|prediction_response|socketio" | tail -10

  subheader "Polling REST activo"
  kubectl logs deployment/flask --tail=120 2>&1 | grep "classify_realtime" | tail -10

  subheader "JS servido al navegador"
  if websocket_static_check_code | kubectl exec -i deployment/flask -- sh; then
    ok "Frontend WebSocket K8s correcto"
  else
    err "Frontend K8s no confirmado: el navegador puede estar usando polling REST"
  fi

  subheader "Prueba end-to-end K8s -- Kafka por WebSocket"
  echo ""
  if websocket_probe_code | kubectl exec -i deployment/flask -- python3 -; then
    ok "Pipeline WebSocket/Kafka K8s FUNCIONA"
  else
    err "Pipeline WebSocket/Kafka K8s no confirmado"
  fi
}

# Permite ejecutar el diagnostico WebSocket en uno o ambos entornos.
diag_websockets() {
  while true; do
    header "DIAGNOSTICO -- WEBSOCKETS"
    echo -e "  ${GREEN}1)${NC} Docker"
    echo -e "  ${GREEN}2)${NC} Kubernetes / GKE"
    echo -e "  ${GREEN}3)${NC} Ambos"
    echo ""
    echo -e "  ${GREEN}0)${NC} Volver"
    echo ""
    echo -n "  Selecciona entorno: "
    read wsopcion || break

    case $wsopcion in
      1)
        diag_websockets_docker
        pause
        ;;
      2)
        diag_websockets_k8s
        pause
        ;;
      3)
        diag_websockets_docker
        diag_websockets_k8s
        pause
        ;;
      0)
        break
        ;;
      *)
        warn "Opcion no valida"
        ;;
    esac
  done
}

# Consulta experimentos y ejecuciones registradas por los entrenamientos.
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

# Revisa DAGs, ejecuciones recientes y actividad del scheduler.
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

# Envia una prediccion Docker y verifica su recorrido por todos los sinks.
diag_test_e2e_docker() {
  header "TEST END-TO-END AUTOMATICO -- DOCKER"

  local endpoint="http://localhost:5001/flights/delays/predict/classify_realtime"
  local payload="DepDelay=15&Carrier=AA&FlightDate=2016-12-25&Origin=ATL&Dest=SFO&FlightNum=1234"
  local response body http_code uuid result result_body status prediction label

  # Publica una solicitud valida y separa el cuerpo del codigo HTTP.
  info "Enviando prediccion a Flask Docker..."
  response=$(curl -s -w "\nHTTP_CODE=%{http_code}\n" -X POST "$endpoint" -d "$payload" 2>/dev/null)
  http_code=$(printf "%s\n" "$response" | awk -F= '/^HTTP_CODE=/{print $2}' | tail -1)
  body=$(printf "%s" "$response" | sed '/^HTTP_CODE=/d')

  if [ "$http_code" = "200" ]; then
    pass_check "Flask acepto la peticion (HTTP 200)"
  else
    fail_check "Flask no respondio correctamente (HTTP ${http_code:-N/A})"
    echo "$body"
    pause
    return
  fi

  # Extrae el identificador que correlaciona peticion y respuesta.
  uuid=$(JSON_BODY="$body" python3 -c 'import os,json; d=json.loads(os.environ.get("JSON_BODY","{}")); print(d.get("id",""))' 2>/dev/null)
  if [ -z "$uuid" ]; then
    fail_check "No se pudo extraer UUID de la respuesta"
    echo "$body"
    pause
    return
  fi
  pass_check "UUID generado: $uuid"

  # Consulta el endpoint de respuesta hasta recibir OK o agotar 60 segundos.
  info "Esperando respuesta de Spark/Kafka (max 60s)..."
  status="TIMEOUT"
  for _ in $(seq 1 20); do
    result=$(curl -s -w "\nHTTP_CODE=%{http_code}\n" "$endpoint/response/$uuid" 2>/dev/null)
    result_body=$(printf "%s" "$result" | sed '/^HTTP_CODE=/d')
    status=$(JSON_BODY="$result_body" python3 -c 'import os,json; d=json.loads(os.environ.get("JSON_BODY","{}")); print(d.get("status",""))' 2>/dev/null)
    [ "$status" = "OK" ] && break
    sleep 3
  done

  if [ "$status" = "OK" ]; then
    pass_check "Respuesta recibida por polling REST"
  else
    fail_check "No se recibio respuesta en 60s"
  fi

  # Traduce la clase numerica a una etiqueta legible.
  prediction=$(JSON_BODY="$result_body" python3 -c 'import os,json; d=json.loads(os.environ.get("JSON_BODY","{}")); print(d.get("prediction",{}).get("Prediction",""))' 2>/dev/null)
  case "$prediction" in
    0|0.0) label="no delay" ;;
    1|1.0) label="small delay" ;;
    2|2.0) label="moderate delay" ;;
    3|3.0) label="severe delay" ;;
    *) label="desconocida" ;;
  esac
  echo ""
  echo -e "  ${BOLD}Prediction:${NC} ${prediction:-N/A} ($label)"

  # Busca el mismo UUID en MongoDB, Cassandra y el topic de respuesta Kafka.
  subheader "Verificacion de sinks"
  local mongo_count cassandra_hit kafka_hit
  mongo_count=$(docker exec mongo mongosh --quiet agile_data_science \
    --eval "db.flight_delay_ml_response.countDocuments({UUID: '$uuid'})" 2>/dev/null | tail -1 | tr -d '[:space:]')
  [ "${mongo_count:-0}" -gt 0 ] 2>/dev/null && pass_check "MongoDB contiene UUID $uuid" || fail_check "MongoDB no contiene UUID $uuid"

  cassandra_hit=$(docker exec cassandra cqlsh -e \
    "SELECT uuid FROM agile_data_science.flight_delay_classification_response WHERE uuid='$uuid';" 2>/dev/null | grep "$uuid" | head -1)
  [ -n "$cassandra_hit" ] && pass_check "Cassandra contiene UUID $uuid" || fail_check "Cassandra no contiene UUID $uuid"

  kafka_hit=$(docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic flight-delay-ml-response \
    --from-beginning --timeout-ms 5000 --max-messages 1000 2>/dev/null | grep "$uuid" | tail -1)
  [ -n "$kafka_hit" ] && pass_check "Kafka response topic contiene UUID $uuid" || fail_check "Kafka response topic no muestra UUID $uuid"

  pause
}

# Repite el test end-to-end contra el servicio NodePort desplegado en GKE.
diag_test_e2e_k8s() {
  header "TEST END-TO-END AUTOMATICO -- K8S"

  if ! command -v kubectl >/dev/null 2>&1; then
    err "kubectl no esta disponible"
    pause
    return
  fi

  local node_ip endpoint payload response body http_code uuid result result_body status prediction label
  node_ip=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}' 2>/dev/null)
  if [ -z "$node_ip" ]; then
    err "No se pudo obtener la IP externa del nodo GKE"
    pause
    return
  fi

  endpoint="http://$node_ip:30001/flights/delays/predict/classify_realtime"
  payload="DepDelay=10&Carrier=UA&FlightDate=2016-07-04&Origin=ORD&Dest=LAX&FlightNum=500"

  # Envia la solicitud al nodo GKE y separa cuerpo y estado HTTP.
  info "Enviando prediccion a Flask K8s ($endpoint)..."
  response=$(curl -s -w "\nHTTP_CODE=%{http_code}\n" -X POST "$endpoint" -d "$payload" 2>/dev/null)
  http_code=$(printf "%s\n" "$response" | awk -F= '/^HTTP_CODE=/{print $2}' | tail -1)
  body=$(printf "%s" "$response" | sed '/^HTTP_CODE=/d')

  if [ "$http_code" = "200" ]; then
    pass_check "Flask K8s acepto la peticion (HTTP 200)"
  else
    fail_check "Flask K8s no respondio correctamente (HTTP ${http_code:-N/A})"
    echo "$body"
    pause
    return
  fi

  # Recupera el UUID usado para seguir la prediccion en el pipeline.
  uuid=$(JSON_BODY="$body" python3 -c 'import os,json; d=json.loads(os.environ.get("JSON_BODY","{}")); print(d.get("id",""))' 2>/dev/null)
  if [ -z "$uuid" ]; then
    fail_check "No se pudo extraer UUID de la respuesta"
    echo "$body"
    pause
    return
  fi
  pass_check "UUID generado: $uuid"

  # Espera la respuesta procesada por Spark y publicada por Kafka.
  info "Esperando respuesta de Spark/Kafka en K8s (max 60s)..."
  status="TIMEOUT"
  for _ in $(seq 1 20); do
    result=$(curl -s -w "\nHTTP_CODE=%{http_code}\n" "$endpoint/response/$uuid" 2>/dev/null)
    result_body=$(printf "%s" "$result" | sed '/^HTTP_CODE=/d')
    status=$(JSON_BODY="$result_body" python3 -c 'import os,json; d=json.loads(os.environ.get("JSON_BODY","{}")); print(d.get("status",""))' 2>/dev/null)
    [ "$status" = "OK" ] && break
    sleep 3
  done

  if [ "$status" = "OK" ]; then
    pass_check "Respuesta recibida por polling REST"
  else
    fail_check "No se recibio respuesta en 60s"
  fi

  # Convierte la categoria del modelo en una descripcion.
  prediction=$(JSON_BODY="$result_body" python3 -c 'import os,json; d=json.loads(os.environ.get("JSON_BODY","{}")); print(d.get("prediction",{}).get("Prediction",""))' 2>/dev/null)
  case "$prediction" in
    0|0.0) label="no delay" ;;
    1|1.0) label="small delay" ;;
    2|2.0) label="moderate delay" ;;
    3|3.0) label="severe delay" ;;
    *) label="desconocida" ;;
  esac
  echo ""
  echo -e "  ${BOLD}Prediction:${NC} ${prediction:-N/A} ($label)"

  # Comprueba el UUID dentro de los tres servicios desplegados en Kubernetes.
  subheader "Verificacion de sinks K8s"
  local mongo_count cassandra_hit kafka_hit
  mongo_count=$(kubectl exec deployment/mongo -- mongosh --quiet agile_data_science \
    --eval "db.flight_delay_ml_response.countDocuments({UUID: '$uuid'})" 2>/dev/null | tail -1 | tr -d '[:space:]')
  [ "${mongo_count:-0}" -gt 0 ] 2>/dev/null && pass_check "MongoDB K8s contiene UUID $uuid" || fail_check "MongoDB K8s no contiene UUID $uuid"

  cassandra_hit=$(kubectl exec deployment/cassandra -- cqlsh -e \
    "SELECT uuid FROM agile_data_science.flight_delay_classification_response WHERE uuid='$uuid';" 2>/dev/null | grep "$uuid" | head -1)
  [ -n "$cassandra_hit" ] && pass_check "Cassandra K8s contiene UUID $uuid" || fail_check "Cassandra K8s no contiene UUID $uuid"

  kafka_hit=$(kubectl exec deployment/kafka -- /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic flight-delay-ml-response \
    --from-beginning --timeout-ms 5000 --max-messages 1000 2>/dev/null | grep "$uuid" | tail -1)
  [ -n "$kafka_hit" ] && pass_check "Kafka K8s response topic contiene UUID $uuid" || fail_check "Kafka K8s response topic no muestra UUID $uuid"

  pause
}

# Demuestra que los drivers se ejecutan en workers y no dentro del cliente submitter.
diag_deploy_mode_cluster() {
  header "DIAGNOSTICO -- DEPLOY-MODE CLUSTER"

  # La API JSON del master identifica el worker asignado a cada driver Docker.
  subheader "Docker Spark Standalone"
  python3 - <<'PY'
import json
import urllib.request

try:
    # Consulta el estado real anunciado por el master Docker.
    data = json.load(urllib.request.urlopen("http://localhost:8080/json/", timeout=10))
except Exception as exc:
    print(f"  ERROR leyendo Spark UI Docker: {exc}")
    raise SystemExit(0)

drivers = data.get("activedrivers", [])
completed = data.get("completeddrivers", [])[:5]
# Un driver con worker asignado demuestra el uso de deploy-mode cluster.
print(f"  Active drivers: {len(drivers)}")
for driver in drivers:
    worker = driver.get("worker")
    print(f"  Driver ID: {driver.get('id')}")
    print(f"    MainClass: {driver.get('mainclass')}")
    print(f"    State: {driver.get('state')}")
    print(f"    Worker: {worker or 'None'}")
    print("    ✓ CLUSTER MODE: driver en worker" if worker else "    ✗ CLIENT MODE: sin worker")
if completed:
    print("  Completed drivers recientes:")
    for driver in completed:
        print(f"    {driver.get('id')} {driver.get('mainclass')} state={driver.get('state')} worker={driver.get('worker')}")

# Resume los recursos totales y ocupados de cada worker.
workers = data.get("workers", [])
print(f"  Workers activos: {len(workers)}")
for worker in workers:
    print(f"    {worker.get('id')}: cores={worker.get('coresused')}/{worker.get('cores')} mem={worker.get('memoryused')}/{worker.get('memory')}MB")
PY

  # Muestra el stderr del driver mas reciente alojado por cada worker.
  subheader "Docker stderr del driver mas reciente en workers"
  echo "  spark-worker-1:"
  docker exec spark-worker-1 bash -c "cat \$(ls -td /opt/spark/work/driver-*/stderr 2>/dev/null | head -1) 2>/dev/null | grep -E 'MicroBatch|bootstrap.servers|ERROR' | tail -5" 2>/dev/null || warn "No hay stderr reciente en spark-worker-1"
  echo ""
  echo "  spark-worker-2:"
  docker exec spark-worker-2 bash -c "cat \$(ls -td /opt/spark/work/driver-*/stderr 2>/dev/null | head -1) 2>/dev/null | grep -E 'MicroBatch|bootstrap.servers|ERROR' | tail -5" 2>/dev/null || warn "No hay stderr reciente en spark-worker-2"

  # En K8s, workerHostPort debe ser la IP interna de un pod worker.
  subheader "K8s Spark Standalone"
  if command -v kubectl >/dev/null 2>&1; then
    kubectl exec -i deployment/spark-master -- python3 - <<'PY' 2>/dev/null || echo "  No se pudo consultar spark-master en K8s"
import json
import re
import urllib.request

try:
    # Consulta el master desde dentro del cluster para evitar exponer su UI.
    data = json.load(urllib.request.urlopen("http://spark-master:8080/json/", timeout=10))
except Exception as exc:
    print(f"  ERROR leyendo Spark UI K8s: {exc}")
    raise SystemExit(0)

drivers = data.get("activedrivers", [])
print(f"  Active drivers: {len(drivers)}")
for driver in drivers:
    worker = driver.get("worker")
    print(f"  Driver ID: {driver.get('id')}")
    print(f"    MainClass: {driver.get('mainclass')}")
    print(f"    State: {driver.get('state')}")
    print(f"    Worker: {worker or 'None'}")
    # La API REST aporta el host y puerto concretos donde vive el driver.
    try:
        status = json.load(urllib.request.urlopen(
            f"http://spark-master:6066/v1/submissions/status/{driver.get('id')}",
            timeout=10,
        ))
        host_port = status.get("workerHostPort", "")
        print(f"    REST State: {status.get('driverState')}")
        print(f"    workerHostPort: {host_port}")
        if re.match(r"^\d+\.\d+\.\d+\.\d+:", str(host_port)):
            print("    ✓ CLUSTER MODE: workerHostPort es IP de pod")
        else:
            print("    ✗ Revisar: workerHostPort no parece IP")
    except Exception as exc:
        print(f"    REST status no disponible: {exc}")

# Muestra la distribucion de cores y memoria entre los pods worker.
workers = data.get("workers", [])
print(f"  Workers activos: {len(workers)}")
for worker in workers:
    print(f"    {worker.get('id')}: cores={worker.get('coresused')}/{worker.get('cores')} mem={worker.get('memoryused')}/{worker.get('memory')}MB")
PY
    echo ""
    echo "  Pods spark-worker:"
    kubectl get pods -l app=spark-worker -o wide 2>/dev/null || true
  else
    warn "kubectl no esta disponible"
  fi

  pause
}

# Compara las versiones en ejecucion con las exigidas por el enunciado.
diag_versiones() {
  header "VERIFICACION DE VERSIONES DEL ENUNCIADO"

  local spark_out spark_real scala_real kafka_version kafka_real mongo_real cassandra_real airflow_real mlflow_real python_real jar_classes
  local zookeeper_count kafka_proc

  # Obtiene cada version desde el binario o API del servicio realmente arrancado.
  spark_out=$(docker exec spark-master /opt/spark/bin/spark-submit --version 2>&1)
  spark_real=$(echo "$spark_out" | grep -oE 'version [0-9]+\.[0-9]+\.[0-9]+' | head -1 | awk '{print $2}')
  scala_real=$(echo "$spark_out" | sed -nE 's/.*Scala version ([0-9]+\.[0-9]+).*/\1/p' | head -1)
  kafka_version=$(docker exec kafka /opt/kafka/bin/kafka-topics.sh --version 2>/dev/null | head -1)
  zookeeper_count=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -ci zookeeper || true)
  kafka_proc=$(docker exec kafka ps aux 2>/dev/null | grep -v grep | grep kafka | grep -v zookeeper | head -1)
  if [ "$kafka_version" = "4.2.0" ] && [ "$zookeeper_count" = "0" ] && [ -n "$kafka_proc" ]; then
    kafka_real="kafka_2.13-$kafka_version (KRaft, sin Zookeeper)"
  else
    kafka_real="${kafka_version:-N/A}"
  fi
  mongo_real=$(docker exec mongo mongosh --quiet --eval 'db.version()' 2>/dev/null | head -1)
  cassandra_real=$(docker exec cassandra cqlsh -e "SELECT release_version FROM system.local;" 2>/dev/null | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
  airflow_real=$(docker exec airflow airflow version 2>/dev/null | head -1)
  mlflow_real=$(docker exec mlflow mlflow --version 2>/dev/null | awk '{print $3}' | head -1)
  if [ -z "$mlflow_real" ]; then
    mlflow_real=$(curl -s http://localhost:5002/api/2.0/mlflow/experiments/search -H 'Content-Type: application/json' -d '{}' 2>/dev/null | python3 -c "import sys,json; json.load(sys.stdin); print('API OK')" 2>/dev/null)
  fi
  python_real=$(python3 --version 2>/dev/null | awk '{print $2}')
  jar_classes=$(jar tf shared-jars/flight_prediction_2.13-0.1.jar 2>/dev/null | grep -c '\.class')

  # Imprime una fila y evalua la condicion de compatibilidad indicada.
  printf "  %-14s | %-24s | %-36s | %s\n" "Componente" "Version esperada" "Version real" "Estado"
  printf "  %-14s-+-%-24s-+-%-36s-+-%s\n" "--------------" "------------------------" "------------------------------------" "------"
  version_row() {
    local component="$1" expected="$2" real="$3" condition="$4" state
    if eval "$condition"; then
      state="${GREEN}✓${NC}"
    else
      state="${RED}✗${NC}"
    fi
    printf "  %-14s | %-24s | %-36s | %b\n" "$component" "$expected" "${real:-N/A}" "$state"
  }

  version_row "Spark" "4.1.1" "$spark_real" "[[ \"$spark_real\" == \"4.1.1\" ]]"
  version_row "Scala" "2.13" "$scala_real" "[[ \"$scala_real\" == \"2.13\" ]]"
  version_row "Kafka" "kafka_2.13-4.2.0 KRaft" "$kafka_real" "[[ \"$kafka_real\" == *\"4.2.0\"* && \"$kafka_real\" == *\"KRaft\"* ]]"
  version_row "MongoDB" "7.0.17" "$mongo_real" "[[ \"$mongo_real\" == \"7.0.17\" ]]"
  version_row "Cassandra" "4.1" "$cassandra_real" "[[ \"$cassandra_real\" == 4.1* ]]"
  version_row "Airflow" "2.10.4" "$airflow_real" "[[ \"$airflow_real\" == \"2.10.4\" ]]"
  version_row "MLflow" "2.19.0" "$mlflow_real" "[[ \"$mlflow_real\" == \"2.19.0\" || \"$mlflow_real\" == \"API OK\" ]]"
  version_row "Python" "3.10+" "$python_real" "[[ \"$python_real\" == 3.10* || \"$python_real\" == 3.11* || \"$python_real\" == 3.12* || \"$python_real\" == 3.13* ]]"

  echo ""
  echo "  JAR classes: ${jar_classes:-0}"
  if [ "$zookeeper_count" = "0" ] && [ -n "$kafka_proc" ]; then
    pass_check "KRaft mode confirmado (sin Zookeeper)"
  else
    warn "Verificar modo Kafka: no se pudo confirmar KRaft completamente"
  fi

  pause
}

# Menu de acceso a las comprobaciones individuales y end-to-end.
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
    echo -e "  ${GREEN}4)${NC} WebSockets (Docker/K8s, prueba end-to-end automatica)"
    echo -e "  ${GREEN}5)${NC} MinIO -- Data Lakehouse (modelos, Iceberg)"
    echo -e "  ${GREEN}6)${NC} Spark Streaming (sinks, micro-batches)"
    echo -e "  ${GREEN}7)${NC} MongoDB (predicciones almacenadas)"
    echo -e "  ${GREEN}8)${NC} Prometheus & Grafana (metricas)"
    echo -e "  ${GREEN}9)${NC} MLflow (experimentos y runs)"
    echo -e "  ${GREEN}a)${NC} Airflow (DAGs y ejecuciones)"
    echo -e "  ${GREEN}b)${NC} Test end-to-end automatico Docker"
    echo -e "  ${GREEN}c)${NC} Test end-to-end automatico K8s"
    echo -e "  ${GREEN}d)${NC} Diagnostico deploy-mode cluster"
    echo -e "  ${GREEN}e)${NC} Verificar versiones del enunciado"
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
      b|B) diag_test_e2e_docker ;;
      c|C) diag_test_e2e_k8s ;;
      d|D) diag_deploy_mode_cluster ;;
      e|E) diag_versiones ;;
      0) break ;;
      *) warn "Opcion no valida" ;;
    esac
  done
}

# ============================================================
#   LOGS
# ============================================================

# Localiza el ultimo driver alojado por un worker y muestra su salida reciente.
logs_spark_worker_driver() {
  local worker="$1"
  header "LOGS -- ${worker^^} (DRIVERS)"
  docker exec "$worker" bash -lc '
    DRIVER=$(ls -td /opt/spark/work/driver-*/ 2>/dev/null | head -1)
    if [ -n "$DRIVER" ]; then
      echo "=== Driver mas reciente: $DRIVER ==="
      echo "--- stdout ---"
      cat "$DRIVER/stdout" 2>/dev/null | tail -20
      echo "--- stderr (ultimas 30 lineas) ---"
      cat "$DRIVER/stderr" 2>/dev/null | grep -v NativeCodeLoader | grep -v SLF4J | tail -30
    else
      echo "No hay drivers en este worker"
    fi
  ' 2>/dev/null || warn "No se pudo leer $worker"
  pause
}

# Filtra las metricas propias del pipeline expuestas por Flask.
logs_flask_metrics() {
  header "FLASK METRICS -- /metrics"
  curl -s http://localhost:5001/metrics 2>/dev/null | \
    grep -E "^flight_|^flask_http_request_total|^flask_http_request_duration" || warn "No se pudieron leer metricas Flask"
  pause
}

# Menu para consultar logs Docker sin recordar los nombres de los contenedores.
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
    echo -e "  ${GREEN}d)${NC}  Spark Worker-1 (logs de drivers)"
    echo -e "  ${GREEN}e)${NC}  Spark Worker-2 (logs de drivers)"
    echo -e "  ${GREEN}f)${NC}  Flask metricas Prometheus (/metrics)"
    echo ""
    echo -e "  ${GREEN}0)${NC}  Volver"
    echo -e "${BOLD}${YELLOW}============================================${NC}"
    echo ""
    echo -n "  Selecciona servicio: "
    read logop

    # Limita la salida normal a las ultimas cincuenta lineas.
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
        # Resume todos los servicios con cinco lineas por contenedor.
        for svc in flask spark-predictor spark-master kafka mongo cassandra minio airflow mlflow prometheus grafana; do
          subheader "$svc"
          docker logs $svc --tail=5 2>&1
        done
        pause ;;
      d|D)
        logs_spark_worker_driver "spark-worker-1" ;;
      e|E)
        logs_spark_worker_driver "spark-worker-2" ;;
      f|F)
        logs_flask_metrics ;;
      0) break ;;
      *) warn "Opcion no valida" ;;
    esac
  done
}

# Borra el estado de Structured Streaming y reinicia el predictor Docker.
limpiar_checkpoints_docker() {
  header "LIMPIAR CHECKPOINTS S3A -- DOCKER"
  info "Limpiando checkpoints S3A del predictor..."
  warn "Esto requiere reiniciar el predictor"
  echo -n "  Confirmas? (s/N): "
  read confirm

  if [[ "$confirm" =~ ^[sS]$ ]]; then
    # Detiene primero el proceso que envia MakePrediction al master.
    docker compose --profile predictor stop spark-predictor 2>/dev/null || true

    # Obtiene el driver activo desde la API JSON de Spark.
    local driver_id
    driver_id=$(curl -s http://localhost:8080/json/ | python3 -c 'import sys,json; d=json.load(sys.stdin); drivers=[x for x in d.get("activedrivers",[]) if "MakePrediction" in x.get("mainclass","")]; print(drivers[0]["id"] if drivers else "")' 2>/dev/null)
    if [ -n "$driver_id" ]; then
      # Intenta matar el driver por REST con curl y usa Python como alternativa.
      if docker exec spark-master curl -s -X POST "http://spark-master:6066/v1/submissions/kill/$driver_id" >/dev/null 2>&1; then
        ok "Driver $driver_id eliminado"
      elif docker exec spark-master python3 -c "import urllib.request; urllib.request.urlopen('http://spark-master:6066/v1/submissions/kill/$driver_id', data=b'')" >/dev/null 2>&1; then
        ok "Driver $driver_id eliminado"
      else
        warn "No se pudo confirmar la eliminacion del driver $driver_id"
      fi
    else
      warn "No hay driver MakePrediction activo"
    fi

    # Elimina los offsets y metadatos guardados en MinIO antes del nuevo arranque.
    docker exec minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null && mc rm --recursive --force local/flight-data/checkpoints/predictor/ 2>/dev/null && echo 'Checkpoints eliminados' || echo 'No habia checkpoints'"
    docker compose --profile predictor up -d spark-predictor
    ok "Predictor reiniciado con checkpoints limpios"
  else
    info "Operacion cancelada"
  fi

  pause
}

# ============================================================
#   MENU PRINCIPAL
# ============================================================

# Mantiene el menu activo hasta que el usuario seleccione salir.
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
  echo -e "  ${GREEN}9)${NC} Limpiar checkpoints S3A (Docker)"
  echo ""
  echo -e "  ${BOLD}DIAGNOSTICO & LOGS${NC}"
  echo -e "  ${GREEN}10)${NC} Diagnostico del sistema"
  echo -e "  ${GREEN}11)${NC} Ver logs por servicio"
  echo ""
  echo -e "  ${GREEN}0)${NC} Salir"
  echo -e "${BOLD}${BLUE}============================================${NC}"
  echo ""
  echo -n "  Selecciona una opcion: "
  read opcion

  # Delega cada opcion en la funcion correspondiente.
  case $opcion in
    1) arrancar_docker ;;
    2) arrancar_k8s ;;
    3) reentrenar_docker ;;
    4) reentrenar_k8s ;;
    5) show_urls_docker ;;
    6) show_urls_k8s ;;
    7) apagar_k8s ;;
    8) parar_docker ;;
    9) limpiar_checkpoints_docker ;;
    10) menu_diagnostico ;;
    11) menu_logs ;;
    0) echo ""; info "Hasta luego!"; echo ""; break ;;
    *) warn "Opcion no valida" ;;
  esac
done
