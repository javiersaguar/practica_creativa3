# Práctica Big Data — Predicción de Retraso de Vuelos

![Spark](https://img.shields.io/badge/Spark-4.1.1-E25A1C?logo=apachespark&logoColor=white)
![Scala](https://img.shields.io/badge/Scala-2.13-DC322F?logo=scala&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-4.2.0-231F20?logo=apachekafka&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-GKE-326CE5?logo=kubernetes&logoColor=white)

Sistema Big Data para entrenar y servir predicciones de retraso de vuelos con Spark, Kafka, Cassandra, MongoDB, MinIO/Iceberg, MLflow, Airflow, Prometheus y Grafana. El proyecto soporta despliegue con Docker Compose y despliegue cloud en Kubernetes sobre Google Kubernetes Engine.

**Autores**

- Iria Lozano Carrasco — <irialozanocarrasco26@gmail.com> — ETSIT UPM
- Javier Saguar — <javisaguarantona@gmail.com> — ETSIT UPM
- Asignatura: Ingeniería Big Data en la Nube (GISD)
- Curso: 2025-2026
- Repositorio reproducible: <https://github.com/javiersaguar/practica_creativa2>

## ✅ Requisitos Cumplidos

| Requisito | Puntos | Estado | Descripción de implementación |
|-----------|--------|--------|-------------------------------|
| Iceberg Data Lakehouse | 1 | ✅ | Datos en MinIO `s3a://flight-data/warehouse` mediante Apache Iceberg, tabla `minio.flights.training_data`. |
| Distancias en Cassandra | 1 | ✅ | Distancias en `agile_data_science.origin_dest_distances`; Flask las consulta desde Cassandra. |
| Kafka + Cassandra + WebSockets | 1 | ✅ | `MakePrediction` escribe en `flight-delay-ml-response` y Cassandra; Flask presenta la respuesta mediante WebSockets. |
| TrainModel lee/guarda Lakehouse | 1 | ✅ | `TrainModel.scala` lee `minio.flights.training_data` y guarda modelos en `s3a://flight-data/models`. |
| Docker Compose completo | 1 | ✅ | Servicios dockerizados, Spark Standalone y predictor en `deploy-mode cluster`. |
| K8s GKE completo | 3 | ✅ | Escenario completo desplegable en GKE y `deploy-mode cluster` verificable desde la API/UI de Spark. |
| Airflow + MLflow Docker | 1 | ✅ | Airflow orquesta reentrenamientos Spark y MLflow registra parámetros, métricas y estado. |
| GCloud | 1 | ✅ | VM de Compute Engine, GKE y Artifact Registry dentro del mismo proyecto de Google Cloud. |
| Observabilidad | 1 | ✅ | Prometheus, Grafana, métricas Flask y diagnósticos automáticos en `practica.sh`. |

## 📊 Arquitectura

El sistema implementa dos flujos principales: entrenamiento batch y predicción en tiempo real.

### Batch: entrenamiento

Los datos históricos de vuelos se incluyen en `data/simple_flight_delay_features.jsonl.bz2`. Durante el arranque se cargan en MinIO y se crea la tabla Iceberg `minio.flights.training_data` sobre `s3a://flight-data/warehouse`.

`TrainModel.scala` ejecuta Spark MLlib en `deploy-mode cluster`, lee la tabla Iceberg y guarda los componentes del modelo en `s3a://flight-data/models`:

- `arrival_bucketizer_2.0.bin`
- `string_indexer_model_Carrier.bin`
- `string_indexer_model_Origin.bin`
- `string_indexer_model_Dest.bin`
- `string_indexer_model_Route.bin`
- `numeric_vector_assembler.bin`
- `spark_random_forest_classifier.flight_delays.5.0.bin`

MLflow registra parámetros y métricas del entrenamiento, incluyendo `deploy_mode`, `training_records` y `accuracy`.

### Realtime: predicción

1. El usuario envía los datos del vuelo desde Flask.
2. Flask consulta la distancia origen-destino en Cassandra y genera un UUID.
3. Flask publica la petición en Kafka `flight-delay-ml-request`.
4. `MakePrediction.scala` consume la petición con Spark Structured Streaming, carga modelos desde MinIO y calcula la predicción.
5. Spark escribe el resultado en Kafka `flight-delay-ml-response`, Cassandra, MongoDB y consola.
6. Flask consume el topic de respuesta y emite `prediction_response` mediante Socket.IO/WebSocket.
7. El navegador muestra únicamente la respuesta cuyo UUID coincide con su petición.

El endpoint REST de respuesta se conserva como compatibilidad y diagnóstico, pero el flujo normal de la interfaz Kafka presenta la predicción mediante WebSocket.

### Servicios y puertos

| Servicio | Puerto Docker | Puerto K8s (NodePort) | Credenciales |
|----------|---------------|-----------------------|--------------|
| Flask UI | 5001 | 30001 | - |
| Spark UI | 8080 | 30880 | - |
| MLflow | 5002 | 30502 | - |
| Airflow | 8081 | 30808 | `admin/admin` |
| MinIO Console | 9001 | 30901 | `minioadmin/minioadmin` |
| Grafana | 3000 | 30300 | `admin/admin` |
| Prometheus | 9090 | 30909 | - |
| MongoDB | 27017 | - | - |
| Cassandra | 9042 | - | - |
| Kafka | 9092 | - | - |

### Deploy-Mode Cluster

`TrainModel` y `MakePrediction` se envían a Spark Standalone con `--deploy-mode cluster` tanto en Docker como en GKE. El driver se ejecuta dentro de un worker Spark, no dentro del contenedor o pod que lanza `spark-submit`.

Para evaluación:

- Docker Spark UI: `http://IP_VM:8080`
- K8s Spark UI: `http://IP_NODO_GKE:30880`
- `practica.sh` → `10` → `d` confirma que cada driver tiene un worker asignado.

## Entorno de despliegue (VM de Google Cloud)

El escenario completo está pensado para administrarse desde una VM de Compute Engine dentro del mismo proyecto de Google Cloud que GKE y Artifact Registry. Docker Compose se ejecuta en la VM; Kubernetes se despliega en el cluster GKE.

Especificaciones mínimas basadas en la VM real `practica-bigdata-v2`:

| Parámetro | Valor recomendado |
|-----------|-------------------|
| Sistema operativo | Ubuntu 22.04 LTS |
| Tipo de máquina | `e2-standard-4` — 4 vCPU, 16 GB RAM |
| Disco de arranque | 100 GB como mínimo |
| Scopes | Acceso total a las API Cloud o autenticación de usuario mediante `gcloud auth login` |
| Zona de la VM real | `europe-southwest1-b` |
| Zona del cluster GKE real | `europe-southwest1-a` |

La VM y el cluster pueden estar en zonas distintas de la misma región. Lo obligatorio para este repositorio es que la variable `ZONE` de `practica.sh` coincida con la zona del cluster GKE. Actualmente ambos usan `europe-southwest1-a`.

Ejemplo para crear una VM equivalente desde una máquina que ya tenga `gcloud`:

```bash
export PROJECT_ID="TU_PROJECT_ID"
gcloud config set project "$PROJECT_ID"

gcloud compute instances create practica-bigdata-v2 \
  --zone=europe-southwest1-b \
  --machine-type=e2-standard-4 \
  --boot-disk-size=100GB \
  --boot-disk-type=pd-balanced \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --scopes=https://www.googleapis.com/auth/cloud-platform
```

Si la política del proyecto no permite scopes amplios, inicia sesión en la VM con una cuenta de usuario que tenga permisos sobre Compute Engine, GKE y Artifact Registry:

```bash
gcloud auth login --no-launch-browser
gcloud config set project "$PROJECT_ID"
```

## Instalación de dependencias en la VM

Una VM limpia debe ejecutar primero `install.sh`. Este script prepara herramientas; no despliega la práctica ni modifica `practica.sh`.

```bash
git clone https://github.com/javiersaguar/practica_creativa2.git
cd practica_creativa2
chmod +x install.sh
./install.sh

# Seleccionar 0 para instalar todo, o instalar componentes uno a uno.
# Después de añadir el usuario al grupo docker:
exit
# Volver a entrar por SSH.
```

`install.sh` instala o verifica de forma idempotente:

- Docker Engine y Docker Compose plugin.
- Java 17.
- Google Cloud CLI.
- `kubectl` oficial en `/usr/local/bin`.
- Plugin oficial `gke-gcloud-auth-plugin` o el shim compatible usado por `practica.sh`.
- Dependencias Python de `requirements.txt` dentro de `.venv`.
- Autenticación de Google Cloud, proyecto, zona y APIs necesarias.
- Versiones de herramientas y presencia de los recursos críticos del repositorio.

Para usar el entorno Python creado:

```bash
source .venv/bin/activate
```

## Creación del cluster GKE desde cero

El cluster real se llama `practica-k8s`, está en `europe-southwest1-a` y usa nodos `e2-standard-4`. Primero habilita APIs y crea el repositorio Docker de Artifact Registry que usa `practica.sh`:

```bash
export PROJECT_ID="TU_PROJECT_ID"
export GKE_ZONE="europe-southwest1-a"
export AR_REGION="europe-southwest1"

gcloud config set project "$PROJECT_ID"
gcloud services enable compute.googleapis.com container.googleapis.com artifactregistry.googleapis.com

gcloud artifacts repositories describe practica \
  --location="$AR_REGION" >/dev/null 2>&1 || \
gcloud artifacts repositories create practica \
  --repository-format=docker \
  --location="$AR_REGION" \
  --description="Imagenes Docker de la practica Big Data"

gcloud auth configure-docker "${AR_REGION}-docker.pkg.dev"
```

Crear el cluster zonal:

```bash
gcloud container clusters create practica-k8s \
  --zone="$GKE_ZONE" \
  --num-nodes=2 \
  --machine-type=e2-standard-4 \
  --disk-type=pd-balanced \
  --disk-size=100 \
  --enable-ip-alias

gcloud container clusters get-credentials practica-k8s \
  --zone="$GKE_ZONE" \
  --project="$PROJECT_ID"

kubectl get nodes
```

Para ahorrar costes, el cluster puede dejarse con 0 nodos después de crearlo:

```bash
gcloud container clusters resize practica-k8s \
  --num-nodes=0 \
  --zone="$GKE_ZONE" \
  --quiet
```

La opción `2` de `practica.sh` vuelve a escalar automáticamente el cluster al número de nodos indicado por `K8S_NODE_COUNT`, con valor predeterminado `2`.

## 🚀 Primer Uso

Flujo completo desde cero:

1. Crear la VM de Compute Engine.
2. Clonar este repositorio.
3. Ejecutar `install.sh` y volver a entrar por SSH para activar el grupo `docker`.
4. Crear el cluster GKE o recuperar sus credenciales.
5. Ejecutar `practica.sh`.

Los datos necesarios ya se incluyen en Git:

```text
data/origin_dest_distances.jsonl
data/simple_flight_delay_features.jsonl.bz2
```

Los contextos de build necesarios también están incluidos:

```text
docker/kafka/
docker/spark/
```

Arranque:

```bash
git clone https://github.com/javiersaguar/practica_creativa2.git
cd practica_creativa2

chmod +x install.sh practica.sh
./install.sh
# Opción 0. Después, cerrar y volver a abrir SSH.

source .venv/bin/activate
./practica.sh
```

En `practica.sh`:

- Opción `1`: arranque completo con Docker Compose.
- Opción `2`: autenticación, publicación de imágenes y despliegue completo en GKE.

El JAR compilado ya se incluye en `shared-jars/flight_prediction_2.13-0.1.jar` y en el contexto Spark. La recompilación es solo un fallback si se modifica el código Scala:

```bash
docker run --rm \
  -v "$PWD/flight_prediction":/app \
  -w /app \
  sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.12.10_2.13.18 \
  sbt clean assembly

cp flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar shared-jars/
cp flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar docker/spark/
```

## Uso de `practica.sh`

`practica.sh` es el punto de entrada operativo. Centraliza arranque, reentrenamiento, URLs, diagnósticos, logs y mantenimiento.

### Arranque

1. **Arrancar con Docker Compose**
   Levanta los servicios, configura MinIO y Cassandra, crea la tabla Iceberg, entrena el modelo y arranca el predictor Spark Streaming.

2. **Arrancar con Kubernetes (GKE)**
   Autentica con GKE, escala el cluster, publica imágenes en Artifact Registry, aplica manifests, carga datos, entrena y arranca el predictor.

### Reentrenamiento

3. **Reentrenar modelo — Docker**
   Lanza el DAG de Airflow que ejecuta `TrainModel` en Spark `deploy-mode cluster`.

4. **Reentrenar modelo — Kubernetes**
   Lanza el DAG de Airflow K8s, que usa `kubectl exec` para enviar `TrainModel` al cluster Spark.

### Gestión

- `5`: ver URLs Docker.
- `6`: ver URLs Kubernetes.
- `7`: apagar nodos del cluster GKE.
- `8`: parar Docker Compose sin borrar volúmenes.
- `9`: limpiar checkpoints S3A del predictor Docker.

### Diagnóstico

La opción `10` abre comprobaciones de:

- Pipeline completo.
- Cassandra, Kafka, MinIO/Iceberg, MongoDB y Spark Streaming.
- WebSockets reales para Docker y K8s.
- Prometheus, Grafana, MLflow y Airflow.
- Tests end-to-end Docker/K8s.
- Drivers Spark en `deploy-mode cluster`.
- Versiones exigidas.

La opción `11` muestra logs por servicio y logs de drivers dentro de los Spark workers.

## ⚠️ Errores Comunes y Soluciones

### Error 1: la predicción se queda en `Processing...`

**Causa:** predictor Spark detenido, error en el pipeline o checkpoints incompatibles.

**Solución:**

```text
practica.sh -> 10 -> 4   Diagnóstico WebSockets
practica.sh -> 10 -> d   Diagnóstico deploy-mode cluster
practica.sh -> 9         Limpiar checkpoints S3A Docker
practica.sh -> 11        Logs por servicio
```

### Error 2: MLflow vacío o `TrainModel` no registra runs

Comprueba los modelos en MinIO y ejecuta el reentrenamiento:

```text
practica.sh -> 3   Reentrenamiento Docker
practica.sh -> 4   Reentrenamiento Kubernetes
```

### Error 3: `gcloud auth` muestra `insufficient authentication scopes`

La VM usa una service account con scopes insuficientes. Autentica una cuenta de usuario:

```bash
gcloud auth login --no-launch-browser
gcloud config set project TU_PROJECT_ID
```

### Error 4: `kubectl: command not found`

Ejecuta `install.sh` → opción `4`, o instala todo con la opción `0`.

### Error 5: `gke-gcloud-auth-plugin not found`

Ejecuta `install.sh` → opción `5`. Instala el plugin oficial cuando está disponible y usa un shim compatible como fallback.

### Error 6: `No resources found` en `kubectl get nodes`

El cluster puede estar escalado a 0 nodos:

```bash
./practica.sh
# Opción 2, o:
gcloud container clusters resize practica-k8s \
  --num-nodes=2 \
  --zone=europe-southwest1-a \
  --quiet
```

### Error 7: Airflow no carga en el puerto 8081

```bash
docker compose restart airflow
docker logs airflow --tail=50
```

### Error 8: el JAR es más antiguo que el código Scala

Recompila el JAR usando el comando indicado en Primer Uso. `practica.sh` también detecta si las fuentes Scala son más recientes que el JAR.

### Error 9: la IP externa cambia tras reiniciar la VM

Reserva una IP estática o consulta la IP actual:

```bash
gcloud compute instances describe practica-bigdata-v2 \
  --zone=europe-southwest1-b \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
```

### Error 10: `docker compose down -v` elimina los modelos

Los modelos Docker están en el volumen de MinIO. No uses `down -v` salvo que quieras resetear el escenario y volver a entrenar.

### Error 11: `docker/spark` o `docker/kafka` no encontrado al hacer build

**Causa:** el repositorio se clonó desde una versión antigua que no incluía los contextos de build.

```bash
git pull
test -f docker/spark/Dockerfile
test -f docker/kafka/Dockerfile
```

### Error 12: `data/` vacío o falla la creación de Iceberg

**Causa:** el repositorio se clonó desde una versión antigua que no incluía los datos.

```bash
git pull
test -f data/simple_flight_delay_features.jsonl.bz2
test -f data/origin_dest_distances.jsonl
```

## Versiones Instaladas

| Componente | Versión | Notas |
|------------|---------|-------|
| Apache Spark | 4.1.1 | Scala 2.13, deploy-mode cluster |
| Scala | 2.13.17 | Runtime usado por Spark |
| Apache Kafka | 4.2.0 | KRaft mode, sin Zookeeper |
| MongoDB | 7.0.17 | Persistencia compatible y diagnóstico |
| Apache Cassandra | 4.1.11 | Distancias y predicciones evaluables |
| Apache Airflow | 2.10.4 | Orquestación de entrenamiento |
| MLflow | 2.19.0 | Tracking de entrenamientos |
| Python | 3.10.12 | Flask y utilidades |
| MinIO | latest | Almacenamiento S3-compatible |
| Apache Iceberg | 1.10.1 | Tabla Lakehouse |
| Prometheus | latest | Métricas |
| Grafana | latest | Dashboard |

## Estructura del Repositorio

```text
.
├── install.sh
├── practica.sh
├── docker-compose.yml
├── data/
│   ├── origin_dest_distances.jsonl
│   └── simple_flight_delay_features.jsonl.bz2
├── docker/
│   ├── airflow/
│   ├── flask/
│   ├── kafka/
│   ├── prometheus/
│   ├── grafana/
│   └── spark/
├── flight_prediction/
├── shared-jars/
├── k8s/
├── k8s-gke/
└── resources/web/
```

- `install.sh`: prepara una VM Ubuntu limpia.
- `practica.sh`: gestiona arranque, reentrenamiento, diagnósticos y mantenimiento.
- `docker-compose.yml`: define el escenario Docker.
- `docker/kafka/` y `docker/spark/`: contextos de build requeridos por Docker Compose y GKE.
- `data/`: datos de entrenamiento y distancias incluidos para reproducibilidad.
- `shared-jars/`: JAR Scala compilado.
- `k8s-gke/`: manifests usados por el despliegue GKE.
- `resources/web/`: aplicación Flask y frontend WebSocket.

---

**Para evaluación:** ejecuta `practica.sh` → `10` para comprobar todos los componentes y `10` → `d` para demostrar que los drivers Spark están ejecutándose dentro de workers.
