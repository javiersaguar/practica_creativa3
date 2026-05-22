# Práctica Big Data — Predicción de Retraso de Vuelos

![Spark](https://img.shields.io/badge/Spark-4.1.1-E25A1C?logo=apachespark&logoColor=white)
![Scala](https://img.shields.io/badge/Scala-2.13-DC322F?logo=scala&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-4.2.0-231F20?logo=apachekafka&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-GKE-326CE5?logo=kubernetes&logoColor=white)

Sistema Big Data completo para entrenar y servir predicciones de retraso de vuelos con Spark, Kafka, Cassandra, MongoDB, MinIO/Iceberg, MLflow, Airflow, Prometheus y Grafana. El proyecto soporta despliegue local con Docker Compose y despliegue cloud en Kubernetes sobre Google Kubernetes Engine.

**Autores**

- Iria Lozano Carrasco — <irialozanocarrasco26@gmail.com> — ETSIT UPM
- Javier Saguar — <javisaguarantona@gmail.com> — ETSIT UPM
- Asignatura: Ingeniería Big Data en la Nube (GISD)
- Curso: 2025-2026
- Repositorio: <https://github.com/Big-Data-ETSIT/practica_creativa>

## ✅ Requisitos Cumplidos

| Requisito | Puntos | Estado | Descripción de implementación |
|-----------|--------|--------|-------------------------------|
| Iceberg Data Lakehouse | 1 | ✅ | Datos en MinIO `s3a://flight-data/warehouse` vía Apache Iceberg, tabla `minio.flights.training_data` con 457k registros. |
| Distancias en Cassandra | 1 | ✅ | 4696 distancias en keyspace `agile_data_science.origin_dest_distances`; Flask lee de Cassandra vía `predict_utils`. |
| Kafka + Cassandra + WebSockets | 1 | ✅ | `MakePrediction` escribe en topic `flight-delay-ml-response`, predicciones en Cassandra, Flask usa SocketIO WebSockets. |
| TrainModel lee/guarda Lakehouse | 1 | ✅ | `TrainModel.scala` lee de `minio.flights.training_data` y guarda modelos en `s3a://flight-data/models`. |
| Docker Compose completo | 1 | ✅ | 13 servicios dockerizados, Spark en deploy-mode CLUSTER, predictor corre en worker. |
| K8s GKE completo | 3 | ✅ | Cluster `practica-k8s` en GKE, deploy-mode CLUSTER verificado vía REST API Spark, todos los pods `1/1 Running`. |
| Airflow + MLflow Docker | 1 | ✅ | DAG `retrain_flight_delay_model`, runs `FINISHED` con accuracy 62.24%, `deploy_mode=cluster` registrado en MLflow. |
| GCloud K8s | 1 | ✅ | Desplegado en `europe-southwest1`, NodePorts accesibles, cluster GKE gestionado con `practica.sh`. |
| Observabilidad | 1 | ✅ | Prometheus + Grafana con métricas Flask, dashboard `Flight Delay Pipeline`, métricas personalizadas `flight_requests_total`. |
| TOTAL | 10/10 | ✅ | Todos los requisitos obligatorios están implementados y verificados. |

## 📊 Arquitectura

El sistema implementa dos flujos principales: entrenamiento batch del modelo y predicción en tiempo real.

### Batch: entrenamiento

Los datos históricos de vuelos se almacenan en MinIO como Data Lakehouse mediante Apache Iceberg. El script de arranque crea la tabla `minio.flights.training_data` a partir de `data/simple_flight_delay_features.jsonl.bz2`, con aproximadamente 457k registros.

`TrainModel.scala` ejecuta un entrenamiento Spark MLlib en `deploy-mode cluster`. El modelo final es un `RandomForestClassifier` y se persisten siete componentes en `s3a://flight-data/models`:

- `arrival_bucketizer_2.0.bin`
- `string_indexer_model_Carrier.bin`
- `string_indexer_model_Origin.bin`
- `string_indexer_model_Dest.bin`
- `string_indexer_model_Route.bin`
- `numeric_vector_assembler.bin`
- `spark_random_forest_classifier.flight_delays.5.0.bin`

MLflow registra parámetros y métricas del entrenamiento, incluyendo `deploy_mode=cluster`, `training_records` y `accuracy`.

### Realtime: predicción

El usuario introduce los datos del vuelo en la UI Flask. Flask calcula características derivadas como distancia, día del año, día del mes y día de la semana. La distancia se consulta en Cassandra, no en MongoDB.

Flask envía la petición al topic Kafka `flight-delay-ml-request`. `MakePrediction.scala`, ejecutado como Spark Streaming en `deploy-mode cluster`, consume ese topic, carga los modelos desde MinIO y calcula la predicción. El resultado se escribe en cuatro salidas:

- Kafka topic `flight-delay-ml-response`
- MongoDB colección `agile_data_science.flight_delay_ml_response`
- Cassandra tabla `agile_data_science.flight_delay_classification_response`
- Consola Spark para diagnóstico

Flask mantiene un consumidor Kafka sobre el topic de respuesta y emite las predicciones a la UI con SocketIO. La UI también puede consultar el resultado por polling REST usando el UUID generado.

### Servicios y puertos

| Servicio | Puerto Docker | Puerto K8s (NodePort) | Credenciales |
|----------|---------------|----------------------|--------------|
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

Tanto `TrainModel` como `MakePrediction` corren siempre en Spark `deploy-mode cluster`, tanto en Docker como en K8s. Esto significa que el driver Spark no se queda en el contenedor o pod que lanza `spark-submit`; Spark Standalone lo ejecuta dentro de uno de los workers.

Para evaluación, esto se verifica en la Spark UI:

- Docker: `http://IP:8080`
- K8s: `http://NODE_IP:30880`

Los drivers activos deben mostrar un worker asignado, por ejemplo `worker-...-172.x.x.x` en Docker o `worker-...-10.x.x.x` en K8s. Si el campo worker aparece como `None`, el proceso no está en deploy-mode cluster.

## Requisitos Previos

### Docker

- Docker Engine 24+
- Docker Compose v2
- 16 GB RAM mínimo
- 32 GB RAM recomendado
- 20 GB de disco libre

### Kubernetes / GKE

- Cuenta Google Cloud con proyecto activo
- `gcloud` CLI instalado y autenticado
- Cluster GKE `practica-k8s` creado con nodos `e2-standard-4`
- `kubectl` instalado
- Zona configurada en el script actual: `europe-southwest1-a`

> Si se recrea el cluster en otra zona, actualiza la variable `ZONE` al inicio de `practica.sh`.

## 🚀 Primer Uso

Pasos para arrancar el proyecto desde un clon limpio:

```bash
# 1. Clonar el repositorio
git clone https://github.com/javiersaguar/practica_creativa2.git
cd practica_creativa2

# 2. Verificar que el JAR compilado existe
ls -la shared-jars/flight_prediction_2.13-0.1.jar

# Si no existe, compilar:
docker run --rm -v "$PWD/flight_prediction":/app -w /app \
  sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.12.10_2.13.18 sbt clean assembly

mkdir -p shared-jars
cp flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar shared-jars/
cp flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar docker/spark/

# 3. Verificar datos disponibles
ls -la data/
# Debe haber:
# - simple_flight_delay_features.jsonl.bz2
# - origin_dest_distances.jsonl

# 4. Lanzar el menú principal
bash practica.sh
```

Para Kubernetes, además:

```bash
# Autenticarse con Google Cloud (necesario tras reinicios o expiración de sesión)
gcloud auth login --no-launch-browser
gcloud config set project PROJECT_ID

export USE_GKE_GCLOUD_AUTH_PLUGIN=True
gcloud container clusters get-credentials practica-k8s --zone europe-southwest1-a

# Después, lanzar el menú y usar la opción 2
bash practica.sh
```

## Uso de `practica.sh`

`practica.sh` es el punto de entrada operativo del proyecto. Centraliza arranque, reentrenamiento, URLs, diagnóstico, logs y operaciones de mantenimiento.

### Arranque

1. **Arrancar con Docker Compose**
   Levanta los 13 servicios, crea bucket MinIO, carga datos, crea tabla Iceberg, importa distancias en Cassandra, entrena el modelo y arranca el predictor Spark Streaming.

2. **Arrancar con Kubernetes (GKE)**
   Verifica autenticación GKE, instala o asegura `kubectl`, escala el cluster, publica imágenes en Artifact Registry, despliega manifests, carga datos, entrena modelo y arranca el predictor en K8s.

### Reentrenamiento

3. **Reentrenar modelo — Docker**
   Lanza el DAG `retrain_flight_delay_model` en Airflow Docker. El DAG detiene el predictor, mata drivers Spark activos, ejecuta `TrainModel` en deploy-mode cluster y reinicia el predictor.

4. **Reentrenar modelo — Kubernetes**
   Lanza el mismo DAG en Airflow K8s. El DAG usa `kubectl exec` contra el pod `spark-master` para lanzar `TrainModel` en deploy-mode cluster.

### Gestión

5. **Ver URLs actuales — Docker**
   Muestra las URLs Docker de Flask, Spark UI, Grafana, Prometheus, MinIO, MLflow y Airflow.

6. **Ver URLs actuales — Kubernetes**
   Muestra las URLs NodePort usando la IP externa del nodo GKE.

7. **Apagar cluster GKE**
   Escala el cluster a 0 nodos para ahorrar costes.

8. **Parar Docker Compose**
   Para los contenedores Docker sin borrar volúmenes.

9. **Limpiar checkpoints S3A — Docker**
   Detiene el predictor, mata el driver `MakePrediction`, borra `s3a://flight-data/checkpoints/predictor` en MinIO y reinicia el predictor.

### Diagnóstico

10. **Diagnóstico del sistema** abre un submenú con:

- `1` Pipeline completo: resumen rápido de contenedores, Kafka, Cassandra, MongoDB, MinIO, Prometheus y Spark.
- `2` Cassandra: keyspaces, tablas, distancias y predicciones.
- `3` Kafka: topics, detalles y mensajes recientes.
- `4` WebSockets: prueba de envío y recepción de predicción.
- `5` MinIO: modelos, tabla Iceberg y tamaño de bucket.
- `6` Spark Streaming: estado del predictor, sinks y micro-batches.
- `7` MongoDB: predicciones almacenadas.
- `8` Prometheus & Grafana: métricas y dashboard.
- `9` MLflow: experimentos y runs.
- `a` Airflow: DAGs y ejecuciones.
- `b` Test end-to-end automático Docker: envía predicción y verifica MongoDB, Cassandra y Kafka.
- `c` Test end-to-end automático K8s: igual que el anterior, pero usando NodePort.
- `d` Diagnóstico deploy-mode cluster: confirma que los drivers Spark corren en workers.
- `e` Verificar versiones del enunciado: tabla de versiones esperadas frente a instaladas.

11. **Ver logs por servicio** abre un submenú con todos los servicios:

- Flask
- Spark Predictor
- Spark Master
- Kafka
- MongoDB
- Cassandra
- MinIO
- Airflow
- MLflow
- Prometheus
- Grafana
- Todos los servicios
- Spark Worker-1: logs de drivers
- Spark Worker-2: logs de drivers
- Flask `/metrics`

## ⚠️ Errores Comunes y Soluciones

### Error 1: la predicción se queda en `Processing...`

**Causa:** el predictor Spark no arrancó correctamente o los checkpoints están corruptos.

**Solución:**

```bash
# Desde practica.sh:
# 10 -> d) Diagnostico deploy-mode cluster
# 9  -> Limpiar checkpoints S3A y reiniciar predictor
# 11 -> d/e) Logs de Spark Worker-1/2
```

### Error 2: MLflow vacío o `TrainModel` no registra runs

**Causa:** modelos no entrenados o entrenamiento no completado.

**Solución:**

```bash
# Verificar modelos en MinIO
# Docker: http://IP:9001 -> flight-data/models/
# K8s:    http://NODE_IP:30901 -> flight-data/models/

# Reentrenar manualmente
bash practica.sh
# Opción 3 para Docker
# Opción 4 para Kubernetes
```

### Error 3: `gcloud auth` muestra `insufficient authentication scopes`

**Causa:** la VM fue creada con scopes limitados y está usando la service account de Compute Engine.

**Solución:**

```bash
gcloud auth login --no-launch-browser
# Abrir el enlace, autenticarse con la cuenta Google personal y pegar el código.
```

### Error 4: `kubectl: command not found`

**Causa:** `kubectl` no está instalado en la VM.

**Solución:** la opción 2 de `practica.sh` lo instala automáticamente. Si hace falta hacerlo manualmente:

```bash
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x kubectl
sudo mv kubectl /usr/local/bin/
```

### Error 5: `gke-gcloud-auth-plugin not found`

**Causa:** el plugin de autenticación GKE no está instalado.

**Solución:** la opción 2 de `practica.sh` crea automáticamente un shim en `/usr/local/bin/gke-gcloud-auth-plugin`. Manualmente, basta con relanzar:

```bash
bash practica.sh
# Opción 2
```

### Error 6: `No resources found` en `kubectl get nodes`

**Causa:** el cluster GKE está escalado a 0 nodos en modo ahorro.

**Solución:**

```bash
bash practica.sh
# Opción 2: escala automáticamente el cluster

# Alternativa manual:
gcloud container clusters resize practica-k8s --num-nodes=2 --zone europe-southwest1-a
```

### Error 7: Airflow no carga en el puerto 8081

**Causa:** el webserver o scheduler de Airflow no arrancó correctamente.

**Solución:**

```bash
docker compose restart airflow
docker logs airflow --tail=50
```

### Error 8: el JAR es más antiguo que el código fuente

**Causa:** se modificó el código Scala pero no se recompiló.

**Solución:**

```bash
docker run --rm -v "$PWD/flight_prediction":/app -w /app \
  sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.12.10_2.13.18 sbt clean assembly

cp flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar shared-jars/
cp flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar docker/spark/

docker compose build --no-cache spark-master spark-worker-1 spark-worker-2 spark-predictor
```

### Error 9: la IP cambia tras reiniciar la VM

**Causa:** la VM tiene IP externa dinámica.

**Solución:**

```bash
curl ifconfig.me
# Actualizar SSH config o URLs compartidas con la nueva IP.
```

### Error 10: `docker compose down -v` elimina los modelos

**Causa:** los modelos están en el volumen Docker `minio_data`.

**Solución:** no usar `down -v` salvo que se quiera resetear todo. Los modelos se regeneran automáticamente con `practica.sh` opción 1. El JAR en `shared-jars/` sobrevive porque es un bind mount, no un volumen Docker.

## Versiones Instaladas

| Componente | Versión | Notas |
|------------|---------|-------|
| Apache Spark | 4.1.1 | Scala 2.13, deploy-mode cluster |
| Scala | 2.13.17 | Runtime usado por Spark |
| Apache Kafka | 4.2.0 | KRaft mode, sin Zookeeper |
| MongoDB | 7.0.17 | Persistencia de predicciones para Flask |
| Apache Cassandra | 4.1.11 | Distancias y predicciones evaluables |
| Apache Airflow | 2.10.4 | SQLite para desarrollo |
| MLflow | 2.19.0 | Tracking de entrenamientos |
| Python | 3.10.12 | Flask y utilidades |
| MinIO | latest | S3-compatible, Iceberg warehouse |
| Prometheus | latest | Métricas Flask personalizadas |
| Grafana | latest | Dashboard `Flight Delay Pipeline` |

## Estructura del Repositorio

```text
.
├── practica.sh
├── docker-compose.yml
├── shared-jars/
├── data/
├── docker/
├── flight_prediction/
├── k8s-gke/
└── resources/web/
```

- `practica.sh`: script principal de gestión; arranque Docker/K8s, reentrenamiento, diagnósticos, logs y mantenimiento.
- `docker-compose.yml`: definición de los 13 servicios Docker.
- `shared-jars/`: JAR Scala compilado usado como bind mount por Spark en Docker.
- `data/`: datos de entrenamiento (`simple_flight_delay_features.jsonl.bz2`) y distancias (`origin_dest_distances.jsonl`).
- `docker/`: Dockerfiles, DAG de Airflow, configuración Kafka, Prometheus y Grafana.
- `flight_prediction/`: proyecto Scala/SBT con `MakePrediction.scala` y `TrainModel.scala`.
- `k8s-gke/`: manifests Kubernetes para GKE, incluyendo Spark master/workers, predictor, Flask, Kafka, Cassandra, MinIO, MLflow, Airflow, Prometheus y Grafana.
- `resources/web/`: aplicación Flask, API de predicción, SocketIO, métricas Prometheus y utilidades.

---

**Para evaluación:** verificar deploy-mode cluster en Spark UI (`http://IP:8080` en Docker o `http://NODE_IP:30880` en K8s). Los drivers deben mostrar un worker asignado, no `None`.
