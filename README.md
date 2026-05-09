# ✈️ Flight Delay Prediction — Big Data Pipeline

> **Práctica Creativa Big Data · ETSIT UPM 2026**  
> Predicción de retrasos de vuelos en tiempo real con arquitectura Big Data completa

<div align="center">

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://python.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.1.1-orange?logo=apachespark)](https://spark.apache.org)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-black?logo=apachekafka)](https://kafka.apache.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)](https://docker.com)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-GKE-326CE5?logo=kubernetes)](https://kubernetes.io)
[![MLflow](https://img.shields.io/badge/MLflow-2.19-blue?logo=mlflow)](https://mlflow.org)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10.4-017CEE?logo=apacheairflow)](https://airflow.apache.org)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

</div>

---

## 📋 Tabla de Contenidos

- [Descripción del Proyecto](#-descripción-del-proyecto)
- [Arquitectura](#-arquitectura)
- [Stack Tecnológico](#-stack-tecnológico)
- [Requisitos Previos](#-requisitos-previos)
- [Instalación y Despliegue](#-instalación-y-despliegue)
  - [Opción 1: Docker Compose (local)](#opción-1-docker-compose-local)
  - [Opción 2: Kubernetes en GKE](#opción-2-kubernetes-en-gke)
- [Uso del Sistema](#-uso-del-sistema)
- [Reentrenamiento del Modelo](#-reentrenamiento-del-modelo)
- [URLs de Acceso](#-urls-de-acceso)
- [Errores Comunes y Soluciones](#-errores-comunes-y-soluciones)
- [Autores](#-autores)

---

## 🎯 Descripción del Proyecto

Sistema de predicción de retrasos de vuelos en tiempo real que combina batch processing para el entrenamiento de modelos ML con streaming para predicciones en vivo.

El dataset contiene datos del **90-95% de los vuelos con origen en EE.UU. desde 2015** (Bureau of Transportation Statistics). Se entrena un modelo **RandomForest** con PySpark MLlib que clasifica los vuelos en 4 categorías de retraso:

| Categoría | Descripción |
|-----------|-------------|
| `0` | Adelantado (< -15 min) |
| `1` | A tiempo / leve retraso (−15 a 0 min) |
| `2` | Retraso moderado (0 a 30 min) |
| `3` | Retraso severo (> 30 min) |

### Flujo completo

```
Usuario (Web)
    │ POST /flights/delays/predict/classify_realtime
    ▼
Flask (puerto 5001)
    │ produce → Kafka topic: flight-delay-ml-request
    ▼
Spark Streaming (predictor)
    │ consume → predice con RandomForest
    │ publica → Kafka topic: flight-delay-ml-response
    │ guarda  → MongoDB + Cassandra
    ▼
Flask (kafka_consumer_thread)
    │ WebSocket emit → Navegador
    ▼
Usuario recibe predicción en tiempo real
```

---

## 🏗️ Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│                        BATCH (Entrenamiento)                     │
│                                                                   │
│  Airflow DAG ──► Spark MLlib ──► MLflow (tracking)              │
│       │              │                                           │
│       │         lee datos de                                     │
│       │         Iceberg/MinIO ◄── Data Lakehouse (457k registros)│
│       │              │                                           │
│       └──────────────► guarda modelos ──► MinIO /models/         │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      REALTIME (Predicción)                       │
│                                                                   │
│  Flask ──► Kafka ──► Spark Streaming ──► Kafka response          │
│                            │                                     │
│                    ┌───────┴────────┐                            │
│                  MongoDB        Cassandra                        │
│                 (predicciones) (predicciones + distancias)       │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      OBSERVABILIDAD                              │
│                                                                   │
│  Flask /metrics ──► Prometheus ──► Grafana Dashboard            │
│  MLflow UI (experimentos y runs de entrenamiento)                │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Stack Tecnológico

| Componente | Tecnología | Versión |
|------------|------------|---------|
| Web Server | Flask + SocketIO | 3.x |
| Stream Processing | Apache Spark Streaming | 4.1.1 |
| Message Broker | Apache Kafka (KRaft) | 3.x |
| ML Training | PySpark MLlib (RandomForest) | 4.1.1 |
| Data Lakehouse | Apache Iceberg + MinIO | 1.10.1 |
| ML Tracking | MLflow | 2.19.0 |
| Workflow | Apache Airflow | 2.10.4 |
| NoSQL | MongoDB + Cassandra | latest |
| Monitoring | Prometheus + Grafana | latest |
| Containerización | Docker Compose / Kubernetes GKE | — |

---

## 📦 Requisitos Previos

### Para Docker Compose

```bash
# Sistema operativo: Ubuntu 20.04+ / Debian
# Recursos mínimos recomendados: 8 GB RAM, 4 cores

# Docker Engine
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER

# Docker Compose v2
sudo apt-get install docker-compose-plugin

# Java 17 (para spark-submit local)
sudo apt install openjdk-17-jdk

# Python 3.10+
sudo apt install python3 python3-pip
```

### Para Kubernetes (GKE)

```bash
# Google Cloud SDK
curl https://sdk.cloud.google.com | bash

# kubectl
sudo apt-get install kubectl

# Proyecto GCP con billing activo
# Cluster GKE con al menos 2 nodos e2-standard-4
```

---

## 🚀 Instalación y Despliegue

### 1. Clonar el repositorio

```bash
git clone https://github.com/Big-Data-ETSIT/practica_creativa
cd practica_creativa
```

### 2. Descargar el dataset

```bash
mkdir -p data
# Descargar simple_flight_delay_features.jsonl.bz2 desde:
# https://github.com/Big-Data-ETSIT/practica_creativa
# y colocarlo en ./data/
```

### 3. Dar permisos al script

```bash
chmod +x practica.sh
```

---

### Opción 1: Docker Compose (local)

```bash
./practica.sh
# Seleccionar opción 1: "Arrancar con Docker Compose"
```

El script realiza automáticamente:
- ✅ Levanta los 13 servicios Docker
- ✅ Espera a que Cassandra esté lista
- ✅ Configura MinIO con el bucket `flight-data`
- ✅ Sube los modelos pre-entrenados a MinIO
- ✅ Carga las distancias origin-dest en Cassandra
- ✅ Instala MLflow en los workers de Spark
- ✅ Dispara el DAG de reentrenamiento inicial en Airflow

> ⏱️ **Tiempo estimado de arranque**: 3-5 minutos

---

### Opción 2: Kubernetes en GKE

#### Preparación del cluster

```bash
# Autenticarse con GCP
gcloud auth login
gcloud config set project TU_PROJECT_ID

# Crear cluster (si no existe)
gcloud container clusters create practica-k8s \
  --zone europe-southwest1-a \
  --num-nodes 2 \
  --machine-type e2-standard-4

# O escalar uno existente
gcloud container clusters resize practica-k8s \
  --num-nodes=2 --zone europe-southwest1-a --quiet
```

#### Variables a configurar en `practica.sh`

Edita las primeras líneas del script con tus valores:

```bash
ZONE="europe-southwest1-a"          # Zona de tu cluster
CLUSTER="practica-k8s"              # Nombre del cluster
PROJECT_HOME=~/practica_creativa    # Directorio del proyecto
```

Y en los manifests `k8s-gke/*.yaml`, actualiza el registry de imágenes:

```yaml
image: TU_REGION-docker.pkg.dev/TU_PROJECT_ID/practica/spark-master:latest
```

#### Construir y subir imágenes

```bash
# Configurar Docker para usar Artifact Registry
gcloud auth configure-docker TU_REGION-docker.pkg.dev

# Construir y publicar imágenes
docker build -t TU_REGION-docker.pkg.dev/TU_PROJECT/practica/spark-master:latest \
  -f docker/spark/Dockerfile docker/spark/
docker push TU_REGION-docker.pkg.dev/TU_PROJECT/practica/spark-master:latest

# Repetir para spark-worker, flask, spark-predictor
```

#### Arrancar en K8s

```bash
./practica.sh
# Seleccionar opción 2: "Arrancar con Kubernetes (GKE)"
```

El script realiza automáticamente:
- ✅ Autentica con el cluster GKE
- ✅ Aplica todos los manifests de `k8s-gke/`
- ✅ Configura MinIO con PVC de 10Gi
- ✅ Carga Iceberg con 457k registros de entrenamiento
- ✅ Configura Cassandra con keyspace y tablas
- ✅ Descarga `kubectl` al pod de Airflow
- ✅ Instala MLflow en spark-master
- ✅ Arranca el scheduler de Airflow

> ⏱️ **Tiempo estimado de arranque**: 5-10 minutos

> ⚠️ **Importante**: Recuerda apagar el cluster cuando no lo uses (opción 7) para ahorrar créditos GCP.

---

## 💻 Uso del Sistema

### Menú principal

```
============================================
  ARRANQUE
  1) Arrancar con Docker Compose
  2) Arrancar con Kubernetes (GKE)

  REENTRENAMIENTO
  3) Reentrenar modelo -- Docker
  4) Reentrenar modelo -- Kubernetes (DAG Airflow)

  GESTION
  5) Ver URLs actuales -- Docker
  6) Ver URLs actuales -- Kubernetes
  7) Apagar cluster GKE (ahorra dinero)
  8) Parar Docker Compose

  DIAGNOSTICO & LOGS
  9)  Diagnostico del sistema
  10) Ver logs por servicio

  0) Salir
============================================
```

### Hacer una predicción

1. Abre la interfaz web de Flask
2. Introduce los datos del vuelo:
   - **Departure Delay**: retraso inicial en minutos
   - **Carrier**: código de aerolínea (AA, DL, UA...)
   - **Date**: fecha del vuelo
   - **Origin**: aeropuerto de origen (ATL, JFK...)
   - **Destination**: aeropuerto de destino
3. Pulsa **Submit**
4. La predicción aparece en tiempo real vía WebSocket

---

## 🔄 Reentrenamiento del Modelo

El reentrenamiento se gestiona mediante un DAG de Apache Airflow (`retrain_flight_delay_model`) con planificación `@weekly`.

```bash
# Reentrenar manualmente (Docker)
./practica.sh → opción 3

# Reentrenar manualmente (K8s)
./practica.sh → opción 4
```

El proceso completo:

```
Airflow DAG trigger
    │
    ▼
spark-submit train_spark_mllib_model_iceberg.py
    │
    ├── Lee datos de Iceberg (MinIO warehouse)
    ├── Bucketiza ArrDelay en 4 categorías
    ├── StringIndexer para Carrier, Origin, Dest, Route
    ├── VectorAssembler → Features_vec
    ├── RandomForestClassifier.fit()
    ├── Guarda modelos → s3a://flight-data/models/
    ├── Evalúa accuracy
    └── Registra métricas en MLflow
```

Los modelos guardados en MinIO:

```
flight-data/models/
├── arrival_bucketizer_2.0.bin
├── numeric_vector_assembler.bin
├── spark_random_forest_classifier.flight_delays.5.0.bin
├── string_indexer_model_Carrier.bin
├── string_indexer_model_Dest.bin
├── string_indexer_model_Origin.bin
└── string_indexer_model_Route.bin
```

---

## 🌐 URLs de Acceso

### Docker Compose

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| **Flask (predicción)** | `http://IP:5001/flights/delays/predict_kafka` | — |
| Spark UI | `http://IP:8080` | — |
| Grafana | `http://IP:3000` | admin/admin |
| Prometheus | `http://IP:9090` | — |
| MinIO Console | `http://IP:9001` | minioadmin/minioadmin |
| MLflow | `http://IP:5002` | — |
| Airflow | `http://IP:8081` | admin/admin |

### Kubernetes (GKE)

| Servicio | Puerto NodePort | Credenciales |
|----------|----------------|--------------|
| **Flask** | `NODE_IP:30001` | — |
| Grafana | `NODE_IP:30300` | admin/admin |
| Prometheus | `NODE_IP:30909` | — |
| MinIO Console | `NODE_IP:30901` | minioadmin/minioadmin |
| MLflow | `NODE_IP:30502` | — |
| Airflow | `NODE_IP:30808` | admin/admin |

> 💡 Obtén la IP del nodo con: `kubectl get nodes -o wide`

---

## 🐛 Errores Comunes y Soluciones

### Docker Compose

<details>
<summary>❌ <code>Cassandra no disponible</code> al arrancar</summary>

Cassandra tarda varios minutos en inicializarse. El script espera automáticamente, pero si falla:

```bash
docker logs cassandra --tail=50
# Esperar hasta ver: "Created default superuser role 'cassandra'"
docker exec cassandra cqlsh -e "describe keyspaces"
```
</details>

<details>
<summary>❌ Predicción no aparece en la web (spinner infinito)</summary>

Verificar que Spark Streaming está procesando:
```bash
docker logs spark-predictor --tail=50 | grep -E "Stream started|ERROR"
# Si no está activo:
docker compose restart spark-predictor
```
</details>

<details>
<summary>❌ <code>ModuleNotFoundError: No module named 'mlflow'</code> en reentrenamiento</summary>

```bash
docker exec spark-master pip install mlflow
# O reconstruir la imagen (ya incluye mlflow en el Dockerfile)
docker compose build spark-master spark-worker-1 spark-worker-2
```
</details>

---

### Kubernetes (GKE)

<details>
<summary>❌ Spark training falla con <code>RECEIVED SIGNAL TERM</code></summary>

El driver Spark necesita puertos fijos para comunicarse con los executors. Verificar que el Service `spark-master` expone los puertos 7078 y 7079:

```bash
kubectl get svc spark-master
# Debe mostrar: 7077/TCP,8080/TCP,7078/TCP,7079/TCP
```

Si faltan, aplicar el manifest actualizado:
```bash
kubectl apply -f k8s-gke/spark.yaml
```
</details>

<details>
<summary>❌ Airflow DAG en <code>queued</code> sin ejecutarse</summary>

El scheduler de Airflow muere frecuentemente con SQLite. Reiniciarlo:

```bash
AIRFLOW_POD=$(kubectl get pod -l app=airflow --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
kubectl exec $AIRFLOW_POD -- bash -c "nohup airflow scheduler >> /tmp/scheduler.log 2>&1 &"
```
</details>

<details>
<summary>❌ <code>cannot exec into a container in a completed pod</code></summary>

El pod de spark-master se reinició y el DAG tiene el nombre del pod antiguo. El DAG ya incluye `--field-selector=status.phase=Running` para evitarlo. Si persiste:

```bash
kubectl rollout restart deployment/spark-master
# Esperar y re-disparar el DAG
```
</details>

<details>
<summary>❌ MLflow vacío tras reiniciar el pod</summary>

MLflow necesita el PVC para persistir datos. Verificar que está aplicado:

```bash
kubectl get pvc mlflow-pvc
# Si no existe:
kubectl apply -f k8s-gke/mlflow.yaml
```
</details>

<details>
<summary>❌ <code>App requires more resource than any of Workers could have</code></summary>

Los workers no tienen suficiente memoria libre. Añadir límites explícitos al spark-submit:

```
--conf spark.executor.memory=1g
--conf spark.executor.cores=1
--conf spark.executor.memoryOverhead=256m
```
Estos parámetros ya están configurados en el DAG de K8s.
</details>

<details>
<summary>❌ Pod Airflow en CrashLoopBackOff por OOM</summary>

Aumentar el límite de memoria:

```bash
kubectl patch deployment airflow --patch '{
  "spec":{"template":{"spec":{"containers":[{
    "name":"airflow",
    "resources":{"requests":{"memory":"2Gi"},"limits":{"memory":"3Gi"}}
  }]}}}}'
```
</details>

---

## 👥 Autores

<table>
  <tr>
    <td align="center">
      <b>Javier Saguar Antona</b><br/>
      <a href="https://github.com/javisaguarantona">@javisaguarantona</a>
    </td>
    <td align="center">
      <b>Iria Lozano</b><br/>
      ETSIT UPM 2026
    </td>
  </tr>
</table>

---

## 📚 Referencias

- [Repositorio base de la práctica](https://github.com/Big-Data-ETSIT/practica_creativa)
- [Agile Data Science 2.0 — Russell Jurney](https://github.com/rjurney/Agile_Data_Code_2)
- [Bureau of Transportation Statistics](https://www.transtats.bts.gov/)
- [Apache Spark MLlib](https://spark.apache.org/mllib/)
- [Apache Iceberg](https://iceberg.apache.org/)

---

<div align="center">
<sub>Práctica Creativa Big Data · ETSIT UPM 2026</sub>
</div>