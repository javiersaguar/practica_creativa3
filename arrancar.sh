#!/bin/bash
echo "============================================"
echo "  ARRANCANDO STACK DOCKER COMPOSE"
echo "============================================"
cd ~/practica_creativa
docker compose up -d

echo "Esperando inicialización (10s)..."
sleep 10

echo "Reiniciando Flask y Spark (Cassandra tarda en arrancar)..."
docker compose stop flask spark-predictor
docker compose start flask spark-predictor

echo "Esperando inicialización final (15s)..."
sleep 15

echo "Configurando Airflow..."
docker exec airflow airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname Admin \
  --role Admin --email admin@example.com 2>/dev/null || true

echo "Configurando MinIO..."
docker exec minio sh -c \
  "mc alias set local http://localhost:9000 minioadmin minioadmin && mc mb local/flight-data 2>/dev/null || true" 2>/dev/null

echo "Configurando Grafana datasource..."
sleep 5
curl -s -X POST http://localhost:3000/api/datasources \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{
    "name": "Prometheus",
    "type": "prometheus",
    "url": "http://prometheus:9090",
    "access": "proxy",
    "isDefault": true
  }' > /dev/null 2>&1 || true

echo ""
~/practica_creativa/status.sh