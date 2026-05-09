#!/bin/bash
# ============================================
#   ARRANQUE KUBERNETES — GKE
#   Cluster: practica-k8s (europe-southwest1-a)
# ============================================

ZONE="europe-southwest1-a"
CLUSTER="practica-k8s"
NAMESPACE="default"
MANIFESTS=~/practica_creativa/k8s-gke

echo "============================================"
echo "  PASO 1 — Autenticar y conectar al cluster"
echo "============================================"
gcloud container clusters get-credentials $CLUSTER --zone $ZONE

echo ""
echo "============================================"
echo "  PASO 2 — Arrancar nodos (si están a 0)"
echo "============================================"
NODE_COUNT=$(kubectl get nodes --no-headers 2>/dev/null | wc -l)
if [ "$NODE_COUNT" -lt "2" ]; then
  echo "Escalando cluster a 2 nodos..."
  gcloud container clusters resize $CLUSTER --num-nodes=2 --zone $ZONE --quiet
  echo "Esperando que los nodos estén Ready (60s)..."
  sleep 60
  kubectl wait --for=condition=Ready nodes --all --timeout=120s
else
  echo "Cluster ya tiene $NODE_COUNT nodos activos."
fi

echo ""
echo "============================================"
echo "  PASO 3 — Aplicar manifests"
echo "============================================"
kubectl apply -f $MANIFESTS/mongo.yaml
kubectl apply -f $MANIFESTS/cassandra.yaml
kubectl apply -f $MANIFESTS/minio.yaml
kubectl apply -f $MANIFESTS/kafka.yaml
kubectl apply -f $MANIFESTS/spark.yaml
kubectl apply -f $MANIFESTS/spark-master-patch.yaml
kubectl apply -f $MANIFESTS/flask.yaml
kubectl apply -f $MANIFESTS/prometheus.yaml
kubectl apply -f $MANIFESTS/grafana.yaml
kubectl apply -f $MANIFESTS/mlflow.yaml
kubectl apply -f $MANIFESTS/airflow.yaml
kubectl apply -f $MANIFESTS/spark-predictor-patch.yaml

echo ""
echo "============================================"
echo "  PASO 4 — Esperar pods Running (120s)"
echo "============================================"
echo "Esperando que todos los pods arranquen..."
sleep 30
kubectl wait --for=condition=Ready pod -l app=mongo --timeout=120s 2>/dev/null || true
kubectl wait --for=condition=Ready pod -l app=kafka --timeout=120s 2>/dev/null || true
kubectl wait --for=condition=Ready pod -l app=flask --timeout=120s 2>/dev/null || true

echo ""
echo "============================================"
echo "  PASO 5 — Configurar MinIO"
echo "============================================"
MINIO_POD=$(kubectl get pod -l app=minio -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -n "$MINIO_POD" ]; then
  kubectl exec $MINIO_POD -- sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin && mc mb local/flight-data 2>/dev/null || true" 2>/dev/null || true
  echo "MinIO configurado."
fi

echo ""
echo "============================================"
echo "  PASO 6 — Estado final"
echo "============================================"
echo ""
echo "Pods:"
kubectl get pods -o wide
echo ""
echo "Servicios:"
kubectl get services
echo ""

# IPs externas de LoadBalancer / NodePort
FLASK_IP=$(kubectl get service flask -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}' 2>/dev/null)

echo "============================================"
echo "  URLS GKE"
echo "============================================"
if [ -n "$FLASK_IP" ]; then
  echo "  Flask (LoadBalancer):"
  echo "    http://$FLASK_IP:5001/flights/delays/predict_kafka"
else
  echo "  Flask (NodePort):"
  echo "    http://$NODE_IP:30001/flights/delays/predict_kafka"
fi
echo ""
echo "  Grafana:        (admin / admin)"
echo "    http://$NODE_IP:30300"
echo ""
echo "  Prometheus:"
echo "    http://$NODE_IP:30909"
echo ""
echo "  Spark UI:"
echo "    http://$NODE_IP:30080  (si expuesto)"
echo ""
echo "  MinIO consola:  (minioadmin / minioadmin)"
echo "    http://$NODE_IP:30901"
echo ""
echo "  MLflow:"
echo "    http://$NODE_IP:30502"
echo ""
echo "  Airflow:        (admin / admin)"
echo "    http://$NODE_IP:30808"
echo ""
echo "============================================"
echo "  PARA APAGAR EL CLUSTER (ahorra dinero):"
echo "  gcloud container clusters resize $CLUSTER --num-nodes=0 --zone $ZONE"
echo "============================================"