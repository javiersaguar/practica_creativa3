#!/bin/bash
set -e

KAFKA_HOME=/opt/kafka
CLUSTER_ID="dGhpcy1pcy1teS1rYWZrYS1pZA"

# Configurar listeners
sed -i "s|^#listeners=.*|listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093|" \
    $KAFKA_HOME/config/server.properties
sed -i "s|^#advertised.listeners=.*|advertised.listeners=PLAINTEXT://${KAFKA_ADVERTISED_HOST:-localhost}:9092|" \
    $KAFKA_HOME/config/server.properties
sed -i "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://${KAFKA_ADVERTISED_HOST:-localhost}:9092|" \
    $KAFKA_HOME/config/server.properties

$KAFKA_HOME/bin/kafka-storage.sh format \
    --standalone \
    -t $CLUSTER_ID \
    -c $KAFKA_HOME/config/server.properties \
    --ignore-formatted

$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &

echo "Esperando a que Kafka arranque..."
sleep 15

$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic flight-delay-ml-request \
    --partitions 1 --replication-factor 1

$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic flight-delay-ml-response \
    --partitions 1 --replication-factor 1

echo "Topics creados"
wait
