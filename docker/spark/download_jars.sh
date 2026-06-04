#!/bin/bash
export HOME=/home/spark
su spark -c "/opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.mongodb.spark:mongo-spark-connector_2.13:10.4.1,com.datastax.spark:spark-cassandra-connector_2.13:3.5.1,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
  --conf spark.jars.ivy=/home/spark/.ivy2 \
  --class org.apache.spark.examples.SparkPi \
  /opt/spark/examples/jars/spark-examples_2.13-4.1.1.jar 1 2>/dev/null || true"
