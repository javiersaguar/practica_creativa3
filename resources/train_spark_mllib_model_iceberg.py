#!/usr/bin/env python
import sys, os

def main(base_path):
    if not base_path:
        base_path = "."

    APP_NAME = "train_spark_mllib_model.py"

    try:
        sc and spark
    except (NameError, UnboundLocalError):
        import pyspark
        import pyspark.sql
        sc = pyspark.SparkContext()
        spark = pyspark.sql.SparkSession(sc).builder \
            .appName(APP_NAME) \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.minio", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.minio.type", "hadoop") \
            .config("spark.sql.catalog.minio.warehouse", "s3a://flight-data/warehouse") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

    # MLflow setup
    import mlflow
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("flight_delay_prediction")

    with mlflow.start_run(run_name="k8s_iceberg_retrain"):

        from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, DateType, TimestampType
        from pyspark.sql.types import StructType, StructField
        from pyspark.sql.functions import udf, lit, concat

        # Read from Iceberg/MinIO
        features = spark.table("minio.flights.training_data")
        total_records = features.count()
        print(f"Total records: {total_records}")
        mlflow.log_param("total_records", total_records)
        mlflow.log_param("data_source", "iceberg_minio")
        mlflow.log_param("warehouse", "s3a://flight-data/warehouse")

        # Check nulls
        null_counts = [(column, features.where(features[column].isNull()).count()) for column in features.columns]
        cols_with_nulls = list(filter(lambda x: x[1] > 0, null_counts))
        print(cols_with_nulls)

        # Add Route
        features_with_route = features.withColumn(
            'Route',
            concat(features.Origin, lit('-'), features.Dest)
        )

        # Bucketizer
        from pyspark.ml.feature import Bucketizer
        splits = [-float("inf"), -15.0, 0, 30.0, float("inf")]
        arrival_bucketizer = Bucketizer(
            splits=splits,
            inputCol="ArrDelay",
            outputCol="ArrDelayBucket"
        )
        arrival_bucketizer_path = "s3a://flight-data/models/arrival_bucketizer_2.0.bin"
        arrival_bucketizer.write().overwrite().save(arrival_bucketizer_path)
        ml_bucketized_features = arrival_bucketizer.transform(features_with_route)
        mlflow.log_param("bucketizer_splits", str(splits))

        # String Indexers
        from pyspark.ml.feature import StringIndexer, VectorAssembler
        for column in ["Carrier", "Origin", "Dest", "Route"]:
            string_indexer = StringIndexer(inputCol=column, outputCol=column + "_index")
            string_indexer_model = string_indexer.fit(ml_bucketized_features)
            ml_bucketized_features = string_indexer_model.transform(ml_bucketized_features)
            ml_bucketized_features = ml_bucketized_features.drop(column)
            string_indexer_output_path = "s3a://flight-data/models/string_indexer_model_{}.bin".format(column)
            string_indexer_model.write().overwrite().save(string_indexer_output_path)

        # Vector Assembler
        numeric_columns = ["DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear"]
        index_columns = ["Carrier_index", "Origin_index", "Dest_index", "Route_index"]
        vector_assembler = VectorAssembler(
            inputCols=numeric_columns + index_columns,
            outputCol="Features_vec"
        )
        final_vectorized_features = vector_assembler.transform(ml_bucketized_features)
        vector_assembler_path = "s3a://flight-data/models/numeric_vector_assembler.bin"
        vector_assembler.write().overwrite().save(vector_assembler_path)
        for column in index_columns:
            final_vectorized_features = final_vectorized_features.drop(column)

        mlflow.log_param("numeric_features", str(numeric_columns))
        mlflow.log_param("algorithm", "RandomForestClassifier")

        # Train RandomForest
        from pyspark.ml.classification import RandomForestClassifier
        rfc = RandomForestClassifier(
            featuresCol="Features_vec",
            labelCol="ArrDelayBucket",
            predictionCol="Prediction",
            maxBins=4657,
            maxMemoryInMB=1024
        )
        mlflow.log_param("maxBins", 4657)
        mlflow.log_param("maxMemoryInMB", 1024)

        model = rfc.fit(final_vectorized_features)

        # Save model
        model_output_path = "s3a://flight-data/models/spark_random_forest_classifier.flight_delays.5.0.bin"
        model.write().overwrite().save(model_output_path)
        mlflow.log_param("model_path", model_output_path)

        # Evaluate
        predictions = model.transform(final_vectorized_features)
        from pyspark.ml.evaluation import MulticlassClassificationEvaluator
        evaluator = MulticlassClassificationEvaluator(
            predictionCol="Prediction",
            labelCol="ArrDelayBucket",
            metricName="accuracy"
        )
        accuracy = evaluator.evaluate(predictions)
        print(f"Accuracy = {accuracy}")
        mlflow.log_metric("accuracy", accuracy)

        # Distribution
        dist = predictions.groupBy("Prediction").count().collect()
        for row in dist:
            mlflow.log_metric(f"pred_class_{int(row['Prediction'])}_count", row['count'])
            print(f"  Prediction {row['Prediction']}: {row['count']}")

        mlflow.log_param("status", "SUCCESS")
        print(f"MLflow run completado. Accuracy={accuracy}")

if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else ".")
