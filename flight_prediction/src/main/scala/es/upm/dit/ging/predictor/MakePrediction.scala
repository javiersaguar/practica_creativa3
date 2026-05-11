package es.upm.dit.ging.predictor
import com.mongodb.spark._
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{Bucketizer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.functions.{concat, from_json, lit, to_json, struct}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MakePrediction {

  def main(args: Array[String]): Unit = {
    println("Fligth predictor starting...")

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()
    import spark.implicits._

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3a.endpoint", sys.env.getOrElse("S3_ENDPOINT", "http://minio:9000"))
    hadoopConf.set("fs.s3a.access.key", sys.env.getOrElse("AWS_ACCESS_KEY_ID", "minioadmin"))
    hadoopConf.set("fs.s3a.secret.key", sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "minioadmin"))
    hadoopConf.set("fs.s3a.path.style.access", sys.env.getOrElse("S3_PATH_STYLE_ACCESS", "true"))
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", sys.env.getOrElse("S3_SSL_ENABLED", "false"))
    hadoopConf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )

    println("Spark master: " + spark.sparkContext.master)

    val modelBasePath = sys.env
      .getOrElse("MODEL_BASE_PATH", "s3a://flight-data/models")
      .stripSuffix("/")

    def modelPath(name: String): String = "%s/%s".format(modelBasePath, name)

    //Load the arrival delay bucketizer
    val arrivalBucketizerPath = modelPath("arrival_bucketizer_2.0.bin")
    print(arrivalBucketizerPath.toString())
    val arrivalBucketizer = Bucketizer.load(arrivalBucketizerPath)
    val columns= Seq("Carrier","Origin","Dest","Route")

    //Load all the string field vectorizer pipelines into a dict
    val stringIndexerModelPath =  columns.map(n => modelPath("string_indexer_model_%s.bin".format(n)))
    val stringIndexerModel = stringIndexerModelPath.map{n => StringIndexerModel.load(n.toString)}
    val stringIndexerModels  = (columns zip stringIndexerModel).toMap

    // Load the numeric vector assembler
    val vectorAssemblerPath = modelPath("numeric_vector_assembler.bin")
    val vectorAssembler = VectorAssembler.load(vectorAssemblerPath)

    // Load the classifier model
    val randomForestModelPath = modelPath("spark_random_forest_classifier.flight_delays.5.0.bin")
    val rfc = RandomForestClassificationModel.load(randomForestModelPath)

    //Process Prediction Requests in Streaming
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
      .option("subscribe", "flight-delay-ml-request")
      .load()
    df.printSchema()

    val flightJsonDf = df.selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("Origin", DataTypes.StringType)
      .add("FlightNum", DataTypes.StringType)
      .add("DayOfWeek", DataTypes.IntegerType)
      .add("DayOfYear", DataTypes.IntegerType)
      .add("DayOfMonth", DataTypes.IntegerType)
      .add("Dest", DataTypes.StringType)
      .add("DepDelay", DataTypes.DoubleType)
      .add("Prediction", DataTypes.StringType)
      .add("Timestamp", DataTypes.TimestampType)
      .add("FlightDate", DataTypes.DateType)
      .add("Carrier", DataTypes.StringType)
      .add("UUID", DataTypes.StringType)
      .add("Distance", DataTypes.DoubleType)
      .add("Carrier_index", DataTypes.DoubleType)
      .add("Origin_index", DataTypes.DoubleType)
      .add("Dest_index", DataTypes.DoubleType)
      .add("Route_index", DataTypes.DoubleType)

    val flightNestedDf = flightJsonDf.select(from_json($"value", struct).as("flight"))
    flightNestedDf.printSchema()

    // DataFrame for Vectorizing string fields with the corresponding pipeline for that column
    val flightFlattenedDf = flightNestedDf.selectExpr("flight.Origin",
      "flight.DayOfWeek","flight.DayOfYear","flight.DayOfMonth","flight.Dest",
      "flight.DepDelay","flight.Timestamp","flight.FlightDate",
      "flight.Carrier","flight.UUID","flight.Distance")
    flightFlattenedDf.printSchema()

    val predictionRequestsWithRouteMod = flightFlattenedDf.withColumn(
      "Route",
                concat(
                  flightFlattenedDf("Origin"),
                  lit('-'),
                  flightFlattenedDf("Dest")
                )
    )

    // Dataframe for Vectorizing numeric columns
    val flightFlattenedDf2 = flightNestedDf.selectExpr("flight.Origin",
      "flight.DayOfWeek","flight.DayOfYear","flight.DayOfMonth","flight.Dest",
      "flight.DepDelay","flight.Timestamp","flight.FlightDate",
      "flight.Carrier","flight.UUID","flight.Distance",
      "flight.Carrier_index","flight.Origin_index","flight.Dest_index","flight.Route_index")
    flightFlattenedDf2.printSchema()

    val predictionRequestsWithRouteMod2 = flightFlattenedDf2.withColumn(
      "Route",
      concat(
        flightFlattenedDf2("Origin"),
        lit('-'),
        flightFlattenedDf2("Dest")
      )
    )

    // Vectorize string fields with the corresponding pipeline for that column
    // Turn category fields into categoric feature vectors, then drop intermediate fields
    val predictionRequestsWithRoute = stringIndexerModel.map(n=>n.transform(predictionRequestsWithRouteMod))

    //Vectorize numeric columns: DepDelay, Distance and index columns
    val vectorizedFeatures = vectorAssembler.setHandleInvalid("keep").transform(predictionRequestsWithRouteMod2)

    // Inspect the vectors
    vectorizedFeatures.printSchema()

    // Drop the individual index columns
    val finalVectorizedFeatures = vectorizedFeatures
        .drop("Carrier_index")
        .drop("Origin_index")
        .drop("Dest_index")
        .drop("Route_index")

    // Inspect the finalized features
    finalVectorizedFeatures.printSchema()

    // Make the prediction
    val predictions = rfc.transform(finalVectorizedFeatures)
      .drop("Features_vec")

    // Drop the features vector and prediction metadata to give the original fields
    val finalPredictions = predictions.drop("indices").drop("values").drop("rawPrediction").drop("probability")

    // Inspect the output
    finalPredictions.printSchema()

    // Write to MongoDB
    val mongoWriter = finalPredictions
      .writeStream
      .format("mongodb")
      .option("connection.uri", sys.env.getOrElse("MONGO_URI", "mongodb://127.0.0.1:27017"))
      .option("database", "agile_data_science")
      .option("collection", "flight_delay_ml_response")
      .option("checkpointLocation", "/tmp/checkpoint_mongo")
      .outputMode("append")
      .start()

    // Write to Kafka (flight-delay-ml-response topic)
    val kafkaWriter = finalPredictions
      .selectExpr("CAST(UUID AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
      .option("topic", "flight-delay-ml-response")
      .option("checkpointLocation", "/tmp/checkpoint_kafka")
      .outputMode("append")
      .start()

    // Rename columns to lowercase for Cassandra (no underscores)
    val cassandraPredictions = finalPredictions
      .withColumnRenamed("Origin", "origin")
      .withColumnRenamed("DayOfWeek", "dayofweek")
      .withColumnRenamed("DayOfYear", "dayofyear")
      .withColumnRenamed("DayOfMonth", "dayofmonth")
      .withColumnRenamed("Dest", "dest")
      .withColumnRenamed("DepDelay", "depdelay")
      .withColumnRenamed("Timestamp", "timestamp")
      .withColumnRenamed("FlightDate", "flightdate")
      .withColumnRenamed("Carrier", "carrier")
      .withColumnRenamed("UUID", "uuid")
      .withColumnRenamed("Distance", "distance")
      .withColumnRenamed("Route", "route")
      .withColumnRenamed("Prediction", "prediction")

    // Write to Cassandra
    val cassandraWriter = cassandraPredictions
      .writeStream
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "agile_data_science")
      .option("table", "flight_delay_classification_response")
      .option("checkpointLocation", "/tmp/checkpoint_cassandra")
      .outputMode("append")
      .start()

    // Console Output for predictions
    val consoleOutput = finalPredictions.writeStream
      .outputMode("append")
      .format("console")
      .start()

    consoleOutput.awaitTermination()
  }

}
