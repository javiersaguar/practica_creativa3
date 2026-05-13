package es.upm.dit.ging.predictor

import java.net.{HttpURLConnection, URL, URLEncoder}
import java.nio.charset.StandardCharsets

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{Bucketizer, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat, lit}

import scala.io.Source
import scala.util.Try

object TrainModel {

  object MlflowRestClient {
    private def escapeJson(value: String): String =
      value.replace("\\", "\\\\").replace("\"", "\\\"")

    private def request(baseUri: String, method: String, path: String, body: Option[String] = None): String = {
      val url = new URL(baseUri.stripSuffix("/") + path)
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod(method)
      connection.setConnectTimeout(5000)
      connection.setReadTimeout(15000)
      connection.setRequestProperty("Content-Type", "application/json")

      body.foreach { payload =>
        connection.setDoOutput(true)
        val bytes = payload.getBytes(StandardCharsets.UTF_8)
        connection.getOutputStream.write(bytes)
      }

      val code = connection.getResponseCode
      val stream =
        if (code >= 200 && code < 300) connection.getInputStream
        else connection.getErrorStream
      val response = if (stream == null) "" else Source.fromInputStream(stream).mkString

      if (code < 200 || code >= 300) {
        throw new RuntimeException(s"MLflow request failed: $method $path code=$code body=$response")
      }
      response
    }

    private def firstMatch(pattern: String, text: String): Option[String] =
      pattern.r.findFirstMatchIn(text).map(_.group(1))

    def getOrCreateExperiment(baseUri: String, name: String): String = {
      val encodedName = URLEncoder.encode(name, StandardCharsets.UTF_8.toString)
      val getResponse = Try {
        request(baseUri, "GET", s"/api/2.0/mlflow/experiments/get-by-name?experiment_name=$encodedName")
      }.orElse {
        Try {
          val body = s"""{"experiment_name":"${escapeJson(name)}"}"""
          request(baseUri, "POST", "/api/2.0/mlflow/experiments/get-by-name", Some(body))
        }
      }.toOption

      getResponse
        .flatMap(firstMatch("\\\"experiment_id\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"", _))
        .getOrElse {
          val body = s"""{"name":"${escapeJson(name)}"}"""
          val createResponse = request(baseUri, "POST", "/api/2.0/mlflow/experiments/create", Some(body))
          firstMatch("\\\"experiment_id\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"", createResponse)
            .getOrElse(throw new RuntimeException(s"Could not parse MLflow experiment id: $createResponse"))
        }
    }

    def createRun(baseUri: String, experimentId: String): String = {
      val now = System.currentTimeMillis()
      val body = s"""{"experiment_id":"$experimentId","start_time":$now}"""
      val response = request(baseUri, "POST", "/api/2.0/mlflow/runs/create", Some(body))
      firstMatch("\\\"run_id\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"", response)
        .getOrElse(throw new RuntimeException(s"Could not parse MLflow run id: $response"))
    }

    def logParam(baseUri: String, runId: String, key: String, value: String): Unit = {
      val body =
        s"""{"run_id":"$runId","key":"${escapeJson(key)}","value":"${escapeJson(value)}"}"""
      request(baseUri, "POST", "/api/2.0/mlflow/runs/log-parameter", Some(body))
    }

    def logMetric(baseUri: String, runId: String, key: String, value: Double): Unit = {
      val now = System.currentTimeMillis()
      val body =
        s"""{"run_id":"$runId","key":"${escapeJson(key)}","value":$value,"timestamp":$now,"step":0}"""
      request(baseUri, "POST", "/api/2.0/mlflow/runs/log-metric", Some(body))
    }

    def finishRun(baseUri: String, runId: String, status: String): Unit = {
      val now = System.currentTimeMillis()
      val body = s"""{"run_id":"$runId","status":"$status","end_time":$now}"""
      request(baseUri, "POST", "/api/2.0/mlflow/runs/update", Some(body))
    }
  }

  def main(args: Array[String]): Unit = {
    println("Flight delay training starting...")

    val spark = SparkSession
      .builder()
      .appName("FlightDelayTrainingCluster")
      .getOrCreate()

    import spark.implicits._

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3a.endpoint", sys.env.getOrElse("S3_ENDPOINT", sys.props.getOrElse("S3_ENDPOINT", "http://minio:9000")))
    hadoopConf.set("fs.s3a.access.key", sys.env.getOrElse("AWS_ACCESS_KEY_ID", sys.props.getOrElse("AWS_ACCESS_KEY_ID", "minioadmin")))
    hadoopConf.set("fs.s3a.secret.key", sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", sys.props.getOrElse("AWS_SECRET_ACCESS_KEY", "minioadmin")))
    hadoopConf.set("fs.s3a.path.style.access", sys.env.getOrElse("S3_PATH_STYLE_ACCESS", sys.props.getOrElse("S3_PATH_STYLE_ACCESS", "true")))
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", sys.env.getOrElse("S3_SSL_ENABLED", sys.props.getOrElse("S3_SSL_ENABLED", "false")))
    hadoopConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    println("Spark master: " + spark.sparkContext.master)
    println("Spark deploy mode: " + spark.conf.get("spark.submit.deployMode", "client"))

    val trainingTable = sys.env.getOrElse("TRAINING_TABLE", sys.props.getOrElse("TRAINING_TABLE", "minio.flights.training_data"))
    val modelBasePath = sys.env.getOrElse("MODEL_BASE_PATH", sys.props.getOrElse("MODEL_BASE_PATH", "s3a://flight-data/models")).stripSuffix("/")
    val mlflowUri = sys.env.getOrElse("MLFLOW_TRACKING_URI", sys.props.getOrElse("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    val experimentName = sys.env.getOrElse("MLFLOW_EXPERIMENT_NAME", sys.props.getOrElse("MLFLOW_EXPERIMENT_NAME", "flight_delay_prediction"))
    val maxBins = sys.env.get("MAX_BINS").flatMap(v => Try(v.toInt).toOption).getOrElse(4657)
    val maxDepth = sys.env.get("MAX_DEPTH").flatMap(v => Try(v.toInt).toOption).getOrElse(10)
    val numTrees = sys.env.get("NUM_TREES").flatMap(v => Try(v.toInt).toOption).getOrElse(5)

    var runId: Option[String] = None

    try {
      val experimentId = MlflowRestClient.getOrCreateExperiment(mlflowUri, experimentName)
      runId = Some(MlflowRestClient.createRun(mlflowUri, experimentId))
      runId.foreach { id =>
        MlflowRestClient.logParam(mlflowUri, id, "algorithm", "RandomForest")
        MlflowRestClient.logParam(mlflowUri, id, "data_source", trainingTable)
        MlflowRestClient.logParam(mlflowUri, id, "deploy_mode", spark.conf.get("spark.submit.deployMode", "client"))
        MlflowRestClient.logParam(mlflowUri, id, "model_base_path", modelBasePath)
        MlflowRestClient.logParam(mlflowUri, id, "maxBins", maxBins.toString)
        MlflowRestClient.logParam(mlflowUri, id, "maxDepth", maxDepth.toString)
        MlflowRestClient.logParam(mlflowUri, id, "numTrees", numTrees.toString)
      }
    } catch {
      case e: Exception =>
        System.err.println("MLflow tracking failed: " + e.getMessage)
        throw e
    }

    try {
      val features = spark.table(trainingTable)
      val trainingRecords = features.count()
      println("Training records: " + trainingRecords)
      runId.foreach(id => MlflowRestClient.logMetric(mlflowUri, id, "training_records", trainingRecords.toDouble))

      val featuresWithRoute = features.withColumn("Route", concat($"Origin", lit("-"), $"Dest"))

      val arrivalBucketizer = new Bucketizer()
        .setSplits(Array(Double.NegativeInfinity, -15.0, 0.0, 30.0, Double.PositiveInfinity))
        .setInputCol("ArrDelay")
        .setOutputCol("ArrDelayBucket")

      arrivalBucketizer.write.overwrite().save(s"$modelBasePath/arrival_bucketizer_2.0.bin")
      var mlFeatures = arrivalBucketizer.transform(featuresWithRoute)

      Seq("Carrier", "Origin", "Dest", "Route").foreach { column =>
        val indexer = new StringIndexer()
          .setInputCol(column)
          .setOutputCol(column + "_index")
          .setHandleInvalid("keep")

        val model = indexer.fit(mlFeatures)
        mlFeatures = model.transform(mlFeatures).drop(column)
        model.write.overwrite().save(s"$modelBasePath/string_indexer_model_$column.bin")
      }

      val numericColumns = Array("DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear")
      val indexColumns = Array("Carrier_index", "Origin_index", "Dest_index", "Route_index")
      val vectorAssembler = new VectorAssembler()
        .setInputCols(numericColumns ++ indexColumns)
        .setOutputCol("Features_vec")
        .setHandleInvalid("keep")

      vectorAssembler.write.overwrite().save(s"$modelBasePath/numeric_vector_assembler.bin")
      val finalVectorizedFeatures = vectorAssembler.transform(mlFeatures).drop(indexColumns: _*)

      val classifier = new RandomForestClassifier()
        .setFeaturesCol("Features_vec")
        .setLabelCol("ArrDelayBucket")
        .setPredictionCol("Prediction")
        .setNumTrees(numTrees)
        .setMaxDepth(maxDepth)
        .setMaxBins(maxBins)
        .setMaxMemoryInMB(1024)

      val model = classifier.fit(finalVectorizedFeatures)
      model.write.overwrite().save(s"$modelBasePath/spark_random_forest_classifier.flight_delays.5.0.bin")

      val predictions = model.transform(finalVectorizedFeatures)
      val evaluator = new MulticlassClassificationEvaluator()
        .setPredictionCol("Prediction")
        .setLabelCol("ArrDelayBucket")
        .setMetricName("accuracy")
      val accuracy = evaluator.evaluate(predictions)
      println("Accuracy = " + accuracy)
      predictions.groupBy("Prediction").count().show()

      runId.foreach { id =>
        MlflowRestClient.logMetric(mlflowUri, id, "accuracy", accuracy)
        MlflowRestClient.finishRun(mlflowUri, id, "FINISHED")
      }

      println("Training completed successfully")
    } catch {
      case e: Exception =>
        runId.foreach(id => Try(MlflowRestClient.finishRun(mlflowUri, id, "FAILED")))
        throw e
    } finally {
      spark.stop()
    }
  }
}
