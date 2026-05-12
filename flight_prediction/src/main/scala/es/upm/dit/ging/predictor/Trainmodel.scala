package es.upm.dit.ging.predictor

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{Bucketizer, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TrainModel {

  class MlflowRestClient(trackingUri: String) {
    import java.net.{HttpURLConnection, URL}
    import java.nio.charset.StandardCharsets

    private def request(method: String, path: String, body: Option[String] = None): (Int, String) = {
      val url = new URL(s"$trackingUri$path")
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod(method)
      conn.setRequestProperty("Content-Type", "application/json")
      conn.setConnectTimeout(5000)
      conn.setReadTimeout(10000)
      body.foreach { b =>
        conn.setDoOutput(true)
        val os = conn.getOutputStream
        os.write(b.getBytes(StandardCharsets.UTF_8))
        os.close()
      }
      val code = conn.getResponseCode
      val stream = if (code >= 400) conn.getErrorStream else conn.getInputStream
      val response = if (stream != null) {
        val s = new java.util.Scanner(stream).useDelimiter("\\A")
        if (s.hasNext) s.next() else ""
      } else ""
      (code, response)
    }

    def getOrCreateExperiment(name: String): String = {
      val (code, body) = request("GET", s"/api/2.0/mlflow/experiments/get-by-name?experiment_name=${java.net.URLEncoder.encode(name, "UTF-8")}")
      if (code == 200) {
        val id = body.split("\"experiment_id\":\"").lift(1).map(_.split("\"")(0)).getOrElse("")
        if (id.nonEmpty) return id
      }
      val (code2, body2) = request("POST", "/api/2.0/mlflow/experiments/create",
        Some(s"""{"name":"$name"}"""))
      if (code2 == 200) {
        body2.split("\"experiment_id\":\"").lift(1).map(_.split("\"")(0)).getOrElse("0")
      } else {
        println(s"MLflow request failed: path=/api/2.0/mlflow/experiments/create code=$code2 body=$body2")
        "0"
      }
    }

    def createRun(experimentId: String): String = {
      val (code, body) = request("POST", "/api/2.0/mlflow/runs/create",
        Some(s"""{"experiment_id":"$experimentId","start_time":${System.currentTimeMillis()}}"""))
      if (code == 200) {
        body.split("\"run_id\":\"").lift(1).map(_.split("\"")(0)).getOrElse("")
      } else {
        println(s"MLflow create run failed: code=$code body=$body")
        ""
      }
    }

    def logParam(runId: String, key: String, value: String): Unit = {
      val (code, body) = request("POST", "/api/2.0/mlflow/runs/log-parameter",
        Some(s"""{"run_id":"$runId","key":"$key","value":"$value"}"""))
      if (code != 200) println(s"MLflow log param failed: code=$code body=$body")
    }

    def logMetric(runId: String, key: String, value: Double): Unit = {
      val (code, body) = request("POST", "/api/2.0/mlflow/runs/log-metric",
        Some(s"""{"run_id":"$runId","key":"$key","value":$value,"timestamp":${System.currentTimeMillis()},"step":0}"""))
      if (code != 200) println(s"MLflow log metric failed: code=$code body=$body")
    }

    def finishRun(runId: String, status: String = "FINISHED"): Unit = {
      val (code, body) = request("POST", "/api/2.0/mlflow/runs/update",
        Some(s"""{"run_id":"$runId","status":"$status","end_time":${System.currentTimeMillis()}}"""))
      if (code != 200) println(s"MLflow finish run failed: code=$code body=$body")
    }
  }

  def main(args: Array[String]): Unit = {
    val mlflowUri   = sys.env.getOrElse("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    val modelBase   = sys.env.getOrElse("MODEL_BASE_PATH", "s3a://flight-data/models")
    val trainingTable = sys.env.getOrElse("TRAINING_TABLE", "minio.flights.training_data")

    println("Flight delay training starting...")

    val spark = SparkSession.builder()
      .appName("es.upm.dit.ging.predictor.TrainModel")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.minio", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.minio.type", "hadoop")
      .config("spark.sql.catalog.minio.warehouse", sys.env.getOrElse("MINIO_WAREHOUSE", "s3a://flight-data/warehouse"))
      .getOrCreate()

    println(s"Spark master: ${spark.sparkContext.master}")
    println(s"Spark deploy mode: ${spark.sparkContext.deployMode}")

    val mlflow = new MlflowRestClient(mlflowUri)
    val experimentId = mlflow.getOrCreateExperiment("flight_delay_prediction")
    val runId = mlflow.createRun(experimentId)

    try {
      // Load training data from Iceberg
      val df = spark.table(trainingTable)
      val count = df.count()
      println(s"Total training records: $count")

      // Check for nulls
      val nullCols = df.columns.filter(c => df.filter(col(c).isNull).count() > 0).mkString(", ")
      println(s"Columns with nulls: $nullCols")

      val dfClean = df.na.drop()

      // Arrival delay bucketizer
      def modelPath(name: String, base: String): String = s"$base/$name"

      val bucketizer = new Bucketizer()
        .setInputCol("ArrDelay")
        .setOutputCol("ArrDelayBucket")
        .setSplits(Array(Double.NegativeInfinity, 0.0, 30.0, 60.0, Double.PositiveInfinity))

      val bucketizerModel = bucketizer.fit(dfClean)
      bucketizerModel.write.overwrite().save(modelPath("arrival_bucketizer_2.0.bin", modelBase))

      val dfBucketed = bucketizerModel.transform(dfClean)

      // String indexers
      val carrierIndexer = new StringIndexer().setInputCol("Carrier").setOutputCol("CarrierIndex").setHandleInvalid("keep")
      val originIndexer  = new StringIndexer().setInputCol("Origin").setOutputCol("OriginIndex").setHandleInvalid("keep")
      val destIndexer    = new StringIndexer().setInputCol("Dest").setOutputCol("DestIndex").setHandleInvalid("keep")
      val routeIndexer   = new StringIndexer().setInputCol("Route").setOutputCol("RouteIndex").setHandleInvalid("keep")

      val carrierModel = carrierIndexer.fit(dfBucketed)
      val originModel  = originIndexer.fit(dfBucketed)
      val destModel    = destIndexer.fit(dfBucketed)
      val routeModel   = routeIndexer.fit(dfBucketed)

      carrierModel.write.overwrite().save(modelPath("string_indexer_model_Carrier.bin", modelBase))
      originModel.write.overwrite().save(modelPath("string_indexer_model_Origin.bin", modelBase))
      destModel.write.overwrite().save(modelPath("string_indexer_model_Dest.bin", modelBase))
      routeModel.write.overwrite().save(modelPath("string_indexer_model_Route.bin", modelBase))

      val dfIndexed = routeModel.transform(destModel.transform(originModel.transform(carrierModel.transform(dfBucketed))))

      // Vector assembler
      val assembler = new VectorAssembler()
        .setInputCols(Array("DepDelay", "DayOfYear", "DayOfMonth", "DayOfWeek",
          "CarrierIndex", "OriginIndex", "DestIndex", "RouteIndex", "Distance"))
        .setOutputCol("features")
        .setHandleInvalid("skip")

      val assemblerModel = assembler.fit(dfIndexed)
      assemblerModel.write.overwrite().save(modelPath("numeric_vector_assembler.bin", modelBase))

      val dfFeatures = assemblerModel.transform(dfIndexed)

      // Train/test split
      val Array(trainDF, testDF) = dfFeatures.randomSplit(Array(0.8, 0.2), seed = 42)

      // Random Forest
      val rf = new RandomForestClassifier()
        .setLabelCol("ArrDelayBucket")
        .setFeaturesCol("features")
        .setNumTrees(5)
        .setMaxDepth(10)
        .setMaxBins(256)
        .setSeed(42)

      val rfModel = rf.fit(trainDF)
      rfModel.write.overwrite().save(modelPath("spark_random_forest_classifier.flight_delays.5.0.bin", modelBase))

      // Evaluate
      val predictions = rfModel.transform(testDF)
      val total = predictions.count().toDouble
      val correct = predictions.filter(col("prediction") === col("ArrDelayBucket")).count().toDouble
      val accuracy = correct / total
      println(s"Accuracy = $accuracy")

      // Prediction distribution
      predictions.groupBy("prediction").count().orderBy("prediction").collect().foreach { row =>
        println(s"Prediction ${row.getAs[Double]("prediction").toInt}: ${row.getAs[Long]("count")}")
      }

      // Log to MLflow
      if (runId.nonEmpty) {
        mlflow.logParam(runId, "algorithm", "RandomForest")
        mlflow.logParam(runId, "num_trees", "5")
        mlflow.logParam(runId, "max_depth", "10")
        mlflow.logParam(runId, "max_bins", "256")
        mlflow.logParam(runId, "deploy_mode", spark.sparkContext.deployMode)
        mlflow.logMetric(runId, "accuracy", accuracy)
        mlflow.logMetric(runId, "training_records", count.toDouble)
        mlflow.finishRun(runId)
      }

      println("Training completed successfully")
    } catch {
      case e: Exception =>
        println(s"Training failed: ${e.getMessage}")
        e.printStackTrace()
        if (runId.nonEmpty) mlflow.finishRun(runId, "FAILED")
        throw e
    } finally {
      spark.stop()
    }
  }
}