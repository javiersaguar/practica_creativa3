package es.upm.dit.ging.predictor

// Clases Java usadas para enviar peticiones HTTP a la API REST de MLflow.
import java.net.{HttpURLConnection, URL, URLEncoder}
import java.nio.charset.StandardCharsets

// Componentes de Spark ML empleados para preparar datos, entrenar y evaluar.
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{Bucketizer, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat, lit}

// Utilidades para leer respuestas HTTP y controlar operaciones que pueden fallar.
import scala.io.Source
import scala.util.Try

// Punto de entrada del entrenamiento del modelo de retrasos.
object TrainModel {

  // Cliente REST minimo para registrar experimentos, parametros y metricas en MLflow.
  object MlflowRestClient {
    // Escapa caracteres especiales antes de incluir valores en un JSON.
    private def escapeJson(value: String): String =
      value.replace("\\", "\\\\").replace("\"", "\\\"")

    // Ejecuta una peticion HTTP contra MLflow y devuelve el cuerpo de la respuesta.
    private def request(baseUri: String, method: String, path: String, body: Option[String] = None): String = {
      // Construye la URL evitando una barra duplicada entre servidor y ruta.
      val url = new URL(baseUri.stripSuffix("/") + path)
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod(method)
      // Limita el tiempo de conexion y de espera de respuesta.
      connection.setConnectTimeout(5000)
      connection.setReadTimeout(15000)
      connection.setRequestProperty("Content-Type", "application/json")

      // Envia el JSON solo cuando la operacion incluye cuerpo.
      body.foreach { payload =>
        connection.setDoOutput(true)
        val bytes = payload.getBytes(StandardCharsets.UTF_8)
        connection.getOutputStream.write(bytes)
      }

      // Lee la respuesta normal o el detalle de error segun el codigo HTTP.
      val code = connection.getResponseCode
      val stream =
        if (code >= 200 && code < 300) connection.getInputStream
        else connection.getErrorStream
      val response = if (stream == null) "" else Source.fromInputStream(stream).mkString

      // Convierte cualquier respuesta no exitosa en una excepcion del entrenamiento.
      if (code < 200 || code >= 300) {
        throw new RuntimeException(s"MLflow request failed: $method $path code=$code body=$response")
      }
      response
    }

    // Extrae el primer grupo capturado por una expresion regular.
    private def firstMatch(pattern: String, text: String): Option[String] =
      pattern.r.findFirstMatchIn(text).map(_.group(1))

    // Recupera el experimento por nombre o lo crea si todavia no existe.
    def getOrCreateExperiment(baseUri: String, name: String): String = {
      // Codifica el nombre para poder enviarlo como parametro de la URL.
      val encodedName = URLEncoder.encode(name, StandardCharsets.UTF_8.toString)
      // Prueba las variantes GET y POST admitidas por distintas versiones de MLflow.
      val getResponse = Try {
        request(baseUri, "GET", s"/api/2.0/mlflow/experiments/get-by-name?experiment_name=$encodedName")
      }.orElse {
        Try {
          val body = s"""{"experiment_name":"${escapeJson(name)}"}"""
          request(baseUri, "POST", "/api/2.0/mlflow/experiments/get-by-name", Some(body))
        }
      }.toOption

      // Usa el id encontrado o crea un experimento nuevo y obtiene su id.
      getResponse
        .flatMap(firstMatch("\\\"experiment_id\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"", _))
        .getOrElse {
          val body = s"""{"name":"${escapeJson(name)}"}"""
          val createResponse = request(baseUri, "POST", "/api/2.0/mlflow/experiments/create", Some(body))
          firstMatch("\\\"experiment_id\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"", createResponse)
            .getOrElse(throw new RuntimeException(s"Could not parse MLflow experiment id: $createResponse"))
        }
    }

    // Abre una nueva ejecucion dentro del experimento seleccionado.
    def createRun(baseUri: String, experimentId: String): String = {
      val now = System.currentTimeMillis()
      val body = s"""{"experiment_id":"$experimentId","start_time":$now}"""
      val response = request(baseUri, "POST", "/api/2.0/mlflow/runs/create", Some(body))
      firstMatch("\\\"run_id\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"", response)
        .getOrElse(throw new RuntimeException(s"Could not parse MLflow run id: $response"))
    }

    // Registra un parametro de configuracion asociado a la ejecucion.
    def logParam(baseUri: String, runId: String, key: String, value: String): Unit = {
      val body =
        s"""{"run_id":"$runId","key":"${escapeJson(key)}","value":"${escapeJson(value)}"}"""
      request(baseUri, "POST", "/api/2.0/mlflow/runs/log-parameter", Some(body))
    }

    // Registra una metrica numerica junto con su instante de medida.
    def logMetric(baseUri: String, runId: String, key: String, value: Double): Unit = {
      val now = System.currentTimeMillis()
      val body =
        s"""{"run_id":"$runId","key":"${escapeJson(key)}","value":$value,"timestamp":$now,"step":0}"""
      request(baseUri, "POST", "/api/2.0/mlflow/runs/log-metric", Some(body))
    }

    // Finaliza la ejecucion de MLflow con estado FINISHED o FAILED.
    def finishRun(baseUri: String, runId: String, status: String): Unit = {
      val now = System.currentTimeMillis()
      val body = s"""{"run_id":"$runId","status":"$status","end_time":$now}"""
      request(baseUri, "POST", "/api/2.0/mlflow/runs/update", Some(body))
    }
  }

  // Flujo principal ejecutado por spark-submit.
  def main(args: Array[String]): Unit = {
    println("Flight delay training starting...")

    // Crea o reutiliza la sesion proporcionada por el cluster Spark.
    val spark = SparkSession
      .builder()
      .appName("FlightDelayTrainingCluster")
      .getOrCreate()

    // Habilita la sintaxis $"columna" para referenciar columnas del DataFrame.
    import spark.implicits._

    // Configura Hadoop S3A para acceder a MinIO desde el driver y los ejecutores.
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    // Endpoint y credenciales pueden llegar como variables de entorno o propiedades Java.
    hadoopConf.set("fs.s3a.endpoint", sys.env.getOrElse("S3_ENDPOINT", sys.props.getOrElse("S3_ENDPOINT", "http://minio:9000")))
    hadoopConf.set("fs.s3a.access.key", sys.env.getOrElse("AWS_ACCESS_KEY_ID", sys.props.getOrElse("AWS_ACCESS_KEY_ID", "minioadmin")))
    hadoopConf.set("fs.s3a.secret.key", sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", sys.props.getOrElse("AWS_SECRET_ACCESS_KEY", "minioadmin")))
    // El acceso por ruta es necesario para servidores S3 compatibles como MinIO.
    hadoopConf.set("fs.s3a.path.style.access", sys.env.getOrElse("S3_PATH_STYLE_ACCESS", sys.props.getOrElse("S3_PATH_STYLE_ACCESS", "true")))
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    // La practica usa HTTP interno y credenciales estaticas sencillas.
    hadoopConf.set("fs.s3a.connection.ssl.enabled", sys.env.getOrElse("S3_SSL_ENABLED", sys.props.getOrElse("S3_SSL_ENABLED", "false")))
    hadoopConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    // Muestra donde se esta ejecutando realmente la aplicacion.
    println("Spark master: " + spark.sparkContext.master)
    println("Spark deploy mode: " + spark.conf.get("spark.submit.deployMode", "client"))

    // Rutas, servicios e hiperparametros configurables del entrenamiento.
    val trainingTable = sys.env.getOrElse("TRAINING_TABLE", sys.props.getOrElse("TRAINING_TABLE", "minio.flights.training_data"))
    val modelBasePath = sys.env.getOrElse("MODEL_BASE_PATH", sys.props.getOrElse("MODEL_BASE_PATH", "s3a://flight-data/models")).stripSuffix("/")
    val mlflowUri = sys.env.getOrElse("MLFLOW_TRACKING_URI", sys.props.getOrElse("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    val experimentName = sys.env.getOrElse("MLFLOW_EXPERIMENT_NAME", sys.props.getOrElse("MLFLOW_EXPERIMENT_NAME", "flight_delay_prediction"))
    // Si una variable no es un entero valido se utiliza el valor por defecto.
    val maxBins = sys.env.get("MAX_BINS").flatMap(v => Try(v.toInt).toOption).getOrElse(4657)
    val maxDepth = sys.env.get("MAX_DEPTH").flatMap(v => Try(v.toInt).toOption).getOrElse(10)
    val numTrees = sys.env.get("NUM_TREES").flatMap(v => Try(v.toInt).toOption).getOrElse(5)

    // Guarda el identificador para actualizar el estado de MLflow al terminar.
    var runId: Option[String] = None

    // Prepara el experimento de MLflow antes de comenzar el trabajo pesado.
    try {
      val experimentId = MlflowRestClient.getOrCreateExperiment(mlflowUri, experimentName)
      runId = Some(MlflowRestClient.createRun(mlflowUri, experimentId))
      // Registra la configuracion necesaria para reproducir el entrenamiento.
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
      // El entrenamiento se detiene si no puede quedar registrado en MLflow.
      case e: Exception =>
        System.err.println("MLflow tracking failed: " + e.getMessage)
        throw e
    }

    // Carga los datos, prepara las caracteristicas y entrena el modelo.
    try {
      // Lee la tabla Iceberg registrada en el catalogo configurado por spark-submit.
      val features = spark.table(trainingTable)
      val trainingRecords = features.count()
      println("Training records: " + trainingRecords)
      runId.foreach(id => MlflowRestClient.logMetric(mlflowUri, id, "training_records", trainingRecords.toDouble))

      // Crea una categoria de ruta combinando aeropuerto de origen y destino.
      val featuresWithRoute = features.withColumn("Route", concat($"Origin", lit("-"), $"Dest"))

      // Convierte el retraso de llegada en cuatro clases para clasificacion.
      val arrivalBucketizer = new Bucketizer()
        .setSplits(Array(Double.NegativeInfinity, -15.0, 0.0, 30.0, Double.PositiveInfinity))
        .setInputCol("ArrDelay")
        .setOutputCol("ArrDelayBucket")

      // Guarda el transformador para aplicar exactamente los mismos cortes al predecir.
      arrivalBucketizer.write.overwrite().save(s"$modelBasePath/arrival_bucketizer_2.0.bin")
      var mlFeatures = arrivalBucketizer.transform(featuresWithRoute)

      // Convierte variables de texto en indices numericos aprendidos de los datos.
      Seq("Carrier", "Origin", "Dest", "Route").foreach { column =>
        val indexer = new StringIndexer()
          .setInputCol(column)
          .setOutputCol(column + "_index")
          // Reserva un indice adicional para valores desconocidos durante la prediccion.
          .setHandleInvalid("keep")

        // Ajusta, transforma y persiste un indexador independiente por columna.
        val model = indexer.fit(mlFeatures)
        mlFeatures = model.transform(mlFeatures).drop(column)
        model.write.overwrite().save(s"$modelBasePath/string_indexer_model_$column.bin")
      }

      // Define el orden exacto de las variables que formaran el vector del modelo.
      val numericColumns = Array("DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear")
      val indexColumns = Array("Carrier_index", "Origin_index", "Dest_index", "Route_index")
      // Une columnas numericas y categoricas indexadas en un unico vector de entrada.
      val vectorAssembler = new VectorAssembler()
        .setInputCols(numericColumns ++ indexColumns)
        .setOutputCol("Features_vec")
        .setHandleInvalid("keep")

      // Persiste el ensamblador y genera el DataFrame final para Spark ML.
      vectorAssembler.write.overwrite().save(s"$modelBasePath/numeric_vector_assembler.bin")
      val finalVectorizedFeatures = vectorAssembler.transform(mlFeatures).drop(indexColumns: _*)

      // Configura el clasificador Random Forest y sus hiperparametros.
      val classifier = new RandomForestClassifier()
        .setFeaturesCol("Features_vec")
        .setLabelCol("ArrDelayBucket")
        .setPredictionCol("Prediction")
        .setNumTrees(numTrees)
        .setMaxDepth(maxDepth)
        // Debe ser suficiente para las categorias de las variables indexadas.
        .setMaxBins(maxBins)
        // Limita la memoria usada para agregar estadisticas durante el entrenamiento.
        .setMaxMemoryInMB(1024)

      // Entrena el bosque y guarda el modelo resultante en MinIO.
      val model = classifier.fit(finalVectorizedFeatures)
      model.write.overwrite().save(s"$modelBasePath/spark_random_forest_classifier.flight_delays.5.0.bin")

      // Evalua el modelo sobre los mismos registros usados para entrenarlo.
      val predictions = model.transform(finalVectorizedFeatures)
      val evaluator = new MulticlassClassificationEvaluator()
        .setPredictionCol("Prediction")
        .setLabelCol("ArrDelayBucket")
        // Accuracy representa la proporcion total de clases acertadas.
        .setMetricName("accuracy")
      val accuracy = evaluator.evaluate(predictions)
      println("Accuracy = " + accuracy)
      // Muestra cuantas filas han sido asignadas a cada clase prevista.
      predictions.groupBy("Prediction").count().show()

      // Registra el resultado y marca la ejecucion de MLflow como completada.
      runId.foreach { id =>
        MlflowRestClient.logMetric(mlflowUri, id, "accuracy", accuracy)
        MlflowRestClient.finishRun(mlflowUri, id, "FINISHED")
      }

      println("Training completed successfully")
    } catch {
      // Si falla cualquier fase, intenta marcar el run como FAILED y propaga el error.
      case e: Exception =>
        runId.foreach(id => Try(MlflowRestClient.finishRun(mlflowUri, id, "FAILED")))
        throw e
    } finally {
      // Libera siempre los recursos de Spark, tanto en exito como en error.
      spark.stop()
    }
  }
}
