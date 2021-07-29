package bio.ferlab.clin.etl.enriched

import bio.ferlab.datalake.spark3.config.{Configuration, ConfigurationLoader}
import org.apache.spark.sql.SparkSession

object RunEnriched extends App {

  val Array(input, output, lastBatch, runType, configFile) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", value = false)
    .config("spark.delta.merge.repartitionBeforeWrite", value = true)
    //    .config("", value = 20) //Avoid too many small files as output of merging delta
    .enableHiveSupport()
    .appName(s"Run Enriched $runType").getOrCreate()

  //val minimumDateTime = LocalDateTime.of(1900, 1 , 1, 0, 0, 0)
  //val lastExecutionTimestamp = Try(Timestamp.valueOf(lastExecutionString)).getOrElse(Timestamp.valueOf(minimumDateTime))

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources(configFile)

  run(input, output, lastBatch, runType)

  def run(input: String, output: String, lastBatch: String, runType: String = "all")(implicit spark: SparkSession): Unit = {
    runType match {
      case "variants" => new Variants(lastBatch).run()
      case "consequences" => Consequences.run(input, output, lastBatch)
      case "all" =>
        new Variants(lastBatch).run()
        Consequences.run(input, output, lastBatch)
      case s: String => throw new IllegalArgumentException(s"Runtype [$s] unknown.")
    }

  }


}

