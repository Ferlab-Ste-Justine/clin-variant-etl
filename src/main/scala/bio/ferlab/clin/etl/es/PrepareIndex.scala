package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.RunType
import bio.ferlab.datalake.spark3.public.SparkApp

object PrepareIndex extends SparkApp {

  val Array(_, jobName, releaseId, runType) = args

  implicit val (conf, spark) = init()

  spark.sparkContext.setLogLevel("ERROR")

  //TODO include the following code in [[SparkApp]]
  val rt = runType match {
    case "first_load" => RunType.FIRST_LOAD
    case "sample_load" => RunType.SAMPLE_LOAD
    case _ => RunType.INCREMENTAL_LOAD
  }

  println(s"Job: $jobName")
  println(s"releaseId: $releaseId")
  println(s"runType: $rt")

  jobName match {
    case "gene_centric" => new PrepareGeneCentric(releaseId).run(rt)
    case "gene_suggestions" => new PrepareGeneSuggestions(releaseId).run(rt)
    case "variant_centric" => new PrepareVariantCentric(releaseId).run(rt)
    case "variant_suggestions" => new PrepareVariantSuggestions(releaseId).run(rt)
  }

}

