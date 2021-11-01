package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model.{ConsequenceEnrichedOutput, ConsequenceRawOutput, Dbnsfp_originalOutput, EnsemblMappingOutput}
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.loader.LoadResolver
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class ConsequencesSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile)))

  val normalized_consequences: DatasetConf = conf.getDataset("normalized_consequences")
  val dbnsfp_original: DatasetConf = conf.getDataset("normalized_dbnsfp_original")
  val enriched_consequences: DatasetConf = conf.getDataset("enriched_consequences")
  val normalized_ensembl_mapping: DatasetConf = conf.getDataset("normalized_ensembl_mapping")

  val data = Map(
    normalized_consequences.id -> Seq(ConsequenceRawOutput()).toDF(),
    dbnsfp_original.id -> Seq(Dbnsfp_originalOutput()).toDF,
    normalized_ensembl_mapping.id -> Seq(EnsemblMappingOutput()).toDF
  )

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File(enriched_consequences.location))
    spark.sql("CREATE DATABASE IF NOT EXISTS clin")

    data.foreach { case (id, df) =>
      val ds = conf.getDataset(id)

      LoadResolver
        .resolve(spark, conf)(ds.format, LoadType.OverWrite)
        .apply(ds, df)
    }
  }

  "consequences job" should "transform data in expected format" in {
    val result = new Consequences().transform(data).as[ConsequenceEnrichedOutput].collect().head
    result shouldBe ConsequenceEnrichedOutput(
      `created_on` = result.`created_on`,
      `updated_on` = result.`updated_on`)
  }

  "consequences job" should "run" in {
    new Consequences().run()

    enriched_consequences.read.show(false)
    val result = enriched_consequences.read.as[ConsequenceEnrichedOutput].collect().head
    result shouldBe ConsequenceEnrichedOutput(
      `created_on` = result.`created_on`,
      `updated_on` = result.`updated_on`)
  }
}


