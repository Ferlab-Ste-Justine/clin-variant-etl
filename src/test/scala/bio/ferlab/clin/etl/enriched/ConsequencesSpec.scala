package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model.{ConsequenceEnrichedOutput, ConsequenceRawOutput, Dbnsfp_originalOutput}
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.spark3.config.{Configuration, ConfigurationLoader, DatasetConf, StorageConf}
import bio.ferlab.datalake.spark3.loader.{LoadResolver, LoadType}
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class ConsequencesSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_storage", this.getClass.getClassLoader.getResource(".").getFile)))

  val normalized_consequences: DatasetConf = conf.getDataset("normalized_consequences")
  val dbnsfp_original: DatasetConf = conf.getDataset("dbnsfp_original")

  val data = Map(
    normalized_consequences.id -> Seq(ConsequenceRawOutput()).toDF(),
    dbnsfp_original.id -> Seq(Dbnsfp_originalOutput()).toDF
  )

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    spark.sql("CREATE DATABASE IF NOT EXISTS clin")

    data.foreach { case (id, df) =>
      val ds = conf.getDataset(id)

      LoadResolver
        .resolve(spark, conf)(ds.format, LoadType.OverWrite)
        .apply(ds, df)
    }
  }

  "consequences job" should "transform data in expected format" in {
    val result = new Consequences("BAT0").transform(data).as[ConsequenceEnrichedOutput].collect().head
    result shouldBe ConsequenceEnrichedOutput(
      `createdOn` = result.`createdOn`,
      `updatedOn` = result.`updatedOn`)
  }

  "consequences job" should "run" in {
    new Consequences("BAT0").run()
    val result = spark.table("clin.consequences").as[ConsequenceEnrichedOutput].collect().head
    result shouldBe ConsequenceEnrichedOutput(
      `createdOn` = result.`createdOn`,
      `updatedOn` = result.`updatedOn`)
  }
}


