package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.model.normalized.NormalizedExomiser
import bio.ferlab.clin.model.raw.RawExomiser
import bio.ferlab.clin.testutils.{WithSparkSession, WithTestConfig}
import bio.ferlab.datalake.commons.config.DatasetConf
import org.apache.spark.sql.functions.input_file_name
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExomiserSpec extends AnyFlatSpec with WithSparkSession with WithTestConfig with Matchers {

  import spark.implicits._

  val mainDestination: DatasetConf = conf.getDataset("normalized_exomiser")
  val raw_exomiser: DatasetConf = conf.getDataset("raw_exomiser")

  val job1 = new Exomiser("BAT1")
  val job2 = new Exomiser("BAT2")

  it should "extract all files from the batch" in {
    val result = job2.extract()(spark)(raw_exomiser.id)

    result
      .as[RawExomiser]
      .withColumn("input_file_name", input_file_name())
      .groupBy("input_file_name")
      .count()
      .count() shouldBe 2
  }

  it should "normalize exomiser data" in {
    val data = job1.extract()
    val result = job1.transformSingle(data)

    result
      .as[NormalizedExomiser]
      .select("aliquot_id")
      .distinct()
      .as[String]
      .collect() should contain theSameElementsAs Seq("aliquot1")
  }
}
