package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.{WithSparkSession, WithTestConfig}
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SNVSpec extends AnyFlatSpec with WithSparkSession with WithTestConfig with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  val normalized_occurrences: DatasetConf = conf.getDataset("normalized_snv")

  "Enriched occurrences job" should "filter occurrences by zygosity" in {

    val occurrencesDf = Seq(
      NormalizedSNV(chromosome = "1"  , zygosity = "HET" , affected_status = true),
      NormalizedSNV(chromosome = "1"  , zygosity = "HOM" , affected_status = true),
      NormalizedSNV(chromosome = "1"  , zygosity = "UNK" , affected_status = true),
      NormalizedSNV(chromosome = "1"  , zygosity = "WT"  , affected_status = true),
      NormalizedSNV(chromosome = "1"  , zygosity = null  , affected_status = true)
    ).toDF()

    val inputData = Map(normalized_occurrences.id -> occurrencesDf)
    val df = new SNV().transformSingle(inputData)
    val result = df.as[NormalizedSNV].collect()

    result.length shouldBe 2

  }

}

