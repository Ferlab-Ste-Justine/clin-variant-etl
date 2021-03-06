package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConsequencesSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  "consequences job" should "transform data in expected format" in {

    val df = Seq(VCFInput()).toDF()

    val result = Consequences.build(df, "BAT1").as[ConsequenceRawOutput].collect().head
    result shouldBe
      ConsequenceRawOutput("1", 69897, 69898, "T", "C", "rs200676709", List("synonymous_variant"), "LOW", "OR4F5", "ENSG00000186092",
        "ENST00000335137", "ENST00000335137", None, "Transcript", 1, "protein_coding", "SNV", EXON(Some(1), Some(1)), INTRON(None, None),
        Some("ENST00000335137.4:c.807T>C"), Some("ENSP00000334393.3:p.Ser269%3D"), "chr1:g.69897T>C", Some(807), Some(843), Some(269),
        AMINO_ACIDS(Some("S"), None), CODONS(Some("tcT"), Some("tcC")), true, true, None, Some("807T>C"), 2, "BAT1",
        `createdOn` = result.`createdOn`, `updatedOn` = result.`updatedOn`)
  }
}
