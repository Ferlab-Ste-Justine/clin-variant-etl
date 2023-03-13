package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.enriched.Consequences._
import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.{WithSparkSession, WithTestConfig}
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.loader.LoadResolver
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class ConsequencesSpec extends AnyFlatSpec with WithSparkSession with WithTestConfig with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  val normalized_consequences: DatasetConf = conf.getDataset("normalized_consequences")
  val dbnsfp_original: DatasetConf = conf.getDataset("enriched_dbnsfp")
  val enriched_consequences: DatasetConf = conf.getDataset("enriched_consequences")
  val normalized_ensembl_mapping: DatasetConf = conf.getDataset("normalized_ensembl_mapping")
  val enriched_genes: DatasetConf = conf.getDataset("enriched_genes")

  private val data = Map(
    normalized_consequences.id -> Seq(NormalizedConsequences()).toDF(),
    dbnsfp_original.id -> Seq(Dbnsfp_originalOutput()).toDF,
    normalized_ensembl_mapping.id -> Seq(EnsemblMappingOutput()).toDF,
    enriched_genes.id -> Seq(GenesOutput()).toDF,
  )

  val etl = new Consequences()

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File(enriched_consequences.location))
    spark.sql("CREATE DATABASE IF NOT EXISTS clin")

    data.foreach { case (id, df) =>
      val ds = conf.getDataset(id)

      LoadResolver
        .write(spark, conf)(ds.format, LoadType.OverWrite)
        .apply(ds, df)
    }
  }

  "consequences job" should "transform data in expected format" in {
    val resultDf = etl.transformSingle(data)
    val result = resultDf.as[EnrichedConsequences].collect().head

    //    ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "EnrichedConsequences", resultDf, "src/test/scala/")

    result shouldBe EnrichedConsequences(
      `predictions` = null,
      `conservations` = null
    )
  }

  "consequences job" should "run" in {
    etl.run()

    val result = enriched_consequences.read.as[EnrichedConsequences].collect().head
    result shouldBe EnrichedConsequences(
      `predictions` = null,
      `conservations` = null
    )
  }

  "pickRandomCsqPerVariant" should "return a single consequence for each variant" in {
    val testDf = Seq(
      EnrichedConsequences(`chromosome` = "1", `start` = 1, `reference` = "A", `alternate` = "C", `ensembl_transcript_id` = "ENS1"),
      EnrichedConsequences(`chromosome` = "2", `start` = 2, `reference` = "C", `alternate` = "A", `ensembl_transcript_id` = "ENS2"),
      EnrichedConsequences(`chromosome` = "2", `start` = 2, `reference` = "C", `alternate` = "A", `ensembl_transcript_id` = "ENS3"),
      EnrichedConsequences(`chromosome` = "2", `start` = 2, `reference` = "C", `alternate` = "T", `ensembl_transcript_id` = "ENS4"),
    ).toDF()

    val result = testDf.pickRandomCsqPerVariant
    result.where($"chromosome" === "1").count() shouldBe 1
    result.where($"chromosome" === "2" and $"start" === 2 and $"reference" === "C" and $"alternate" === "A").count() shouldBe 1
    result.where($"chromosome" === "2" and $"start" === 2 and $"reference" === "C" and $"alternate" === "T").count() shouldBe 1
  }

  "withPickedColumn" should "pick one consequence per variant according to prioritization algorithm" in {
    val genesDf = Seq(
      GenesOutput(`ensembl_gene_id` = "ENSG_OMIM", `omim_gene_id` = "1"),
      GenesOutput(`ensembl_gene_id` = "ENSG_NO_OMIM", `omim_gene_id` = null),
    ).toDF()

    val csqDf = Seq(
      // Single csq is max_impact_score
      EnrichedConsequences(`chromosome` = "1", `ensembl_transcript_id` = "ENST1", `impact_score` = 1),
      EnrichedConsequences(`chromosome` = "1", `ensembl_transcript_id` = "ENST2", `impact_score` = 3), // picked

      // No csq in OMIM && no protein coding csq
      EnrichedConsequences(`chromosome` = "2", `ensembl_transcript_id` = "ENST1", `ensembl_gene_id` = "ENSG_NO_OMIM", `impact_score` = 1, `biotype` = "processed_transcript"), // picked at random
      EnrichedConsequences(`chromosome` = "2", `ensembl_transcript_id` = "ENST2", `ensembl_gene_id` = "ENSG_NO_OMIM", `impact_score` = 1, `biotype` = "processed_transcript"), // picked at random

      // No csq in OMIM && protein coding csq && mane select csq
      EnrichedConsequences(`chromosome` = "3", `ensembl_transcript_id` = "ENST1", `ensembl_gene_id` = "ENSG_NO_OMIM", `impact_score` = 2, `biotype` = "protein_coding", `mane_select` = true), // picked
      EnrichedConsequences(`chromosome` = "3", `ensembl_transcript_id` = "ENST2", `ensembl_gene_id` = "ENSG_NO_OMIM", `impact_score` = 2, `biotype` = "protein_coding", `mane_select` = false),

      // Csq in OMIM && mane select csq
      EnrichedConsequences(`chromosome` = "4", `ensembl_transcript_id` = "ENST1", `ensembl_gene_id` = "ENSG_OMIM", `impact_score` = 3, `mane_select` = true), // picked
      EnrichedConsequences(`chromosome` = "4", `ensembl_transcript_id` = "ENST2", `ensembl_gene_id` = "ENSG_OMIM", `impact_score` = 3, `mane_select` = false),

      // Csq in OMIM && no mane select csq && canonical csq
      EnrichedConsequences(`chromosome` = "5", `ensembl_transcript_id` = "ENST1", `ensembl_gene_id` = "ENSG_OMIM", `impact_score` = 4, `mane_select` = false, `canonical` = true), // picked
      EnrichedConsequences(`chromosome` = "5", `ensembl_transcript_id` = "ENST2", `ensembl_gene_id` = "ENSG_OMIM", `impact_score` = 4, `mane_select` = false, `canonical` = false),

      // Csq in OMIM && no mane select csq && no canonical csq && mane plus csq
      EnrichedConsequences(`chromosome` = "6", `ensembl_transcript_id` = "ENST1", `ensembl_gene_id` = "ENSG_OMIM", `impact_score` = 5, `mane_select` = false, `canonical` = false, `mane_plus` = true), // picked
      EnrichedConsequences(`chromosome` = "6", `ensembl_transcript_id` = "ENST2", `ensembl_gene_id` = "ENSG_OMIM", `impact_score` = 5, `mane_select` = false, `canonical` = false, `mane_plus` = false),

      // Csq in OMIM && no mane select csq && no canonical csq && no mane plus csq
      EnrichedConsequences(`chromosome` = "7", `ensembl_transcript_id` = "ENST1", `ensembl_gene_id` = "ENSG_OMIM", `impact_score` = 6, `mane_select` = false, `canonical` = false, `mane_plus` = false), // picked at random
      EnrichedConsequences(`chromosome` = "7", `ensembl_transcript_id` = "ENST2", `ensembl_gene_id` = "ENSG_OMIM", `impact_score` = 6, `mane_select` = false, `canonical` = false, `mane_plus` = false), // picked at random
    ).toDF().drop("picked")

    val result = etl.withPickedColumn(csqDf, genesDf)

    // Only one csq picked per variant
    result
      .where($"picked")
      .groupByLocus()
      .count()
      .select("count").as[String].collect() should contain only "1"

    // Single csq is max_impact_score
    result
      .where($"chromosome" === "1")
      .select("chromosome", "ensembl_transcript_id", "picked")
      .as[(String, String, Option[Boolean])].collect() should contain theSameElementsAs Seq(
      ("1", "ENST1", None),
      ("1", "ENST2", Some(true)),
    )

    // No csq in OMIM && no protein coding csq (picked at random)
    result.where($"chromosome" === "2" && $"picked").count() shouldBe 1

    // No csq in OMIM && protein coding csq && mane select csq
    result
      .where($"chromosome" === "3")
      .select("chromosome", "ensembl_transcript_id", "picked")
      .as[(String, String, Option[Boolean])].collect() should contain theSameElementsAs Seq(
      ("3", "ENST1", Some(true)),
      ("3", "ENST2", None),
    )

    // Csq in OMIM && mane select csq
    result
      .where($"chromosome" === "4")
      .select("chromosome", "ensembl_transcript_id", "picked")
      .as[(String, String, Option[Boolean])].collect() should contain theSameElementsAs Seq(
      ("4", "ENST1", Some(true)),
      ("4", "ENST2", None),
    )

    // Csq in OMIM && no mane select csq && canonical csq
    result
      .where($"chromosome" === "5")
      .select("chromosome", "ensembl_transcript_id", "picked")
      .as[(String, String, Option[Boolean])].collect() should contain theSameElementsAs Seq(
      ("5", "ENST1", Some(true)),
      ("5", "ENST2", None),
    )

    // Csq in OMIM && no mane select csq && no canonical csq && mane plus csq
    result
      .where($"chromosome" === "6")
      .select("chromosome", "ensembl_transcript_id", "picked")
      .as[(String, String, Option[Boolean])].collect() should contain theSameElementsAs Seq(
      ("6", "ENST1", Some(true)),
      ("6", "ENST2", None),
    )

    // Csq in OMIM && no mane select csq && no canonical csq && no mane plus csq (picked at random)
    result.where($"chromosome" === "7" && $"picked").count() shouldBe 1
  }
}


