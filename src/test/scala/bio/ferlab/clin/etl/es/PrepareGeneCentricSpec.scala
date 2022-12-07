package bio.ferlab.clin.etl.es

import bio.ferlab.clin.model.{CnvCentricOutput, CnvEnrichedOutput, DONORS, ENRICHED_CNV_GENES, GeneCentricOutput, GenesOutput, VARIANT_PER_PATIENT, VariantEnrichedOutput}
import bio.ferlab.clin.testutils.{WithSparkSession, WithTestConfig}
import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, DatasetConf, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import org.apache.spark.sql.DataFrame
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PrepareGeneCentricSpec extends AnyFlatSpec with GivenWhenThen with WithTestConfig with WithSparkSession with Matchers {
  import spark.implicits._

  val genesDf: DataFrame = Seq(GenesOutput(), GenesOutput(`symbol` = "OR4F6")).toDF()
  val variantsDf: DataFrame = Seq(
    VariantEnrichedOutput(
      `locus` = "1-10000-A-TAA",
      `genes_symbol` = List("OR4F5", "OR4F4"),
      `donors` = List(DONORS(`patient_id` = "PA0001"), DONORS(`patient_id` = "PA0002"))),
    VariantEnrichedOutput(
      `locus` = "1-10000-A-TA",
      `genes_symbol` = List("OR4F5"),
      `donors` = List(DONORS(`patient_id` = "PA0003"), DONORS(`patient_id` = "PA0002")))
  ).toDF()
  val cnvDf: DataFrame = Seq(
    CnvEnrichedOutput(`patient_id` = "PA0001", genes = List(ENRICHED_CNV_GENES(`symbol` = Some("OR4F4")), ENRICHED_CNV_GENES(`symbol` = Some("OR4F5")), ENRICHED_CNV_GENES(`symbol` = Some("OR4F7")))),
    CnvEnrichedOutput(`patient_id` = "PA0002", `chromosome` = "2", genes = List(ENRICHED_CNV_GENES(`symbol` = Some("OR4F5"))))
  ).toDF()

  val destination: DatasetConf = conf.getDataset("es_index_gene_centric")
  val enriched_genes: DatasetConf = conf.getDataset("enriched_genes")
  val enriched_variants: DatasetConf = conf.getDataset("enriched_variants")
  val enriched_cnv: DatasetConf = conf.getDataset("enriched_cnv")

  val data = Map(
    enriched_genes.id -> genesDf,
    enriched_variants.id -> variantsDf,
    enriched_cnv.id -> cnvDf
  )

  "Gene_centric transform" should "return data as GeneCentricOutput" in {
    val result = new PrepareGeneCentric("re_000").transformSingle(data)
    result.columns should contain allElementsOf Seq("hash")
    result.as[GeneCentricOutput].collect() should contain allElementsOf Seq(
      GeneCentricOutput(symbol = "OR4F4", `entrez_gene_id` = 0, `omim_gene_id` = null, `hgnc` = null, `ensembl_gene_id` = null, `location` = null, name= null, `alias` = List(), `biotype` = null, `orphanet` = null,hpo=null,`omim` = null, chromosome=null, ddd=null, cosmic=null, `number_of_patients_snvs` = 2, `number_of_patients_cnvs` = 1, `number_of_snvs_per_patient` = List(VARIANT_PER_PATIENT("PA0002", 1), VARIANT_PER_PATIENT("PA0001", 1)), `number_of_cnvs_per_patient` = List(VARIANT_PER_PATIENT("PA0001", 1)), hash="63592aea532cb1c022cbc13ea463513df18baf57"),
      GeneCentricOutput(symbol = "OR4F5", `number_of_patients_snvs` = 3, `number_of_snvs_per_patient` = List(VARIANT_PER_PATIENT("PA0003", 1), VARIANT_PER_PATIENT("PA0001", 1), VARIANT_PER_PATIENT("PA0002", 2)), `number_of_patients_cnvs` = 2, `number_of_cnvs_per_patient` = List(VARIANT_PER_PATIENT("PA0001", 1), VARIANT_PER_PATIENT("PA0002", 1))),
      GeneCentricOutput(symbol = "OR4F6", `number_of_patients_snvs` = 0, `number_of_snvs_per_patient` = List(), `number_of_patients_cnvs` = 0,  `number_of_cnvs_per_patient` = List(), hash = "026aba5120030fcfbc29ebed8b2a1d78f90c07ad"),
      GeneCentricOutput(symbol = "OR4F7", `entrez_gene_id` = 0, `omim_gene_id` = null, `hgnc` = null, `ensembl_gene_id` = null, `location` = null, name= null, `alias` = List(), `biotype` = null, `orphanet` = null,hpo=null,`omim` = null, chromosome=null, ddd=null, cosmic=null, `number_of_patients_snvs` = 0, `number_of_snvs_per_patient` = List(), `number_of_patients_cnvs` = 1, `number_of_cnvs_per_patient` = List(VARIANT_PER_PATIENT("PA0001", 1)), hash="0bec4c2958cfbbfe4a212deef68da5d6eaf5ee29"))
  }
}

