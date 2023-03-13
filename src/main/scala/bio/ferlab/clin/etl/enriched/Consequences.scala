package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.enriched.Consequences._
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.{formatted_consequences, locus, locusColumnNames}
import bio.ferlab.datalake.spark3.utils.DeltaUtils.{compact, vacuum}
import bio.ferlab.datalake.spark3.utils.RepartitionByColumns
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime

class Consequences()(implicit configuration: Configuration) extends ETLSingleDestination {

  override val mainDestination: DatasetConf = conf.getDataset("enriched_consequences")
  val normalized_consequences: DatasetConf = conf.getDataset("normalized_consequences")
  val dbnsfp_original: DatasetConf = conf.getDataset("enriched_dbnsfp")
  val normalized_ensembl_mapping: DatasetConf = conf.getDataset("normalized_ensembl_mapping")
  val enriched_genes: DatasetConf = conf.getDataset("enriched_genes")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      normalized_consequences.id -> normalized_consequences.read
        .where(col("updated_on") >= Timestamp.valueOf(lastRunDateTime)),
      dbnsfp_original.id -> dbnsfp_original.read,
      normalized_ensembl_mapping.id -> normalized_ensembl_mapping.read,
      enriched_genes.id -> enriched_genes.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val consequences = data(normalized_consequences.id)

    val ensembl_mapping = data(normalized_ensembl_mapping.id)
      .withColumn("uniprot_id", col("uniprot")(0)("id"))
      .select(
        $"ensembl_transcript_id",
        $"ensembl_gene_id",
        $"uniprot_id",
        //$"refseq_mrna_id",
        //$"refseq_protein_id",
        $"is_mane_select" as "mane_select",
        $"is_mane_plus" as "mane_plus",
        $"is_canonical")

    val chromosomes = consequences.select("chromosome").distinct().as[String].collect()

    val dbnsfp = data(dbnsfp_original.id).where(col("chromosome").isin(chromosomes: _*))

    val csq = consequences
      .drop("batch_id", "name", "end", "hgvsg", "variant_class", "ensembl_regulatory_id")
      .withColumn("consequence", formatted_consequences)
      .withColumnRenamed("impact", "vep_impact")

    val joinDBNSFP = joinWithDBNSFP(csq, dbnsfp)
    val joinEnsembl = joinDBNSFP
      .join(ensembl_mapping, Seq("ensembl_transcript_id", "ensembl_gene_id"), "left")
      //.join(mane_summary, Seq("ensembl_transcript_id", "ensembl_gene_id"), "left")
      .withColumn("mane_plus", coalesce(col("mane_plus"), lit(false)))
      .withColumn("mane_select", coalesce(col("mane_select"), lit(false)))
      .withColumn("canonical", coalesce(col("is_canonical"), lit(false)))
      .drop("is_canonical")

    withPickedColumn(joinEnsembl, data(enriched_genes.id))
  }

  override def publish()(implicit spark: SparkSession): Unit = {
    compact(mainDestination, RepartitionByColumns(Seq("chromosome"), Some(1), Seq(col("start"))))
    vacuum(mainDestination, 2)
  }

  def joinWithDBNSFP(csq: DataFrame, dbnsfp: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val dbnsfpRenamed =
      dbnsfp
        .withColumn("start", col("start").cast(LongType))
        .selectLocus(
          $"ensembl_transcript_id" as "ensembl_feature_id",
          struct(
            $"SIFT_converted_rankscore" as "sift_converted_rank_score",
            $"SIFT_pred" as "sift_pred",
            $"Polyphen2_HVAR_rankscore" as "polyphen2_hvar_score",
            $"Polyphen2_HVAR_pred" as "polyphen2_hvar_pred",
            $"FATHMM_converted_rankscore" as "fathmm_converted_rankscore",
            $"FATHMM_pred" as "fathmm_pred",
            $"CADD_raw_rankscore" as "cadd_score",
            $"CADD_phred" as "cadd_phred",
            $"DANN_rankscore" as "dann_score",
            $"REVEL_rankscore" as "revel_rankscore",
            $"LRT_converted_rankscore" as "lrt_converted_rankscore",
            $"LRT_pred" as "lrt_pred") as "predictions",
          struct($"phyloP17way_primate_rankscore" as "phylo_p17way_primate_rankscore") as "conservations",
        )

    csq
      .join(dbnsfpRenamed, Seq("chromosome", "start", "reference", "alternate", "ensembl_feature_id"), "left")
      .select(csq("*"), dbnsfpRenamed("predictions"), dbnsfpRenamed("conservations"))
      .withColumn(mainDestination.oid, col("created_on"))

  }

  def withPickedColumn(csq: DataFrame, genes: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val variantWindow = Window.partitionBy(locus: _*)
    // Max impact_score consequence by variant
    val maxImpactScores = csq
      .withColumn("max_impact_score", max("impact_score").over(variantWindow))

    // Consequences where impact_score is max_impact_score
    val maxImpactCsq: DataFrame = maxImpactScores
      .where($"max_impact_score" === $"impact_score")

    // Pick variants where only one consequence is max_impact_score
    val picked1MaxImpactCsq = maxImpactCsq
      .withColumn("nb_max_impact_by_var", count("*").over(variantWindow))
      .where($"nb_max_impact_by_var" === 1)
      .selectLocus($"ensembl_transcript_id")

    // Else, if multiple consequences are max_impact_score, join with OMIM
    val joinWithOmim = maxImpactCsq
      .joinByLocus(broadcast(picked1MaxImpactCsq), "left_anti") // remove already picked variants
      .join(genes.select("ensembl_gene_id", "omim_gene_id"), Seq("ensembl_gene_id"), "left")

    // Check if at least one csq in OMIM genes
    val inOmimCsq = joinWithOmim.where($"omim_gene_id".isNotNull)
    val noOmimCsq = joinWithOmim.joinByLocus(inOmimCsq, "left_anti")

    // For non-OMIM consequences, check if at least one csq is protein coding
    // If no OMIM csq and no protein coding csq, pick random csq for each variant
    val proteinCodingCsq = noOmimCsq.where($"biotype" === "protein_coding")
    val pickedNoProteinCodingCsq = noOmimCsq
      .joinByLocus(broadcast(proteinCodingCsq), "left_anti")
      .pickRandomCsqPerVariant

    // If in OMIM csq or protein coding csq, check if at least one csq is mane select
    // If at least one csq is mane_select, pick random csq for each variant
    val pickedManeSelectCsq = inOmimCsq.unionByName(proteinCodingCsq)
      .where($"mane_select")
      .pickRandomCsqPerVariant
    val noManeSelectCsq = inOmimCsq.unionByName(proteinCodingCsq)
      .joinByLocus(pickedManeSelectCsq, "left_anti")

    // If no mane select csq, check if at least one csq is canonical
    // If at least one csq is canonical, pick a random csq for each variant
    val pickedCanonicalCsq = noManeSelectCsq
      .where($"canonical")
      .pickRandomCsqPerVariant
    val noCanonicalCsq = noManeSelectCsq.joinByLocus(broadcast(pickedCanonicalCsq), "left_anti")

    // If no canonical csq, check if at least one csq is mane plus
    // If at least one csq is mane plus, pick random csq for each variant
    // Else, pick random csq
    val pickedManePlusCsq = noCanonicalCsq
      .where($"mane_plus")
      .pickRandomCsqPerVariant
    val pickedNoManePlusCsq = noCanonicalCsq
      .joinByLocus(broadcast(pickedManePlusCsq), "left_anti")
      .pickRandomCsqPerVariant

    // Union all picked consequences and set flag to true
    val pickedCsq = picked1MaxImpactCsq
      .unionByName(pickedNoProteinCodingCsq)
      .unionByName(pickedManeSelectCsq)
      .unionByName(pickedCanonicalCsq)
      .unionByName(pickedManePlusCsq)
      .unionByName(pickedNoManePlusCsq)
      .withColumn("picked", lit(true))
      .selectLocus($"ensembl_transcript_id", $"picked")

    // Join with all consequences to add picked flag and remove old pick flag
    csq
      .join(pickedCsq, locusColumnNames :+ "ensembl_transcript_id", "left")
      .drop("pick")
  }
}

object Consequences {
  implicit class DataFrameOps(df: DataFrame) {
    def pickRandomCsqPerVariant: DataFrame = {
      df
        .groupByLocus()
        .agg(first("ensembl_transcript_id") as "ensembl_transcript_id")
    }
  }
}
