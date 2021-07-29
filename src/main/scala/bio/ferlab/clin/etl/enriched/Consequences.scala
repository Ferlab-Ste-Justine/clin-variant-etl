package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.utils.DeltaUtils
import bio.ferlab.clin.etl.utils.GenomicsUtils._
import bio.ferlab.clin.etl.utils.VcfUtils.columns.{formatted_consequences, _}
import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

class Consequences(lastBatchId: String)(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("enriched_consequences")
  val normalized_consequences: DatasetConf = conf.getDataset("normalized_consequences")
  val dbnsfp_original: DatasetConf = conf.getDataset("dbnsfp_original")

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      normalized_consequences.id -> normalized_consequences.read.where(col("updatedOn") >= lastBatchId),
      dbnsfp_original.id -> dbnsfp_original.read
    )
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val consequences = data(normalized_consequences.id)
    val dbnsfp = data(dbnsfp_original.id)
    val csq = consequences
      .drop("batch_id", "name", "end", "hgvsg", "variant_class", "ensembl_transcript_id", "ensembl_regulatory_id")
      .withColumn("consequence", formatted_consequences)

    joinWithDBNSFP(csq, dbnsfp)
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    super.load(data
      .repartition(1, col("chromosome"))
      .sortWithinPartitions("start"))
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
            $"FATHMM_converted_rankscore",
            $"FATHMM_pred" as "fathmm_pred",
            $"CADD_raw_rankscore" as "cadd_score",
            $"DANN_rankscore" as "dann_score",
            $"REVEL_rankscore" as "revel_rankscore",
            $"LRT_converted_rankscore" as "lrt_converted_rankscore",
            $"LRT_pred" as "lrt_pred") as "predictions",
          struct($"phyloP17way_primate_rankscore" as "phylo_p17way_primate_rankscore") as "conservations",
        )

    csq
      .join(dbnsfpRenamed, Seq("chromosome", "start", "reference", "alternate", "ensembl_feature_id"), "left")
      .select(csq("*"), dbnsfpRenamed("predictions"), dbnsfpRenamed("conservations"))

  }
}
