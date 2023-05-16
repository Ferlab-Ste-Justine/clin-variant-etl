package bio.ferlab.clin.etl.enriched

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.GenomicOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.{locus, locusColumnNames}
import bio.ferlab.datalake.spark3.utils.RepartitionByColumns
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class SNV()(implicit configuration: Configuration) extends ETLSingleDestination {

  override val mainDestination: DatasetConf = conf.getDataset("enriched_snv")
  val normalized_snv: DatasetConf = conf.getDataset("normalized_snv")
  val normalized_exomiser: DatasetConf = conf.getDataset("normalized_exomiser")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      normalized_snv.id -> normalized_snv.read,
      normalized_exomiser.id -> normalized_exomiser.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val snv = data(normalized_snv.id)
    val exomiser = data(normalized_exomiser.id).selectLocus(
      $"aliquot_id",
      $"exomiser_variant_score",
      $"contributing_variant",
      $"gene_combined_score",
      struct(
        "rank",
        "gene_symbol",
        "gene_combined_score",
        "moi",
        "acmg_classification",
        "acmg_evidence"
      ) as "exomiser_struct"
    )

    val geneCombinedScore = Window.partitionBy(locus :+ $"aliquot_id": _*).orderBy($"gene_combined_score".desc)
    val exomiserStructListSize = Window.partitionBy(locus :+ $"aliquot_id": _*).orderBy(size($"exomiser_struct_list").desc)

    snv
      .join(exomiser, locusColumnNames :+ "aliquot_id", "left")
      .withColumn("exomiser_struct_list", collect_list(when($"contributing_variant", $"exomiser_struct")).over(geneCombinedScore))
      .withColumn("exomiser_struct_list", first("exomiser_struct_list").over(exomiserStructListSize))
      .groupBy(locus :+ $"aliquot_id": _*)
      .agg(
        first(struct(snv("*"))) as "snv",
        max($"exomiser_variant_score") as "exomiser_variant_score",
        first("exomiser_struct_list") as "exomiser_struct_list"
      )
      .select(
        $"snv.*",
        $"exomiser_variant_score",
        $"exomiser_struct_list".getItem(0) as "exomiser",
        $"exomiser_struct_list".getItem(1) as "exomiser_other_moi"
      )
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("chromosome"), n = Some(1), sortColumns = Seq(col("start")))
}
