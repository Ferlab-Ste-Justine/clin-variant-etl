package bio.ferlab.clin.etl.vcf

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.vcf
import org.apache.spark.sql.functions.{array_distinct, col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Variants(batchId: String)(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("normalized_variants")
  val complete_joint_calling: DatasetConf = conf.getDataset("complete_joint_calling")

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      //TODO add vcf normalization
      complete_joint_calling.id -> vcf(complete_joint_calling.location, referenceGenomePath = None)
    )
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    data(complete_joint_calling.id)
      .withColumn("annotation", firstCsq)
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        is_multi_allelic,
        old_multi_allelic,
        array_distinct(csq("symbol")) as "genes_symbol",
        hgvsg,
        variant_class,
        pubmed,
        lit(batchId) as "batch_id",
        lit(null).cast("string") as "last_batch_id",
        /*current_timestamp()*/lit(batchId) as "createdOn",
        /*current_timestamp()*/lit(batchId) as "updatedOn"
      )
      .drop("annotation")
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    super.load(data
      .repartition(1, col("chromosome"))
      .sortWithinPartitions("start"))
  }
}
