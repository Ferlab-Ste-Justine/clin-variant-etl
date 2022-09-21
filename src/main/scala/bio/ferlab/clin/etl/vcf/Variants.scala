package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.etl.utils.FrequencyUtils
import bio.ferlab.clin.etl.vcf.Occurrences.getDiseaseStatus
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.{GenomicOperations, vcf}
import bio.ferlab.datalake.spark3.utils.RepartitionByColumns
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

import java.time.LocalDateTime

class Variants(batchId: String)(implicit configuration: Configuration) extends ETLSingleDestination {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_variants")
  val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")
  val clinical_impression: DatasetConf = conf.getDataset("normalized_clinical_impression")
  val observation: DatasetConf = conf.getDataset("normalized_observation")
  val task: DatasetConf = conf.getDataset("normalized_task")
  val service_request: DatasetConf = conf.getDataset("normalized_service_request")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      raw_variant_calling.id -> vcf(raw_variant_calling.location.replace("{{BATCH_ID}}", batchId), referenceGenomePath = None)
        .filter(col("contigName").isin(validContigNames: _*)),
      clinical_impression.id -> clinical_impression.read,
      observation.id -> observation.read,
      task.id -> task.read,
      service_request.id -> service_request.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {

    val variants = getVariants(data(raw_variant_calling.id))
    val clinicalInfos = getClinicalInfo(data)
    val variantsWithClinicalInfo = getVariantsWithClinicalInfo(data(raw_variant_calling.id), clinicalInfos)
    getVariantsWithFrequencies(variantsWithClinicalInfo)
      .joinByLocus(variants, "right")
      .withColumn("frequencies_by_analysis", coalesce(col("frequencies_by_analysis"), array()))
      .withColumn("frequency_RQDM", coalesce(col("frequency_RQDM"), emptyFrequencyRQDM))
      .withColumn("batch_id", lit(batchId))
      .withColumn("created_on", current_timestamp())

  }

  def getVariants(vcf: DataFrame): DataFrame = {
    vcf
      .withColumn("annotation", firstCsq)
      .withColumn("alleleDepths", functions.transform(col("genotypes.alleleDepths"), c => c(1)))
      .filter(size(filter(col("alleleDepths"), ad => ad >= 3)) > 0)
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        array_distinct(csq("symbol")) as "genes_symbol",
        hgvsg,
        variant_class,
        pubmed
      )
      .drop("annotation")
      .withColumn("variant_type", lit("germline"))
  }

  def getVariantsWithClinicalInfo(vcf: DataFrame, clinicalInfos: DataFrame): DataFrame = {
    vcf
      .withColumn("annotation", firstCsq)
      .select(
        explode(col("genotypes")) as "genotype",
        chromosome,
        start,
        reference,
        alternate,
        flatten(functions.transform(col("INFO_FILTERS"), c => split(c, ";"))) as "filters"
      )
      .withColumn("ad", col("genotype.alleleDepths"))
      .withColumn("ad_alt", col("ad")(1))
      .withColumn("gq", col("genotype.conditionalQuality"))
      .withColumn("aliquot_id", col("genotype.sampleId"))
      .withColumn("calls", col("genotype.calls"))
      .withColumn("zygosity", zygosity(col("calls")))
      .drop("genotype")
      .join(broadcast(clinicalInfos), Seq("aliquot_id"))
  }

  val emptyFrequency =
    struct(
      lit(0) as "ac",
      lit(0) as "an",
      lit(0.0) as "af",
      lit(0) as "pc",
      lit(0) as "pn",
      lit(0.0) as "pf",
      lit(0) as "hom"
    )

  val emptyFrequencyRQDM = struct(
    emptyFrequency as "affected",
    emptyFrequency as "non_affected",
    emptyFrequency as "total"
  )

  def getVariantsWithFrequencies(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val frequency: String => Column = {
      case "" =>
        struct(
          $"ac",
          $"an",
          coalesce($"ac" / $"an", lit(0.0)) as "af",
          $"pc",
          $"pn",
          coalesce($"pc" / $"pn", lit(0.0)) as "pf",
          $"hom"
        )
      case prefix: String =>
        struct(
          col(s"${prefix}_ac") as "ac",
          col(s"${prefix}_an") as "an",
          coalesce(col(s"${prefix}_ac") / col(s"${prefix}_an"), lit(0.0)) as "af",
          col(s"${prefix}_pc") as "pc",
          col(s"${prefix}_pn") as "pn",
          coalesce(col(s"${prefix}_pc") / col(s"${prefix}_pn"), lit(0.0)) as "pf",
          col(s"${prefix}_hom") as "hom"
        )
    }

    variants
      .groupBy(locus :+ col("analysis_code") :+ col("affected_status_str"): _*)
      .agg(
        first($"analysis_display_name") as "analysis_display_name",
        first($"affected_status") as "affected_status",
        FrequencyUtils.ac,
        FrequencyUtils.an,
        FrequencyUtils.het,
        FrequencyUtils.hom,
        FrequencyUtils.pc,
        FrequencyUtils.pn,
        first(struct(variants("*"))) as "variant")
      .withColumn("frequency_by_status", frequency(""))
      .groupBy(locus :+ col("analysis_code"): _*)
      .agg(
        map_from_entries(collect_list(struct($"affected_status_str", $"frequency_by_status"))) as "frequency_by_status",

        sum(when($"affected_status", $"ac").otherwise(0)) as "affected_ac",
        sum(when($"affected_status", $"an").otherwise(0)) as "affected_an",
        sum(when($"affected_status", $"pc").otherwise(0)) as "affected_pc",
        sum(when($"affected_status", $"pn").otherwise(0)) as "affected_pn",
        sum(when($"affected_status", $"hom").otherwise(0)) as "affected_hom",

        sum(when($"affected_status", 0).otherwise($"ac")) as "non_affected_ac",
        sum(when($"affected_status", 0).otherwise($"an")) as "non_affected_an",
        sum(when($"affected_status", 0).otherwise($"pc")) as "non_affected_pc",
        sum(when($"affected_status", 0).otherwise($"pn")) as "non_affected_pn",
        sum(when($"affected_status", 0).otherwise($"hom")) as "non_affected_hom",

        sum($"ac") as "ac",
        sum($"an") as "an",
        sum($"pc") as "pc",
        sum($"pn") as "pn",
        sum($"hom") as "hom",
        first($"analysis_display_name") as "analysis_display_name",
        first(col("variant")) as "variant"
      )
      .withColumn("frequency_by_status_total", map_from_entries(array(struct(lit("total"), frequency("")))))
      .withColumn("frequency_by_status", map_concat($"frequency_by_status_total", $"frequency_by_status"))
      .withColumn("frequency_by_status", when(array_contains(map_keys($"frequency_by_status"), "non_affected"), $"frequency_by_status")
        .otherwise(map_concat($"frequency_by_status", map_from_entries(array(struct(lit("non_affected"), emptyFrequency))))))
      .groupBy(locus: _*)
      .agg(
        collect_list(struct(
          $"analysis_display_name" as "analysis_display_name",
          $"analysis_code" as "analysis_code",
          col("frequency_by_status")("affected") as "affected",
          col("frequency_by_status")("non_affected") as "non_affected",
          col("frequency_by_status")("total") as "total"
        )) as "frequencies_by_analysis",
        sum($"affected_ac") as "affected_ac",
        sum($"affected_an") as "affected_an",
        sum($"affected_pc") as "affected_pc",
        sum($"affected_pn") as "affected_pn",
        sum($"affected_hom") as "affected_hom",

        sum($"non_affected_ac") as "non_affected_ac",
        sum($"non_affected_an") as "non_affected_an",
        sum($"non_affected_pc") as "non_affected_pc",
        sum($"non_affected_pn") as "non_affected_pn",
        sum($"non_affected_hom") as "non_affected_hom",

        sum($"ac") as "ac",
        sum($"an") as "an",
        sum($"pc") as "pc",
        sum($"pn") as "pn",
        sum($"hom") as "hom",
        first(col("variant")) as "variant"
      )
      .withColumn("frequency_RQDM", struct(
        frequency("affected") as "affected",
        frequency("non_affected") as "non_affected",
        frequency("") as "total"
      ))
      .select(locus :+ $"frequencies_by_analysis" :+ $"frequency_RQDM": _*)

  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("chromosome"), n = Some(10), sortColumns = Seq(col("start")))

  override def replaceWhere: Option[String] = Some(s"batch_id = '$batchId'")

  def getClinicalInfo(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val serviceRequestDf = data(service_request.id)
      .filter(col("service_request_type") === "sequencing")
      .select(
        col("id") as "service_request_id",
        col("service_request_code") as "analysis_code",
        col("service_request_description") as "analysis_display_name",
        col("analysis_service_request_id")
      )

    val analysisServiceRequestDf = data(service_request.id)
      .filter(col("service_request_type") === "analysis")

    val analysisServiceRequestWithDiseaseStatus = getDiseaseStatus(analysisServiceRequestDf, data(clinical_impression.id), data(observation.id))
      .select("analysis_service_request_id", "patient_id", "affected_status")
      .withColumn("affected_status_str", when(col("affected_status"), lit("affected")).otherwise("non_affected"))


    val taskDf = data(task.id)
      .where(col("experiment.name") === batchId)
      .select(
        col("experiment.aliquot_id") as "aliquot_id",
        col("patient_id"),
        col("service_request_id")
      ).dropDuplicates("aliquot_id", "patient_id")

    taskDf
      .join(serviceRequestDf, Seq("service_request_id"), "left")
      .join(analysisServiceRequestWithDiseaseStatus, Seq("analysis_service_request_id", "patient_id"))
  }
}