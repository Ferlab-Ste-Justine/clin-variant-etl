package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime

class PrepareCnvCentric(releaseId: String)(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("es_index_cnv_centric")
  val enriched_cnv: DatasetConf = conf.getDataset("enriched_cnv")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      enriched_cnv.id -> enriched_cnv.read
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    data(enriched_cnv.id)

  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame =
    loadForReleaseId(data, destination, releaseId)


  /*
root
 |-- aliquot_id: string (nullable = true)
 |-- chromosome: string (nullable = true)
 |-- start: long (nullable = true)
 |-- reference: string (nullable = true)
 |-- alternate: string (nullable = true)
 |-- name: string (nullable = true)
 |-- bc: integer (nullable = true)
 |-- sm: double (nullable = true)
 |-- calls: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- cn: integer (nullable = true)
 |-- pe: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- is_multi_allelic: boolean (nullable = true)
 |-- old_multi_allelic: string (nullable = true)
 |-- ciend: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- cipos: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- svlen: integer (nullable = true)
 |-- reflen: integer (nullable = true)
 |-- end: long (nullable = true)
 |-- svtype: string (nullable = true)
 |-- filters: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- service_request_id: string (nullable = true)
 |-- patient_id: string (nullable = true)
 |-- analysis_service_request_id: string (nullable = true)
 |-- sequencing_strategy: string (nullable = true)
 |-- genome_build: string (nullable = true)
 |-- analysis_code: string (nullable = true)
 |-- analysis_display_name: string (nullable = true)
 |-- affected_status: boolean (nullable = true)
 |-- family_id: string (nullable = true)
 |-- is_proband: boolean (nullable = true)
 |-- gender: string (nullable = true)
 |-- practitioner_role_id: string (nullable = true)
 |-- organization_id: string (nullable = true)
 |-- mother_id: string (nullable = true)
 |-- father_id: string (nullable = true)
 |-- specimen_id: string (nullable = true)
 |-- sample_id: string (nullable = true)
 |-- genes: array (nullable = false)
 |    |-- element: struct (containsNull = false)
 |    |    |-- symbol: string (nullable = true)
 |    |    |-- refseq_id: string (nullable = true)
 |    |    |-- overlap_bases: long (nullable = true)
 |    |    |-- overlap_cnv_ratio: double (nullable = true)
 |    |    |-- overlap_gene_ratio: double (nullable = true)
 |    |    |-- panels: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- overlap_exons: long (nullable = false)
 |-- number_genes: integer (nullable = false)
   */

}

