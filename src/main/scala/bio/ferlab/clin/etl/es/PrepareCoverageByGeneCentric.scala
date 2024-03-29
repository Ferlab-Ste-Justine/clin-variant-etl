package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.{DatasetConf, DeprecatedRuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

case class PrepareCoverageByGeneCentric(rc: DeprecatedRuntimeETLContext) extends SingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_coverage_by_gene_centric")
  val enriched_coverage_by_gene: DatasetConf = conf.getDataset("enriched_coverage_by_gene")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      enriched_coverage_by_gene.id -> enriched_coverage_by_gene.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    data(enriched_coverage_by_gene.id)

  }

}

object PrepareCoverageByGeneCentric {
  @main
  def run(rc: DeprecatedRuntimeETLContext): Unit = {
    PrepareCoverageByGeneCentric(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
