package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainOnlyNullVariantCentric extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainOnlyNull(
        variant_centric
      ),
    )
  }
}
