package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object NonDuplicationSNV extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldBeEmpty(
        snv
          .groupBy($"chromosome", $"start", $"reference", $"alternate", $"service_request_id").count
          .filter($"count" > 1)
      )
    )
  }
}
