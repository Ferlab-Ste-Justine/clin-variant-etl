package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object NonDuplicationGnomad extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldBeEmpty(
        gnomad_genomes_v3
          .groupBy($"chromosome", $"start", $"reference", $"alternate").count
          .filter($"count" > 1)
      )
    )
  }
}
