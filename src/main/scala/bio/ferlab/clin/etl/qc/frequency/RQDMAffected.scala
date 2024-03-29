package bio.ferlab.clin.etl.qc.frequency

import org.apache.spark.sql.functions.lit
import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object RQDMAffected extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df_normalized_snv_filter = normalized_snv
    .where($"affected_status_code" === "affected")

    val NbPatients = df_normalized_snv_filter
    .groupBy($"patient_id").count
    .count

    val df_expected_Freq = df_normalized_snv_filter
    .select($"chromosome", $"start", $"reference", $"alternate", $"patient_id", $"ad_alt", $"gq", $"filters", $"calls", $"analysis_code", $"affected_status_code")
    .dropDuplicates
    .groupBy($"chromosome", $"start", $"reference", $"alternate")
    .agg(ac, pc)
    .withColumn("expected_pn", lit(NbPatients))
    .withColumn("expected_an", lit(2*NbPatients))

    handleErrors(
      shouldBeEmpty(
        variant_centric
        .select($"chromosome", $"start", $"reference", $"alternate", $"frequency_RQDM.affected.*")
        .join(df_expected_Freq, Seq("chromosome", "start", "reference", "alternate"), "inner")
        .filter(!($"expected_ac" <=> $"ac") || !($"expected_an" <=> $"an") || !($"expected_pc" <=> $"pc") || !($"expected_pn" <=> $"pn"))
      )
    )
  }
}
