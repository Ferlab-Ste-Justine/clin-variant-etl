/**
 * Generated by [[bio.ferlab.datalake.testutils.ClassGenerator]]
 * on 2023-11-24T17:06:24.457101
 */
package bio.ferlab.clin.model.normalized




case class NormalizedFranklin(`chromosome`: String = "16",
                              `start`: Long = 2498261,
                              `end`: Long = 2498262,
                              `reference`: String = "AT",
                              `alternate`: String = "A",
                              `batch_id`: String = "BAT1",
                              `family_id`: Option[String] = Some("1"),
                              `aliquot_id`: Option[String] = Some("1"),
                              `analysis_id`: String = "1",
                              `franklin_score`: Double = 0.9089091283319308,
                              `franklin_acmg_classification`: String = "PATHOGENIC",
                              `franklin_link`: String = "https://franklin.genoox.com/clinical-db/variant/snp/chr16-2498261-AT-A-HG38",
                              `franklin_acmg_evidence`: Seq[String] = Seq("PS1", "PS3"))

