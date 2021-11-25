/**
 * Generated by [[bio.ferlab.datalake.spark3.ClassGenerator]]
 * on 2021-07-30T08:51:15.643577
 */

package bio.ferlab.clin.model

import java.sql.Date

case class OccurrenceRawOutput(`chromosome`: String = "1",
                               `start`: Long = 69897,
                               `end`: Long = 69898,
                               `reference`: String = "T",
                               `alternate`: String = "C",
                               `name`: String = "rs200676709",
                               `dp`: Int = 1,
                               `gq`: Int = 2,
                               `calls`: List[Int] = List(1, 1),
                               `qd`: Double = 8.07,
                               `has_alt`: Boolean = true,
                               `is_multi_allelic`: Boolean = false,
                               `old_multi_allelic`: Option[String] = None,
                               `filters`: List[String] = List("PASS"),
                               `ad_ref`: Int = 0,
                               `ad_alt`: Int = 1,
                               `ad_total`: Int = 1,
                               `ad_ratio`: Double = 1.0,
                               `zygosity`: String = "HOM",
                               `hgvsg`: String = "chr1:g.69897T>C",
                               `variant_class`: String = "SNV",
                               `batch_id`: String = "BAT1",
                               `last_update`: Date = Date.valueOf("2020-11-29"),
                               `variant_type`: String = "germline",
                               `aliquot_id`: String = "11111",
                               `analysis_code`: String = "MM-PG",
                               `patient_id`: String = "PA0001",
                               `family_id`: String = "FM00001",
                               `practitioner_role_id`: String = "PPR00101",
                               `organization_id`: String = "OR00201",
                               `sequencing_strategy`: String = "WXS",
                               `genome_build`: String = "GRCh38",
                               `mother_id`: String = "PA0003",
                               `father_id`: String = "PA0002",
                               `mother_calls`: Option[List[Int]] = None,
                               `father_calls`: Option[List[Int]] = None,
                               `mother_affected_status`: Option[Boolean] = None,
                               `father_affected_status`: Option[Boolean] = None,
                               `mother_zygosity`: Option[String] = None,
                               `father_zygosity`: Option[String] = None,
                               `parental_origin`: Option[String] = None,
                               `transmission`: Option[String] = Some("unknown_parents_genotype"))
