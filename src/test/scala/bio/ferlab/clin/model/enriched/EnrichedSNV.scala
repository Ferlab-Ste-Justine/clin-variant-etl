/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2023-05-16T17:35:13.734545
 */
package bio.ferlab.clin.model.enriched

import java.sql.Date


case class EnrichedSNV(`chromosome`: String = "1",
                       `start`: Long = 69897,
                       `reference`: String = "T",
                       `alternate`: String = "C",
                       `patient_id`: String = "PA0001",
                       `aliquot_id`: String = "11111",
                       `end`: Long = 69898,
                       `name`: String = "rs200676709",
                       `dp`: Int = 1,
                       `gq`: Int = 30,
                       `calls`: Seq[Int] = Seq(0, 1),
                       `qd`: Double = 8.07,
                       `mother_gq`: Option[Int] = Some(30),
                       `mother_qd`: Option[Double] = Some(8.07),
                       `father_gq`: Option[Int] = Some(30),
                       `father_qd`: Option[Double] = Some(8.07),
                       `has_alt`: Boolean = true,
                       `is_multi_allelic`: Boolean = false,
                       `old_multi_allelic`: Option[String] = None,
                       `filters`: Seq[String] = Seq("PASS"),
                       `ad_ref`: Int = 0,
                       `ad_alt`: Int = 30,
                       `ad_total`: Int = 30,
                       `ad_ratio`: Double = 1.0,
                       `hgvsg`: String = "chr1:g.69897T>C",
                       `variant_class`: String = "SNV",
                       `batch_id`: String = "BAT1",
                       `last_update`: Date = java.sql.Date.valueOf("2022-04-06"),
                       `variant_type`: String = "germline",
                       `bioinfo_analysis_code`: String = "GEAN",
                       `service_request_id`: String = "SR0095",
                       `analysis_service_request_id`: String = "SRA0001",
                       `sequencing_strategy`: String = "WXS",
                       `genome_build`: String = "GRCh38",
                       `analysis_code`: String = "MM_PG",
                       `analysis_display_name`: String = "Maladies musculaires (Panel global)",
                       `family_id`: String = "FM00001",
                       `is_proband`: Boolean = true,
                       `gender`: String = "Male",
                       `practitioner_role_id`: String = "PPR00101",
                       `organization_id`: String = "OR00201",
                       `affected_status`: Boolean = true,
                       `affected_status_code`: String = "affected",
                       `mother_id`: String = "PA0003",
                       `father_id`: String = "PA0002",
                       `specimen_id`: String = "SP_696",
                       `sample_id`: String = "14-696",
                       `mother_calls`: Seq[Int] = Seq(0, 1),
                       `father_calls`: Seq[Int] = Seq(0, 0),
                       `mother_affected_status`: Boolean = true,
                       `father_affected_status`: Boolean = false,
                       `zygosity`: String = "HET",
                       `mother_zygosity`: String = "HET",
                       `father_zygosity`: String = "WT",
                       `parental_origin`: Option[String] = Some("mother"),
                       `transmission`: Option [String] = Some("autosomal_dominant"),
                       `is_hc`: Boolean = false,
                       `hc_complement`: Seq[HC_COMPLEMENT] = Seq(HC_COMPLEMENT()),
                       `possibly_hc_complement`: Seq[POSSIBLY_HC_COMPLEMENT] = Seq(POSSIBLY_HC_COMPLEMENT()),
                       `is_possibly_hc`: Boolean = false,
                       `exomiser_variant_score`: Option[Float] = Some(0.6581f),
                       `exomiser`: Option[EXOMISER] = Some(EXOMISER()),
                       `exomiser_other_moi`: Option[EXOMISER_OTHER_MOI] = Some(EXOMISER_OTHER_MOI()),
                       `franklin_score`: Option[Double] = Some(0.0036969461275872164))

case class HC_COMPLEMENT(`symbol`: Option[String] = None,
                         `locus`: Option[Seq[String]] = None)

case class POSSIBLY_HC_COMPLEMENT(`symbol`: Option[String] = None,
                                  `count`: Option[Long] = None)

case class EXOMISER(`rank`: Int = 3,
                    `gene_symbol`: String = "ALG13",
                    `gene_combined_score`: Float = 1.0f,
                    `moi`: String = "XR",
                    `acmg_classification`: String = "UNCERTAIN_SIGNIFICANCE",
                    `acmg_evidence`: String = "PP4,BP6")

case class EXOMISER_OTHER_MOI(`rank`: Int = 3,
                              `gene_symbol`: String = "ALG13",
                              `gene_combined_score`: Float = 0.99f,
                              `moi`: String = "AD",
                              `acmg_classification`: String = "UNCERTAIN_SIGNIFICANCE",
                              `acmg_evidence`: String = "PP4,BP6")
