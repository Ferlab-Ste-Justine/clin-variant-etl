/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2022-04-06T11:19:20.272120
 */
package bio.ferlab.clin.model

import java.sql.Date


case class NormalizedSNV(chromosome: String = "1",
                         start: Long = 69897,
                         reference: String = "T",
                         alternate: String = "C",
                         patient_id: String = "PA0001",
                         aliquot_id: String = "11111",
                         end: Long = 69898,
                         name: String = "rs200676709",
                         dp: Int = 1,
                         gq: Int = 2,
                         calls: List[Int] = List(0, 1),
                         qd: Double = 8.07,
                         has_alt: Boolean = true,
                         is_multi_allelic: Boolean = false,
                         old_multi_allelic: Option[String] = None,
                         filters: List[String] = List("PASS"),
                         ad_ref: Int = 0,
                         ad_alt: Int = 1,
                         ad_total: Int = 1,
                         ad_ratio: Double = 1.0,
                         hgvsg: String = "chr1:g.69897T>C",
                         variant_class: String = "SNV",
                         batch_id: String = "BAT1",
                         last_update: Date = java.sql.Date.valueOf("2022-04-06"),
                         variant_type: String = "germline",
                         service_request_id: String = "SR0095",
                         analysis_service_request_id: String = "SRA0001",
                         sequencing_strategy: String = "WXS",
                         genome_build: String = "GRCh38",
                         analysis_code: String = "MM_PG",
                         analysis_display_name: String = "Maladies musculaires (Panel global)",
                         family_id: String = "FM00001",
                         is_proband: Boolean = true,
                         gender: String = "Male",
                         practitioner_role_id: String = "PPR00101",
                         organization_id: String = "OR00201",
                         affected_status: Boolean = true,
                         mother_id: String = "PA0003",
                         father_id: String = "PA0002",
                         specimen_id: String = "SP_696",
                         sample_id: String = "14-696",
                         mother_calls: Option[List[Int]] = Some(List(0, 1)),
                         father_calls: Option[List[Int]] = Some(List(0, 0)),
                         mother_affected_status: Option[Boolean] = Some(true),
                         father_affected_status: Option[Boolean] = Some(false),
                         zygosity: String = "HET",
                         mother_zygosity: Option[String] = Some("HET"),
                         father_zygosity: Option[String] = Some("WT"),
                         parental_origin: Option[String] = Some("mother"),
                         transmission: Option[String] = Some("autosomal_dominant"),
                         is_hc: Boolean = false,
                         hc_complement: List[HC_COMPLEMENT] = List(HC_COMPLEMENT()),
                         possibly_hc_complement: List[POSSIBLY_HC_COMPLEMENT] = List(POSSIBLY_HC_COMPLEMENT()),
                         is_possibly_hc: Boolean = false)

case class HC_COMPLEMENT(`symbol`: Option[String] = None,
                         `locus`: Option[List[String]] = None)

case class POSSIBLY_HC_COMPLEMENT(`symbol`: Option[String] = None,
                                  `count`: Option[Long] = None)
