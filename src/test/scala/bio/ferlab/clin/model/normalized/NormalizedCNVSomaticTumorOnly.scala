/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2022-12-05T15:53:06.893514
 */
package bio.ferlab.clin.model.normalized

case class NormalizedCNVSomaticTumorOnly(`aliquot_id`: String = "11111",
                                         `chromosome`: String = "1",
                                         `start`: Long = 10000,
                                         `end`: Long = 10059,
                                         `reference`: String = "A",
                                         `alternate`: String = "TAA",
                                         `name`: String = "DRAGEN:LOSS:chr1:9823628-9823687",
                                         `qual`: Double = 27.0,
                                         `bc`: Int = 1,
                                         `sm`: Double = 0.57165,
                                         `calls`: Seq[Int] = Seq(0, 1),
                                         `pe`: Seq[Int] = Seq(0, 0),
                                         `is_multi_allelic`: Boolean = false,
                                         `old_multi_allelic`: Option[String] = None,
                                         `svlen`: Int = -60,
                                         `reflen`: Int = 60,
                                         `svtype`: String = "CNV",
                                         `filters`: Seq[String] = Seq("cnvQual"),
                                         `batch_id`: String = "BAT1",
                                         `type`: String = "LOSS",
                                         `variant_type`: String = "somatic",
                                         `sort_chromosome`: Int = 1,
                                         `service_request_id`: String = "SRS0001",
                                         `patient_id`: String = "PA0001",
                                         `analysis_service_request_id`: String = "SRA0001",
                                         `sequencing_strategy`: String = "WXS",
                                         `genome_build`: String = "GRCh38",
                                         `analysis_code`: String = "MMG",
                                         `analysis_display_name`: String = "Maladies musculaires (Panel global)",
                                         `affected_status`: Boolean = true,
                                         `affected_status_code`: String = "affected",
                                         `family_id`: String = "FM00001",
                                         `is_proband`: Boolean = true,
                                         `gender`: String = "Male",
                                         `practitioner_role_id`: String = "PPR00101",
                                         `organization_id`: String = "OR00201",
                                         `mother_id`: String = "PA0003",
                                         `father_id`: String = "PA0002",
                                         `mother_aliquot_id`: String = "33333",
                                         `father_aliquot_id`: String = "22222",
                                         `specimen_id`: String = "SP_001",
                                         `sample_id`: String = "SA_001")

