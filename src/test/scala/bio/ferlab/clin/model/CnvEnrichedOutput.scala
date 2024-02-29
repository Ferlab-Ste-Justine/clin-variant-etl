/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2022-12-05T16:47:05.821711
 */
package bio.ferlab.clin.model

import bio.ferlab.datalake.testutils.models.enriched._


case class CnvEnrichedOutput(`aliquot_id`: String = "11111",
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
                             `cn`: Option[Int] = Some(1),
                             `pe`: Seq[Int] = Seq(0, 0),
                             `is_multi_allelic`: Boolean = false,
                             `old_multi_allelic`: Option[String] = None,
                             `ciend`: Option[Seq[Int]] = None,
                             `cipos`: Option[Seq[Int]] = None,
                             `svlen`: Int = -60,
                             `reflen`: Int = 60,
                             `svtype`: String = "CNV",
                             `filters`: Seq[String] = Seq("cnvQual"),
                             `batch_id`: String = "BAT1",
                             `type`: String = "LOSS",
                             `variant_type`: String = "germline",
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
                             `mother_aliquot_id`: Option[String] = Some("33333"),
                             `father_aliquot_id`: Option[String] = Some("22222"),
                             `specimen_id`: String = "SP_001",
                             `sample_id`: String = "SA_001",
                             `genes`: List[ENRICHED_CNV_GENES] = List(ENRICHED_CNV_GENES()),
                             `number_genes`: Int = 1,
                             `hash`: String = "e9458a88f3281d40be3d71cad7ac44d90b15b2cb")

case class ENRICHED_CNV_GENES(`symbol`: Option[String] = Some("OR4F5"),
                 `refseq_id`: Option[String] = Some("NC_000001.11"),
                 `gene_length`: Option[String] = Some("60.0"),
                 `overlap_bases`: Option[String] = Some("60.0"),
                 `overlap_cnv_ratio`: Option[Double] = Some(1.0),
                 `overlap_gene_ratio`: Option[Double] = Some(1.0),
                 `panels`: Seq[String] = Seq("DYSTM", "MITN"),
                 `orphanet`: List[ORPHANET] = List(ORPHANET()),
                 `hpo`: List[HPO] = List(HPO()),
                 `omim`: List[OMIM] = List(OMIM()),
                 `ddd`: List[DDD] = List(DDD()),
                 `cosmic`: List[COSMIC] = List(COSMIC()),
                 `overlap_exons`: Option[String] = Some("1"))
