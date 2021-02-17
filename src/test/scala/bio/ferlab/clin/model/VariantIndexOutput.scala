/**
 * Generated by [[bio.ferlab.clin.testutils.ClassGenerator]]
 * on 2021-02-18T14:02:07.692
 */
package bio.ferlab.clin.model

import java.sql.Date


case class VariantIndexOutput(`chromosome`: String = "1",
                              `start`: Long = 69897,
                              `reference`: String = "T",
                              `alternate`: String = "C",
                              `end`: Long = 69898,
                              `name`: String = "rs200676709",
                              `is_multi_allelic`: Boolean = false,
                              `old_multi_allelic`: Option[String] = None,
                              `genes_symbol`: List[String] = List("OR4F5"),
                              `hgvsg`: String = "chr1:g.69897T>C",
                              `variant_class`: String = "SNV",
                              `pubmed`: Option[List[String]] = None,
                              `batch_id`: String = "BAT1",
                              `last_batch_id`: Option[String] = None,
                              `assembly_version`: String = "GRCh38",
                              `last_annotation_update`: Date = Date.valueOf("2021-02-18"),
                              `consequences`: List[CONSEQUENCES] = List(CONSEQUENCES()),
                              `impact_score`: Int = 1,
                              `donors`: List[DONORS] = List(DONORS()),
                              `dna_change`: String = "T>C",
                              `frequencies`: FREQUENCIES = FREQUENCIES(),
                              `clinvar`: CLINVAR = CLINVAR(),
                              `dbsnp`: String = "rs200676709",
                              `genes`: List[GENES] = List(GENES()),
                              `omim`: List[String] = List("618285"),
                              `ext_db`: EXT_DB = EXT_DB() )


case class CONSEQUENCES(`consequences`: List[String] = List("downstream_gene_variant"),
                        `impact`: String = "MODIFIER",
                        `symbol`: String = "DDX11L1",
                        `ensembl_gene_id`: String = "ENSG00000223972",
                        `ensembl_feature_id`: String = "ENST00000450305",
                        `feature_type`: String = "Transcript",
                        `strand`: Int = 1,
                        `biotype`: String = "transcribed_unprocessed_pseudogene",
                        `exon`: EXON = EXON() ,
                        `intron`: INTRON = INTRON() ,
                        `hgvsc`: Option[String] = None,
                        `hgvsp`: Option[String] = None,
                        `cds_position`: Option[Int] = None,
                        `cdna_position`: Option[Int] = None,
                        `protein_position`: Option[Int] = None,
                        `amino_acids`: AMINO_ACIDS = AMINO_ACIDS() ,
                        `codons`: CODONS = CODONS() ,
                        `pick`: Boolean = false,
                        `canonical`: Boolean = false,
                        `aa_change`: Option[String] = None,
                        `coding_dna_change`: Option[String] = None,
                        `impact_score`: Int = 1,
                        `consequence`: List[String] = List("downstream gene"),
                        `predictions`: PREDICTIONS = PREDICTIONS(),
                        `conservations`: CONSERVATIONS = CONSERVATIONS()
                       )

case class PREDICTIONS(`sift_converted_rank_score`: Option[Double] = None,
                       `sift_pred`: Option[String] = None,
                       `polyphen2_hvar_score`: Option[Double] = None,
                       `polyphen2_hvar_pred`: Option[String] = None,
                       `FATHMM_converted_rankscore`: Option[Double] = None,
                       `fathmm_pred`: Option[String] = None,
                       `cadd_score`: Double = 0.95178,
                       `dann_score`: Double = 0.66795,
                       `revel_rankscore`: Option[Double] = None,
                       `lrt_converted_rankscore`: Option[Double] = None,
                       `lrt_pred`: Option[String] = None)

case class CONSERVATIONS(`phylo_p17way_primate_rankscore`: Double = 0.94297)

case class DONORS(`dp`: Int = 1,
                  `gq`: Int = 2,
                  `calls`: List[Int] = List(1, 1),
                  `qd`: Double = 8.07,
                  `has_alt`: Boolean = true,
                  `ad_ref`: Int = 0,
                  `ad_alt`: Int = 1,
                  `ad_total`: Int = 1,
                  `ad_ratio`: Double = 1.0,
                  `zygosity`: String = "HOM",
                  `hgvsg`: String = "chr1:g.69897T>C",
                  `variant_class`: String = "SNV",
                  `batch_id`: String = "BAT1",
                  `last_update`: Date = Date.valueOf("2020-11-29"),
                  `biospecimen_id`: String = "SP14909",
                  `patient_id`: String = "PA0001",
                  `family_id`: String = "FA0001",
                  `practitioner_id`: String = "PPR00101",
                  `organization_id`: String = "OR00201",
                  `sequencing_strategy`: String = "WXS",
                  `study_id`: String = "ET00010")


case class FREQUENCIES(//`1000_genomes`: Freq = Freq(3446, 5008,  0.688099),
                       topmed_bravo: Freq = Freq(2, 125568, 0.0000159276, 0, 2),
                       gnomad_genomes_2_1_1: GnomadFreqOutput = GnomadFreqOutput(1, 26342, 0.000037962189659099535, 0),
                       exac: GnomadFreqOutput = GnomadFreqOutput(0, 2, 0.0, 0),
                       gnomad_genomes_3_0: GnomadFreqOutput = GnomadFreqOutput(0, 53780, 0.0, 0),
                       internal: Freq = Freq(2, 2, 1.0, 1, 0))


case class ThousandGenomesFreq(ac: Long = 10,
                               an: Long = 20,
                               af: Double = 0.5)

case class GnomadFreqOutput(ac: Long = 10,
                            an: Long = 20,
                            af: Double = 0.5,
                            hom: Long = 10)

case class Freq(ac: Long = 10,
                an: Long = 20,
                af: Double = 0.5,
                hom: Long = 10,
                het: Long = 10)


case class CLINVAR(`clinvar_id`: String = "445750",
                   `clin_sig`: List[String] = List("Likely_benign"),
                   `conditions`: List[String] = List("not provided"),
                   `inheritance`: List[String] = List("germline"),
                   `interpretations`: List[String] = List("", "Likely_benign"))


case class GENES(`symbol`: Option[String] = Some("OR4F5"),
                 `entrez_gene_id`: Option[Int] = Some(777),
                 `omim_gene_id`: Option[String] = Some("601013"),
                 `hgnc`: Option[String] = Some("HGNC:1392"),
                 `ensembl_gene_id`: Option[String] = Some("ENSG00000198216"),
                 `location`: Option[String] = Some("1q25.3"),
                 `name`: Option[String] = Some("calcium voltage-gated channel subunit alpha1 E"),
                 `alias`: Option[List[String]] = Some(List("BII", "CACH6", "CACNL1A6", "Cav2.3", "EIEE69", "gm139")),
                 `biotype`: Option[String] = Some("protein_coding"),
                 `orphanet`: List[ORPHANET] = List(ORPHANET()) ,
                 `hpo`: List[HPO] = List(HPO()) ,
                 `omim`: List[OMIM] = List(OMIM()) )


case class EXT_DB(`is_pubmed`: Boolean = false,
                  `is_dbsnp`: Boolean = true,
                  `is_clinvar`: Boolean = true,
                  `is_hpo`: Boolean = true,
                  `is_orphanet`: Boolean = true,
                  `is_omim`: Boolean = true)

