/**
 * Generated by [[bio.ferlab.datalake.spark3.ClassGenerator]]
 * on 2021-02-18T14:02:07.692
 */
package bio.ferlab.clin.model

import java.sql.Date
import java.time.LocalDate


case class VariantIndexOutput(`chromosome`: String = "1",
                              `start`: Long = 69897,
                              `reference`: String = "T",
                              `alternate`: String = "C",
                              `end`: Long = 69898,
                              `hash`: String = "314c8a3ce0334eab1a9358bcaf8c6f4206971d92",
                              `is_multi_allelic`: Boolean = false,
                              `old_multi_allelic`: Option[String] = None,
                              `genes_symbol`: List[String] = List("OR4F5"),
                              `hgvsg`: String = "chr1:g.69897T>C",
                              `variant_class`: String = "SNV",
                              `variant_type`: String = "germline",
                              `pubmed`: Option[List[String]] = None,
                              `batch_id`: String = "BAT1",
                              `assembly_version`: String = "GRCh38",
                              `last_annotation_update`: Date = Date.valueOf(LocalDate.now()),
                              `consequences`: List[CONSEQUENCES] = List(CONSEQUENCES()),
                              `max_impact_score`: Int = 1,
                              `donors`: List[DONORS] = List(DONORS(), DONORS(`organization_id` = "OR00202")),
                              `frequencies_by_lab`: Map[String, Freq] = Map("OR00201" -> Freq(2, 2, 1.0, 1, 0), "OR00202" -> Freq(2, 2, 1.0, 1, 0)),
                              `participant_number`: Long = 2,
                              `dna_change`: String = "T>C",
                              `frequencies`: FREQUENCIES = FREQUENCIES(),
                              `clinvar`: CLINVAR = CLINVAR(),
                              `rsnumber`: String = "rs200676709",
                              `genes`: List[GENES] = List(GENES()),
                              `omim`: List[String] = List("618285"),
                              `variant_external_reference`: List[String] = List("DBSNP", "Clinvar"),
                              `gene_external_reference`: List[String] = List("HPO", "Orphanet", "OMIM"))


case class CONSEQUENCES(`consequences`: List[String] = List("downstream_gene_variant"),
                        `vep_impact`: String = "MODIFIER",
                        `symbol`: String = "DDX11L1",
                        `ensembl_gene_id`: String = "ENSG00000223972",
                        `ensembl_feature_id`: String = "ENST00000450305",
                        `feature_type`: String = "Transcript",
                        `strand`: Int = 1,
                        `biotype`: String = "transcribed_unprocessed_pseudogene",
                        `exon`: EXON = EXON(),
                        `intron`: INTRON = INTRON(),
                        `hgvsc`: Option[String] = None,
                        `hgvsp`: Option[String] = None,
                        `cds_position`: Option[Int] = None,
                        `cdna_position`: Option[Int] = None,
                        `protein_position`: Option[Int] = None,
                        `amino_acids`: AMINO_ACIDS = AMINO_ACIDS(),
                        `codons`: CODONS = CODONS(),
                        `pick`: Boolean = false,
                        `original_canonical`: Boolean = false,
                        `aa_change`: Option[String] = None,
                        `coding_dna_change`: Option[String] = None,
                        `impact_score`: Int = 1,
                        `consequence`: List[String] = List("downstream gene"),
                        `predictions`: PREDICTIONS = PREDICTIONS(),
                        `conservations`: CONSERVATIONS = CONSERVATIONS())

