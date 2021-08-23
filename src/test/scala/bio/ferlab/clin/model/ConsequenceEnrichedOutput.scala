/**
 * Generated by [[bio.ferlab.datalake.spark3.ClassGenerator]]
 * on 2021-02-26T14:50:11.503
 */
package bio.ferlab.clin.model

import java.sql.Timestamp


case class ConsequenceEnrichedOutput(`chromosome`: String = "1",
                                     `start`: Long = 69897,
                                     `reference`: String = "T",
                                     `alternate`: String = "C",
                                     `consequences`: List[String] = List("downstream_gene_variant"),
                                     `impact`: String = "MODIFIER",
                                     `symbol`: String = "DDX11L1",
                                     `ensembl_gene_id`: String = "ENSG00000223972",
                                     `ensembl_transcript_id`: String = "ENST00000450305",
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
                                     `created_on`: Timestamp = Timestamp.valueOf("2021-02-26 14:50:08.108"),
                                     `updated_on`: Timestamp = Timestamp.valueOf("2021-02-26 14:50:08.108"),
                                     `consequence`: List[String] = List("downstream gene"),
                                     `predictions`: PREDICTIONS = PREDICTIONS(),
                                     `conservations`: CONSERVATIONS = CONSERVATIONS())


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
