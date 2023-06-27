/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2021-02-18T14:02:07.692
 */
package bio.ferlab.clin.model

import bio.ferlab.clin.etl.model.raw.{AMINO_ACIDS, CODONS, EXON, INTRON}
import bio.ferlab.clin.etl.varsome._
import bio.ferlab.clin.model.enriched.{CLINVAR, DONORS, FREQUENCIES, GENES, VARSOME}

import java.sql.Date
import java.time.LocalDate


case class VariantIndexOutput(`chromosome`: String = "1",
                              `start`: Long = 69897,
                              `reference`: String = "T",
                              `alternate`: String = "C",
                              `end`: Long = 69898,
                              `locus`: String = "1-69897-T-C",
                              `locus_id_1`: String = "1-69897-T-C",
                              `hash`: String = "314c8a3ce0334eab1a9358bcaf8c6f4206971d92",
                              `genes_symbol`: List[String] = List("OR4F5"),
                              `hgvsg`: String = "chr1:g.69897T>C",
                              `variant_class`: String = "SNV",
                              `variant_type`: String = "germline",
                              `pubmed`: Option[List[String]] = None,
                              `assembly_version`: String = "GRCh38",
                              `last_annotation_update`: Date = Date.valueOf(LocalDate.now()),
                              `consequences`: List[CONSEQUENCES] = List(CONSEQUENCES()),
                              `max_impact_score`: Int = 2,
                              `donors`: List[DONORS] = List(DONORS(), DONORS()),
                              `frequencies_by_analysis`: List[AnalysisCodeFrequencies] = List(AnalysisCodeFrequencies()),
                              `frequency_RQDM`: AnalysisFrequencies = AnalysisFrequencies(),
                              `external_frequencies`: FREQUENCIES = FREQUENCIES(),
                              `dna_change`: String = "T>C",
                              `clinvar`: CLINVAR = CLINVAR(),
                              `rsnumber`: String = "rs200676709",
                              `genes`: List[GENES] = List(GENES()),
                              `omim`: List[String] = List("618285"),
                              `variant_external_reference`: List[String] = List("DBSNP", "Clinvar", "Pubmed"),
                              `gene_external_reference`: List[String] = List("HPO", "Orphanet", "OMIM"),
                              `varsome`: Option[VARSOME] = Some(VARSOME()),
                              `exomiser_variant_score`: Option[Float] = Some(0.6581f))

case class CONSEQUENCES(`ensembl_transcript_id`: String = "ENST00000335137",
                        `ensembl_gene_id`: String = "ENSG00000186092",
                        `consequences`: List[String] = List("synonymous_variant"),
                        `vep_impact`: String = "LOW",
                        `symbol`: String = "OR4F5",
                        `symbol_id_1`: String = "OR4F5",
                        `ensembl_feature_id`: String = "ENST00000335137",
                        `feature_type`: String = "Transcript",
                        `strand`: Int = 1,
                        `biotype`: String = "protein_coding",
                        `exon`: EXON = EXON(),
                        `intron`: INTRON = INTRON(),
                        `hgvsc`: String = "ENST00000335137.4:c.807T>C",
                        `hgvsp`: String = "ENSP00000334393.3:p.Ser269=",
                        `cds_position`: Int = 807,
                        `cdna_position`: Int = 843,
                        `protein_position`: Int = 269,
                        `amino_acids`: AMINO_ACIDS = AMINO_ACIDS(),
                        `codons`: CODONS = CODONS(),
                        `aa_change`: String = "p.Ser269=",
                        `coding_dna_change`: String = "c.807T>C",
                        `impact_score`: Int = 2,
                        `predictions`: PREDICTIONS = PREDICTIONS(),
                        `conservations`: CONSERVATIONS = CONSERVATIONS(),
                        `refseq_mrna_id`: Seq[String] = Seq("NM_001005484.1", "NM_001005484.2"),
                        `uniprot_id`: Option[String] = None,
                        `mane_select`: Boolean = false,
                        `mane_plus`: Boolean = false,
                        `canonical`: Boolean = false,
                        `picked`: Boolean = true)

case class AnalysisCodeFrequencies(analysis_code: String = "MM_PG",
                                   analysis_display_name: String = "Maladies musculaires (Panel global)",
                                   affected: Frequency = Frequency(),
                                   non_affected: Frequency = Frequency(0,0,0.0,0,0,0.0,0),
                                   total: Frequency = Frequency())

case class AnalysisFrequencies(affected: Frequency = Frequency(4,4,1.0,2,2,1.0,2),
                               non_affected: Frequency = Frequency(0,0,0.0,0,0,0.0,0),
                               total: Frequency = Frequency(4,4,1.0,2,2,1.0,2))

case class Frequency(ac: Long = 4,
                     an: Long = 4,
                     af: Double = 1.0,
                     pc: Long = 2,
                     pn: Long = 2,
                     pf: Double = 1.0,
                     hom: Long = 2)
