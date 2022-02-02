/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2021-02-16T21:28:13.351
 */
package bio.ferlab.clin.model

case class CosmicGeneSetOutput(`symbol`: String = "BRAF",
                               `name`: String = "v-raf murine sarcoma viral oncogene homolog B1",
                               `entrez_gene_id`: String = "673",
                               `Tier`: Int = 1,
                               `genome_location`: String = "7:140730665-140924928",
                               `hallmark`: Boolean = true,
                               `chr_band`: String = "34",
                               `somatic`: Boolean = true,
                               `germline`: Boolean = false,
                               `tumour_types_somatic`: List[String] = List("melanoma", "colorectal", "papillary thyroid", "borderline ovarian", "NSCLC", "cholangiocarcinoma", "pilocytic astrocytoma", "Spitzoid tumour", "pancreas acinar carcinoma", "melanocytic nevus", "prostate", "gastric"),
                               `tumour_types_germline`: Option[List[String]] = Some(List("breast", "colon", "endometrial cancer under age 50")),
                               `cancer_syndrome`: Option[String] = None,
                               `tissue_type`: List[String] = List("E", "O"),
                               `molecular_genetics`: String = "Dom",
                               `role_in_cancer`: List[String] = List("oncogene", "fusion"),
                               `mutation_types`: List[String] = List("Mis", "T", "O"),
                               `translocation_partner`: List[String] = List("AKAP9", "KIAA1549", "CEP88", "LSM14A", "SND1", "FCHSD1", "SLC45A3", "FAM131B", "RNF130", "CLCN6", "MKRN1", "GNAI1", "AGTRAP"),
                               `other_germline_mutation`: Boolean = true,
                               `other_syndrome`: List[String] = List("Cardio-facio-cutaneous syndrome"),
                               `synonyms`: List[String] = List("673", "BRAF", "BRAF1", "ENSG00000157764.13"))