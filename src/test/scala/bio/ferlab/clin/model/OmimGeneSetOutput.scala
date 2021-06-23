package bio.ferlab.clin.model

case class OmimGeneSetOutput(chromosome: String = "chr1",
                             start: Int = 2228318,
                             end: Int = 2310212,
                             cypto_location: String = "1p36.3",
                             computed_cypto_location: String = "1p36.33-p36.32",
                             omim_gene_id: Int = 164780,
                             symbols: List[String] = List("SKI", "SGS"),
                             name: String = "SKI proto-oncogene",
                             approved_symbol: String = "SKI",
                             entrez_gene_id: Int = 6497,
                             ensembl_gene_id: String = "ENSG00000157933",
                             documentation: String = "formerly mapped to 1q22-q24",
                             phenotype: PHENOTYPE = PHENOTYPE())

case class PHENOTYPE(name: String = "Shprintzen-Goldberg syndrome",
                     omim_id: String = "182212",
                     inheritance: List[String] = List("Autosomal dominant"),
                     inheritance_code: List[String] = List("AD"))
