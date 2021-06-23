package bio.ferlab.clin.model

case class DddGeneSetOutput(`symbol`: String = "HMX1",
                            `omim_gene_id`: String = "142992",
                            `disease_name`: String = "OCULOAURICULAR SYNDROME",
                            `disease_omim_id`: String = "612109",
                            `ddd_category`: String = "probable",
                            `mutation_consequence`: String = "loss of function",
                            `phenotypes`: List[String] = List(
                              "HP:0000007",
                              "HP:0000482",
                              "HP:0000647",
                              "HP:0007906",
                              "HP:0000568",
                              "HP:0000589",
                              "HP:0000639",
                              "HP:0000518",
                              "HP:0001104"
                            ),
                            `organ_specificity`: List[String] = List("Eye", "Ear"),
                            `panel`: String = "DD",
                            `hgnc_id`: String = "5017")
