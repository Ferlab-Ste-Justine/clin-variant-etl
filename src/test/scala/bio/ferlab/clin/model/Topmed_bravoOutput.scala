package bio.ferlab.clin.model

case class Topmed_bravoOutput(`chromosome`: String = "1",
                              `start`: Long = 69897,
                              `end`: Long = 69898,
                              `reference`: String = "T",
                              `alternate`: String = "C",
                              `name`: String = "TOPMed_freeze_5?chr1:10,051",
                              `ac`: Int = 2,
                              `af`: Double = 1.59276E-5,
                              `an`: Int = 125568,
                              `hom`: Int = 0,
                              `het`: Int = 2,
                              `qual`: Double = 255.0,
                              `filters`: List[String] = List("SVM"),
                              `qual_filter`: String = "FAIL")
