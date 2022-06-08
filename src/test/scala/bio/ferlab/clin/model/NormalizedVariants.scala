/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2022-04-06T13:21:03.685265
 */
package bio.ferlab.clin.model

import java.sql.Timestamp


case class NormalizedVariants(`chromosome`: String = "1",
                              `start`: Long = 69897,
                              `end`: Long = 69898,
                              `reference`: String = "T",
                              `alternate`: String = "C",
                              `name`: String = "rs200676709",
                              `genes_symbol`: List[String] = List("OR4F5"),
                              `hgvsg`: String = "chr1:g.69897T>C",
                              `variant_class`: String = "SNV",
                              `pubmed`: List[String] = List("29135816"),
                              `variant_type`: String = "germline",
                              `frequencies_by_analysis`: List[AnalysisCodeFrequencies] = List(AnalysisCodeFrequencies()),
                              `frequency_RQDM`: AnalysisFrequencies = AnalysisFrequencies(),
                              `batch_id`: String = "BAT1",
                              `created_on`: Timestamp = java.sql.Timestamp.valueOf("2022-04-06 13:21:08.019096"))
