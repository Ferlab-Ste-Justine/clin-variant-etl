/**
 * Generated by [[bio.ferlab.datalake.spark3.ClassGenerator]]
 * on 2021-02-24T14:16:04.977
 */
package bio.ferlab.clin.model

case class VariantIndexUpdate(`chromosome`: String = "1",
                              `start`: Long = 69897,
                              `reference`: String = "T",
                              `alternate`: String = "C",
                              `donors`: List[DONORS] = List(DONORS(), DONORS(`organization_id` = "OR00202")),
                              `lab_frequencies`: Map[String, Freq] = Map("OR00201" -> Freq(2, 2, 1.0, 1, 0), "OR00202" -> Freq(2, 2, 1.0, 1, 0)),
                              `frequencies`: Map[String, Freq] = Map("internal" -> Freq(4, 4, 1.0, 2, 0)),
                              `participant_number`: Long = 2)
