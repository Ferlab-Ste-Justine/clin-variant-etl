/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2021-11-25T15:57:23.720689
 */
package bio.ferlab.clin.model.normalized.fhir

import java.sql.Date


case class NormalizedFamily(authored_on: Option[Date] = None,
                            analysis_service_request_id: String = "2908",
                            patient_id: String = "2909",
                            family: Option[FAMILY] = Some(FAMILY()),
                            family_id: Option[String] = Some("_FAMILY_ID_")
                           )

