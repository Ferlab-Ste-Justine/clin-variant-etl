/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2021-11-19T11:00:23.408161
 */
package bio.ferlab.clin.model.normalized.fhir

import java.sql.Timestamp


case class NormalizedPractitioner(`id`: String = "PR00101",
                                  `resource_type`: String = "Practitioner",
                                  `ingestion_file_name`: String = "/raw/landing/fhir/Practitioner/Practitioner_0_20211118_163103.json",
                                  `ingested_on`: Timestamp = Timestamp.valueOf("2021-11-18 16:31:03.0"),
                                  `version_id`: String = "1",
                                  `updated_on`: Timestamp = Timestamp.valueOf("2021-11-16 00:18:52.146"),
                                  `created_on`: Timestamp = Timestamp.valueOf("2021-11-16 00:18:52.146"),
                                  `first_name`: String = "Felix",
                                  `last_name`: String = "Laflamme",
                                  `name_prefix`: String = "Dr.",
                                  //`name_suffix`: Option[String] = None,
                                  `full_name`: String = "Dr. Felix Laflamme",
                                  `medical_license_number`: String = "37498")

